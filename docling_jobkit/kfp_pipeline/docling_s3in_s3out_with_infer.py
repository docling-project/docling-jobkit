# ruff: noqa: E402

from typing import List, NamedTuple

from kfp import dsl, kubernetes
from kfp.dsl import Dataset, Input, Output


@dsl.component(
    packages_to_install=[
        "docling==2.50.0",
        "boto3~=1.35.36",
        "git+https://github.com/docling-project/docling-jobkit@main",
    ],
    base_image="ghcr.io/docling-project/docling-serve-cpu:v1.5.1",
)
def convert_payload(
    options: dict,
    source: dict,
    target: dict,
    batch_index: int,
    # source_keys: List[str],
    dataset: Input[Dataset],
) -> list:
    import json
    import logging
    from pathlib import Path

    from docling.datamodel.base_models import ConversionStatus, InputFormat
    from docling.datamodel.pipeline_options import (
        VlmPipelineOptions,
    )
    from docling.datamodel.pipeline_options_vlm_model import (
        ApiVlmOptions,
        ResponseFormat,
    )
    from docling.datamodel.settings import settings
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.pipeline.vlm_pipeline import VlmPipeline
    from docling_core.types.doc.base import ImageRefMode

    from docling_jobkit.connectors.s3_helper import (
        generate_presign_url,
        get_s3_connection,
    )
    from docling_jobkit.datamodel.s3_coords import S3Coordinates

    def openai_compatible_vlm_options(
        model: str,
        prompt: str,
        format: ResponseFormat,
        hostname_and_port,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        api_key: str = "",
        skip_special_tokens=False,
    ):
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        options = ApiVlmOptions(
            url=f"http://{hostname_and_port}/v1/chat/completions",  # LM studio defaults to port 1234, VLLM to 8000
            params={
                "model": model,
                "max_tokens": max_tokens,
                "skip_special_tokens": skip_special_tokens,  # needed for VLLM
            },
            headers=headers,
            prompt=prompt,
            timeout=90,
            scale=2.0,
            temperature=temperature,
            response_format=format,
        )
        return options

    logging.basicConfig(level=logging.INFO)
    settings.debug.profile_pipeline_timings = True

    # validate coords
    s3_coords_source = S3Coordinates.model_validate(source)
    target_s3_coords = S3Coordinates.model_validate(target)
    s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)
    s3_target_client, s3_target_resource = get_s3_connection(target_s3_coords)

    with open(dataset.path) as f:
        batches = json.load(f)
    source_keys = batches[batch_index]

    presign_filtered_source_keys = [
        generate_presign_url(s3_source_client, key, s3_coords_source.bucket)
        for key in source_keys
    ]

    pipeline_options = VlmPipelineOptions(
        enable_remote_services=True  # required when calling remote VLM endpoints
    )

    pipeline_options.vlm_options = openai_compatible_vlm_options(
        model="ibm-granite/granite-docling-258M",  # For VLLM use "ibm-granite/granite-docling-258M"
        hostname_and_port="vllm-gpu.deep-search.svc.cluster.local:8000",  # LM studio defaults to port 1234, VLLM to 8000
        prompt="Convert this page to docling.",
        format=ResponseFormat.DOCTAGS,
        api_key="",
    )

    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
                pipeline_cls=VlmPipeline,
            )
        }
    )

    for item in presign_filtered_source_keys:
        conv_res = doc_converter.convert(str(item))
        if conv_res.status == ConversionStatus.SUCCESS:
            filename = conv_res.input.document_hash

            # store doc
            temp_folder = Path("/modelcache/docling-temp")
            temp_folder.mkdir(parents=True, exist_ok=True)
            temp_json_file = temp_folder / f"{filename}.temp.json"
            conv_res.document.save_as_json(
                filename=temp_json_file,
                image_mode=ImageRefMode.EMBEDDED,
            )
            kwargs = {}
            kwargs["ContentType"] = "application/json"
            try:
                s3_target_client.upload_file(
                    Filename=temp_json_file,
                    Bucket=target_s3_coords.bucket,
                    Key=f"{target_s3_coords.key_prefix}/{filename}.json",
                    ExtraArgs={**kwargs},
                )
                # s3_target_client.put_object(
                #     Body=json.dumps(conv_res.document.export_to_dict()),
                #     Bucket=target_s3_coords.bucket,
                #     Key=f"{target_s3_coords.key_prefix}/{filename}.json",
                #     ContentType='"application/json"'
                # )
            except Exception as e:
                logging.error("Uploading document to s3 failed: {}".format(e))

            # clean-up
            temp_json_file.unlink()

            # store timings
            timings = {}
            for key in conv_res.timings.keys():
                timings[key] = {
                    "scope": conv_res.timings[key].scope.name,
                    "count": conv_res.timings[key].count,
                    "times": conv_res.timings[key].times,
                }
            try:
                s3_target_client.put_object(
                    Body=json.dumps(timings),
                    Bucket=target_s3_coords.bucket,
                    Key=f"{target_s3_coords.key_prefix}/{filename}.timings.json",
                    ContentType="application/json",
                )
            except Exception as e:
                logging.error("Uploading timings to s3 failed: {}".format(e))

    return []


@dsl.component(
    packages_to_install=[
        "pydantic",
        "boto3~=1.35.36",
        "git+https://github.com/docling-project/docling-jobkit@main",
    ],
    base_image="ghcr.io/docling-project/docling-serve-cpu:v1.5.1",
)
def compute_batches(
    source: dict,
    target: dict,
    dataset: Output[Dataset],
    batch_size: int = 10,
) -> NamedTuple("outputs", [("batch_indices", List[int])]):  # type: ignore[valid-type]
    import json
    import logging
    from typing import NamedTuple

    from docling_jobkit.connectors.s3_helper import (
        check_target_has_source_converted,
        generate_batch_keys,
        get_s3_connection,
        get_source_files,
    )
    from docling_jobkit.datamodel.s3_coords import S3Coordinates

    logging.basicConfig(level=logging.INFO)

    # validate inputs
    s3_coords_source = S3Coordinates.model_validate(source)
    s3_target_coords = S3Coordinates.model_validate(target)

    s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)
    source_objects_list = get_source_files(
        s3_source_client, s3_source_resource, s3_coords_source
    )
    filtered_source_keys = check_target_has_source_converted(
        s3_target_coords, source_objects_list, s3_coords_source.key_prefix
    )
    batch_keys = generate_batch_keys(
        filtered_source_keys,
        batch_size=batch_size,
    )

    # store batches on s3 for debugging
    s3_target_client, s3_target_resource = get_s3_connection(s3_target_coords)
    try:
        s3_target_client.put_object(
            Body=json.dumps(batch_keys),
            Bucket=s3_target_coords.bucket,
            Key=f"{s3_target_coords.key_prefix}/buckets.json",
            ContentType="application/json",
        )
    except Exception as e:
        logging.error("Uploading batch.json to s3 failed: {}".format(e))

    with open(dataset.path, "w") as out_batches:
        json.dump(batch_keys, out_batches)

    batch_indices = list(range(len(batch_keys)))
    outputs = NamedTuple("outputs", [("batch_indices", List[int])])
    print(f"Batches to convert: {len(batch_keys)}")
    return outputs(batch_indices)


@dsl.pipeline
def docling_s3in_s3out_with_infer(
    convertion_options: dict = {
        "from_formats": [
            "docx",
            "pptx",
            "html",
            "image",
            "pdf",
            "asciidoc",
            "md",
            "xlsx",
            "xml_uspto",
            "xml_jats",
            "json_docling",
        ],
        "to_formats": ["md", "json", "html", "text", "doctags"],
        "image_export_mode": "placeholder",
        "do_ocr": True,
        "force_ocr": False,
        "ocr_engine": "easyocr",
        "ocr_lang": [],
        "pdf_backend": "dlparse_v2",
        "table_mode": "accurate",
        "abort_on_error": False,
        "return_as_file": False,
        "do_table_structure": True,
        "do_code_enrichment": False,
        "do_formula_enrichment": False,
        "do_picture_classification": False,
        "do_picture_description": False,
        "generate_picture_images": False,
        "include_images": True,
        "images_scale": 2,
    },
    source: dict = {
        "endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
        "access_key": "123454321",
        "secret_key": "secretsecret",
        "bucket": "source-bucket",
        "key_prefix": "my-docs",
        "verify_ssl": True,
    },
    target: dict = {
        "endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
        "access_key": "123454321",
        "secret_key": "secretsecret",
        "bucket": "target-bucket",
        "key_prefix": "my-docs",
        "verify_ssl": True,
    },
    batch_size: int = 20,
):
    import logging

    logging.basicConfig(level=logging.INFO)

    batches = compute_batches(source=source, target=target, batch_size=batch_size)
    # disable caching on batches as cached pre-signed urls might have already expired
    batches.set_caching_options(False)

    results = []
    with dsl.ParallelFor(batches.outputs["batch_indices"], parallelism=20) as subbatch:
        converter = convert_payload(
            options=convertion_options,
            source=source,
            target=target,
            dataset=batches.outputs["dataset"],
            batch_index=subbatch,
        )
        converter.set_memory_request("1G")
        converter.set_memory_limit("16G")
        converter.set_cpu_request("200m")
        converter.set_cpu_limit("1")

        # All models should be preloaded to pvc, multi access pvc can be used by all pods in parallel
        kubernetes.mount_pvc(
            converter,
            pvc_name="docling-model-cache-pvc-multi",
            mount_path="/modelcache",
        )

        # For enabling document conversion using GPU
        # currently unable to properly pass input parameters into pipeline, therefore node selector and tolerations are hardcoded

        # converter.set_accelerator_type("nvidia.com/gpu")
        # converter.set_accelerator_limit("1")

        # kubernetes.add_node_selector(
        #     task=converter,
        #     label_key="nvidia.com/gpu.product",
        #     label_value="NVIDIA-A10",
        # )

        kubernetes.add_toleration(
            task=converter,
            key="key1",
            operator="Equal",
            value="mcad",
            effect="NoSchedule",
        )

        results.append(converter.output)


### Compile pipeline into a yaml
from kfp import compiler

compiler.Compiler().compile(
    docling_s3in_s3out_with_infer, "docling_s3in_s3out_with_infer.yaml"
)
