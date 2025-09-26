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
    base_image="ghcr.io/docling-project/docling-serve-cpu:v1.5.1",  # base docling-serve image with fixed permissions
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

    from docling_jobkit.connectors.s3_helper import (
        generate_presign_url,
        get_s3_connection,
    )
    from docling_jobkit.connectors.s3_target_processor import S3TargetProcessor
    from docling_jobkit.convert.manager import (
        DoclingConverterManager,
        DoclingConverterManagerConfig,
    )
    from docling_jobkit.convert.results_processor import ResultsProcessor
    from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
    from docling_jobkit.datamodel.s3_coords import S3Coordinates

    logging.basicConfig(level=logging.INFO)

    # validate coords
    s3_coords_source = S3Coordinates.model_validate(source)
    target_s3_coords = S3Coordinates.model_validate(target)

    s3_source_client, s3_source_resource = get_s3_connection(s3_coords_source)

    convert_options = ConvertDocumentsOptions.model_validate(options)

    config = DoclingConverterManagerConfig()
    # Points to the pvc mounted on the pod
    config.artifacts_path = Path("/modelcache")
    converter = DoclingConverterManager(config)

    with open(dataset.path) as f:
        batches = json.load(f)
    source_keys = batches[batch_index]

    presign_filtered_source_keys = [
        url
        for url in [
            generate_presign_url(s3_source_client, key, s3_coords_source.bucket)
            for key in source_keys
        ]
        if url is not None
    ]

    results = []
    with S3TargetProcessor(target_s3_coords) as target_processor:
        result_processor = ResultsProcessor(
            target_processor=target_processor,
            to_formats=[v.value for v in convert_options.to_formats],
            generate_page_images=convert_options.include_images,
            generate_picture_images=convert_options.include_images,
        )
        for item in result_processor.process_documents(
            converter.convert_documents(
                presign_filtered_source_keys, options=convert_options
            )
        ):
            results.append(item)
            logging.info("Convertion result: {}".format(item))

    return results


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
def inputs_s3in_s3out(
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

        converter.set_accelerator_type("nvidia.com/gpu")
        converter.set_accelerator_limit("1")

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

compiler.Compiler().compile(inputs_s3in_s3out, "docling-s3in-s3out.yaml")


### Start pipeline run programatically
# import kfp
# import os

# # TIP: you may need to authenticate with the KFP instance
# kfp_client = kfp.Client(
#     host = os.environ["KFP_FULL_URL"],
#     existing_token = os.environ["OPENSHIFT_TOKEN"],
#     verify_ssl = False,
# )

# kfp_client.create_run_from_pipeline_func(
#     inputs_s3in_s3out,
#     arguments=dict(
#         convertion_options = {
#             "from_formats": [
#                 "pdf",
#             ],
#             "to_formats": ["md", "json", "html", "text", "doctags"],
#             "image_export_mode": "placeholder",
#             "do_ocr": True,
#             "force_ocr": True,
#             "ocr_engine": "easyocr",
#         },
#         source = {
#             "endpoint": os.environ["S3_SOURCE_ENDPOINT"],
#             "access_key": os.environ["S3_SOURCE_ACCESS_KEY"],
#             "secret_key": os.environ["S3_SOURCE_SECRET_KEY"],
#             "bucket": os.environ["S3_SOURCE_BUCKET"],
#             "key_prefix": os.environ["S3_SOURCE_PREFIX"],
#             "verify_ssl": True,
#         },
#         target = {
#             "endpoint": os.environ["S3_TARGET_ENDPOINT"],
#             "access_key": os.environ["S3_TARGET_ACCESS_KEY"],
#             "secret_key": os.environ["S3_TARGET_SECRET_KEY"],
#             "bucket": os.environ["S3_TARGET_BUCKET"],
#             "key_prefix": os.environ["S3_TARGET_PREFIX"],
#             "verify_ssl": True,
#         },
#         batch_size = 100,
#     )
# )
