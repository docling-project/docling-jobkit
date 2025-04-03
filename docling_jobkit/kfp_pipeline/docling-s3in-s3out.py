from typing import Dict, List

from kfp import compiler, dsl, kubernetes


@dsl.component(
    packages_to_install=["docling==2.28.0"],
    base_image="python:3.11",
)
def load_models() -> str:
    from pathlib import Path

    from docling.utils.model_downloader import download_models

    models_path = download_models(output_dir=Path("/models")).absolute().as_posix()

    return models_path


@dsl.component(
    packages_to_install=[
        "docling==2.28.0",
        "git+https://github.com/docling-project/docling-jobkit@snt/add-allow-formats",
    ],
    # pip_index_urls=["https://download.pytorch.org/whl/cpu", "https://pypi.org/simple"],
    base_image="python:3.11",
)
def convert_payload(
    options: Dict,
    source: Dict,
    target: Dict,
    source_keys: List[str],
    cache_path: str,
) -> List:
    import logging
    import os
    from pathlib import Path

    from docling.datamodel.pipeline_options import (
        PdfPipelineOptions,
        TableFormerMode,
        TableStructureOptions,
    )
    from docling.models.factories import get_ocr_factory

    from docling_jobkit.connectors.s3_helper import DoclingConvert, S3Coordinates

    logging.basicConfig(level=logging.INFO)

    logging.info("Type of source_keys: {}".format(type(source_keys)))

    target_s3_coords = S3Coordinates(
        endpoint=target["s3_target_endpoint"],
        verify_ssl=target["s3_target_ssl"],
        access_key=target["s3_target_access_key"],
        secret_key=target["s3_target_secret_key"],
        bucket=target["s3_target_bucket"],
        key_prefix=target["s3_target_prefix"],
    )

    source_s3_coords = S3Coordinates(
        endpoint=source["s3_source_endpoint"],
        verify_ssl=source["s3_source_ssl"],
        access_key=source["s3_source_access_key"],
        secret_key=source["s3_source_secret_key"],
        bucket=source["s3_source_bucket"],
        key_prefix=source["s3_source_prefix"],
    )

    easyocr_path = Path("/models/EasyOcr")
    os.environ["MODULE_PATH"] = str(easyocr_path)
    os.environ["EASYOCR_MODULE_PATH"] = str(easyocr_path)

    if options.get("table_mode"):
        options["table_structure_options"] = TableStructureOptions(
            mode=TableFormerMode(options.pop("table_mode"))
        ).model_dump()

    from_format_options = options.pop("from_formats", None)
    to_format_options = options.pop("to_formats", None)
    pipeline_options = PdfPipelineOptions.model_validate(options)
    if options.get("ocr_engine"):
        pipeline_options.ocr_options = get_ocr_factory().create_options(
            kind=options.pop("ocr_engine")
        )
    pipeline_options.artifacts_path = cache_path

    converter = DoclingConvert(
        source_s3_coords=source_s3_coords,
        target_s3_coords=target_s3_coords,
        pipeline_options=pipeline_options,
        allowed_formats=from_format_options,
        to_formats=to_format_options,
    )

    results = []
    for item in converter.convert_documents(source_keys):
        results.append(item)
        logging.info("Convertion result: {}".format(item))

    return results


@dsl.component(
    packages_to_install=[
        "pydantic",
        "boto3~=1.35.36",
        "git+https://github.com/docling-project/docling-jobkit@snt/add-allow-formats",
    ],
    base_image="python:3.11",
)
def compute_batches(
    source: Dict,
    target: Dict,
    batch_size: int = 10,
) -> List[List[str]]:
    from docling_jobkit.connectors.s3_helper import (
        S3Coordinates,
        check_target_has_source_converted,
        generate_batch_keys,
        get_s3_connection,
        get_source_files,
    )

    s3_target_coords = S3Coordinates(
        endpoint=target["s3_target_endpoint"],
        verify_ssl=target["s3_target_ssl"],
        access_key=target["s3_target_access_key"],
        secret_key=target["s3_target_secret_key"],
        bucket=target["s3_target_bucket"],
        key_prefix=target["s3_target_prefix"],
    )

    s3_coords_source = S3Coordinates(
        endpoint=source["s3_source_endpoint"],
        verify_ssl=source["s3_source_ssl"],
        access_key=source["s3_source_access_key"],
        secret_key=source["s3_source_secret_key"],
        bucket=source["s3_source_bucket"],
        key_prefix=source["s3_source_prefix"],
    )

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

    return batch_keys


@dsl.pipeline
def docling_s3in_s3out_tiago(
    convertion_options: Dict = {
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
        "to_formats": ["json"],
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
        "generate_picture_images": True,
        "generate_page_images": True,
        "images_scale": 2,
    },
    source: Dict = {
        "s3_source_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
        "s3_source_access_key": "123454321",
        "s3_source_secret_key": "secretsecret",
        "s3_source_bucket": "source-bucket",
        "s3_source_prefix": "my-docs",
        "s3_source_ssl": True,
    },
    target: Dict = {
        "s3_target_endpoint": "s3.eu-de.cloud-object-storage.appdomain.cloud",
        "s3_target_access_key": "123454321",
        "s3_target_secret_key": "secretsecret",
        "s3_target_bucket": "target-bucket",
        "s3_target_prefix": "my-docs",
        "s3_target_ssl": True,
    },
    batch_size: int = 20,
    accelerator_settings: Dict = {
        "use_accelerator": False,
        "accelerator_type": "nvidia.com/gpu",
        "accelerator_limit": 1,
    },
    node_selector: Dict = {
        "add_node_selector": False,
        "labels": [
            {"label_key": "nvidia.com/gpu.product", "label_value": "NVIDIA-A10"}
        ],
    },
    tolerations: Dict = {
        "add_tolerations": False,
        "tolerations": [
            {
                "key": "key",
                "operator": "Equal",
                "value": "gpuCompute",
                "effect": "NoSchedule",
            }
        ],
    },
):
    import logging

    logging.basicConfig(level=logging.INFO)

    models_cache = load_models()
    kubernetes.mount_pvc(
        models_cache,
        pvc_name="docling-pipelines-models-cache",
        mount_path="/models",
    )

    batches = compute_batches(source=source, target=target, batch_size=5)
    # disable caching on batches as cached pre-signed urls might have already expired
    batches.set_caching_options(False)

    results = []
    with dsl.ParallelFor(batches.output, parallelism=3) as subbatch:
        converter = convert_payload(
            options=convertion_options,
            source=source,
            target=target,
            source_keys=subbatch,
            cache_path=models_cache.output,
        )
        kubernetes.mount_pvc(
            converter,
            pvc_name="docling-pipelines-models-cache",
            mount_path="/models",
        )
        converter.set_memory_request("1G")
        converter.set_memory_limit("7G")
        converter.set_cpu_request("200m")
        converter.set_cpu_limit("1")

        use_accelerator = True
        if use_accelerator:
            converter.set_accelerator_type("nvidia.com/gpu")
            converter.set_accelerator_limit("1")

        # add_node_selector = True
        # if add_node_selector:
        #     kubernetes.add_node_selector(
        #         task=converter,
        #         label_key="nvidia.com/gpu.product",
        #         label_value="NVIDIA-A10",
        #     )

        add_tolerations = True
        if add_tolerations:
            kubernetes.add_toleration(
                task=converter,
                key="key1",
                operator="Equal",
                value="mcad",
                effect="NoSchedule",
            )

        results.append(converter.output)


compiler.Compiler().compile(docling_s3in_s3out_tiago, "docling-s3in-s3out-tiago.yaml")
