# ruff: noqa: E402

from typing import List, cast

from kfp import dsl


@dsl.component(
    packages_to_install=[
        "docling==2.28.0",
        "git+https://github.com/docling-project/docling-jobkit@5488c3858a00a716fdbbec46dd757c793fa5c2bb",
    ],
    base_image="quay.io/docling-project/docling-serve:dev-0.0.2",  # base docling-serve image with fixed permissions
)
def convert_payload(
    options: dict,
    source: dict,
    target: dict,
    source_keys: List[str],
) -> list:
    import logging
    from typing import Optional

    from docling.backend.docling_parse_backend import DoclingParseDocumentBackend
    from docling.backend.docling_parse_v2_backend import DoclingParseV2DocumentBackend
    from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
    from docling.backend.pdf_backend import PdfDocumentBackend
    from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
    from docling.datamodel.pipeline_options import (
        OcrOptions,
        PdfBackend,
        PdfPipelineOptions,
        TableFormerMode,
    )
    from docling.models.factories import get_ocr_factory

    from docling_jobkit.connectors.s3_helper import DoclingConvert
    from docling_jobkit.model.convert import ConvertDocumentsOptions
    from docling_jobkit.model.s3_inputs import S3Coordinates

    logging.basicConfig(level=logging.INFO)

    # validate inputs
    source_s3_coords = S3Coordinates.model_validate(source)
    target_s3_coords = S3Coordinates.model_validate(target)

    convert_options = ConvertDocumentsOptions.model_validate(options)

    backend: Optional[type[PdfDocumentBackend]] = None
    if convert_options.pdf_backend:
        if convert_options.pdf_backend == PdfBackend.DLPARSE_V1:
            backend = DoclingParseDocumentBackend
        elif convert_options.pdf_backend == PdfBackend.DLPARSE_V2:
            backend = DoclingParseV2DocumentBackend
        elif convert_options.pdf_backend == PdfBackend.DLPARSE_V4:
            backend = DoclingParseV4DocumentBackend
        elif convert_options.pdf_backend == PdfBackend.PYPDFIUM2:
            backend = PyPdfiumDocumentBackend
        else:
            raise RuntimeError(
                f"Unexpected PDF backend type {convert_options.pdf_backend}"
            )

    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = convert_options.do_ocr
    ocr_factory = get_ocr_factory()

    pipeline_options.ocr_options = cast(
        OcrOptions, ocr_factory.create_options(kind=convert_options.ocr_engine)
    )

    pipeline_options.do_table_structure = convert_options.do_table_structure
    pipeline_options.table_structure_options.mode = TableFormerMode(
        convert_options.table_mode
    )
    pipeline_options.generate_page_images = convert_options.include_images
    pipeline_options.do_code_enrichment = convert_options.do_code_enrichment
    pipeline_options.do_formula_enrichment = convert_options.do_formula_enrichment
    pipeline_options.do_picture_classification = (
        convert_options.do_picture_classification
    )
    pipeline_options.do_picture_description = convert_options.do_picture_description
    pipeline_options.generate_picture_images = convert_options.generate_picture_images

    # pipeline_options.accelerator_options = AcceleratorOptions(
    #     num_threads=2, device=AcceleratorDevice.CUDA
    # )

    converter = DoclingConvert(
        source_s3_coords=source_s3_coords,
        target_s3_coords=target_s3_coords,
        pipeline_options=pipeline_options,
        allowed_formats=convert_options.from_formats,
        to_formats=convert_options.to_formats,
        backend=backend,
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
        "git+https://github.com/docling-project/docling-jobkit@5488c3858a00a716fdbbec46dd757c793fa5c2bb",
    ],
    base_image="python:3.11",
)
def compute_batches(
    source: dict,
    target: dict,
    batch_size: int = 10,
) -> List[List[str]]:
    from docling_jobkit.connectors.s3_helper import (
        check_target_has_source_converted,
        generate_batch_keys,
        get_s3_connection,
        get_source_files,
    )
    from docling_jobkit.model.s3_inputs import S3Coordinates

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

    return batch_keys


@dsl.pipeline
def docling_s3in_s3out(
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
    # accelerator_settings: dict = {
    #     "use_accelerator": False,
    #     "accelerator_type": "nvidia.com/gpu",
    #     "accelerator_limit": 1,
    # },
    # node_selector: dict = {
    #     "add_node_selector": False,
    #     "labels": [
    #         {"label_key": "nvidia.com/gpu.product", "label_value": "NVIDIA-A10"}
    #     ],
    # },
    # tolerations: dict = {
    #     "add_tolerations": False,
    #     "tolerations": [
    #         {
    #             "key": "key",
    #             "operator": "Equal",
    #             "value": "gpuCompute",
    #             "effect": "NoSchedule",
    #         }
    #     ],
    # },
):
    import logging

    logging.basicConfig(level=logging.INFO)

    batches = compute_batches(source=source, target=target, batch_size=batch_size)
    # disable caching on batches as cached pre-signed urls might have already expired
    batches.set_caching_options(False)

    results = []
    with dsl.ParallelFor(batches.output, parallelism=3) as subbatch:
        converter = convert_payload(
            options=convertion_options,
            source=source,
            target=target,
            source_keys=subbatch,
        )
        converter.set_memory_request("1G")
        converter.set_memory_limit("7G")
        converter.set_cpu_request("200m")
        converter.set_cpu_limit("1")

        # For enabling document conversion using GPU
        # currently unable to properly pass input parameters into pipeline, therefore node selector and tolerations are hardcoded

        # use_accelerator = True
        # if use_accelerator:
        #     converter.set_accelerator_type("nvidia.com/gpu")
        #     converter.set_accelerator_limit("1")

        # add_node_selector = True
        # if add_node_selector:
        #     kubernetes.add_node_selector(
        #         task=converter,
        #         label_key="nvidia.com/gpu.product",
        #         label_value="NVIDIA-A10",
        #     )

        # add_tolerations = True
        # if add_tolerations:
        #     kubernetes.add_toleration(
        #         task=converter,
        #         key="key1",
        #         operator="Equal",
        #         value="mcad",
        #         effect="NoSchedule",
        #     )

        results.append(converter.output)


from kfp import compiler

compiler.Compiler().compile(docling_s3in_s3out, "docling-s3in-s3out.yaml")
