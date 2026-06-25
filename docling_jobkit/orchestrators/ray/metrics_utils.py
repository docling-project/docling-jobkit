"""Ray Serve metrics generation utilities."""

from typing import TYPE_CHECKING, Optional, List, Tuple, Union
from docling.datamodel.document import ConversionResult
from docling.datamodel.base_models import ConversionStatus
from docling_jobkit.datamodel.exportable_document import ExportableDocument


def calculate_stats(values: List[Union[int, float]]) -> Tuple[Union[int, float], Union[int, float], float]:
    # Validate that all values are numeric
    if not all(isinstance(v, (int, float)) for v in values):
        raise TypeError("All values must be numeric (int or float)")
    
    # Calculate min and max
    min_val = min(values)
    max_val = max(values)
    
    # Calculate median
    sorted_values = sorted(values)
    n = len(sorted_values)
    
    if n % 2 == 0:
        # Even number of elements: average of two middle values
        median = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
    else:
        # Odd number of elements: middle value
        median = float(sorted_values[n // 2])
    
    return min_val, max_val, median


def reduce_timings(timings: dict):
    conversion_timings = timings
    timing_stats={}
    if conversion_timings:
        if "pipeline_total" in conversion_timings:
            timing_stats["pipeline_total"] = conversion_timings["pipeline_total"].times[0]
        if "page_parse" in conversion_timings:
            page_parse_min, page_parse_max, page_parse_median  = calculate_stats(conversion_timings["page_parse"].times)
            timing_stats["page_parse"] = {"min": page_parse_min, "max": page_parse_max, "median": page_parse_median}
        if "ocr" in conversion_timings:
            ocr_min, ocr_max, ocr_median = calculate_stats(conversion_timings["ocr"].times)
            timing_stats["ocr"] = {"min": ocr_min, "max": ocr_max, "median": ocr_median}
        if "layout" in conversion_timings:
            layout_min, layout_max, layout_median = calculate_stats(conversion_timings["layout"].times)
            timing_stats["layout"] = {"min": layout_min, "max": layout_max, "median": layout_median}
        if "table_structure" in conversion_timings:
            table_structure_min, table_structure_max, table_structure_median = calculate_stats(conversion_timings["table_structure"].times)
            timing_stats["table_structure"] = {"min": table_structure_min, "max": table_structure_max, "median": table_structure_median}
        if "page_assemble" in conversion_timings:
            page_assemble_min, page_assemble_max, page_assemble_median = calculate_stats(conversion_timings["page_assemble"].times)
            timing_stats["page_assemble"] = {"min": page_assemble_min, "max": page_assemble_max, "median": page_assemble_median}
        if "doc_assemble" in conversion_timings:
            timing_stats["doc_assemble"] = conversion_timings["doc_assemble"].times[0]
        if "reading_order" in conversion_timings:
            timing_stats["reading_order"] = conversion_timings["reading_order"].times[0]
        if "doc_enrich" in conversion_timings:
            timing_stats["doc_enrich"] = conversion_timings["doc_enrich"].times[0]
    return timing_stats
    
def collect_doc_stats(exp_doc: ExportableDocument):
    doc_stats = {}

    doc_stats["input_format"] = exp_doc.document_type
    doc = exp_doc.document
    if 'pages' in doc:
        doc_stats["num_pages"] = len(doc.pages)
        doc_stats["pictures"] = len(doc.pictures)
        doc_stats["tables"] = len(doc.tables)
        doc_stats["key_value_items"] = len(doc.key_value_items)
        doc_stats["form_items"] = len(doc.form_items)
        doc_stats["texts"] = len(doc.texts)
        doc_stats["groups"] = len(doc.groups)
    return doc_stats

# def get_metrics_from_conversion_result(conversion_result: ConversionResult):
#     metrics = {}
#     metrics["document_hash"] = conversion_result.input.document_hash
#     metrics["timings_stats"] = reduce_timings(timings=conversion_result.timings)
#     metrics["document_stats"] = collect_doc_stats(conv_res=conversion_result)
    
#     conv_status = conversion_result.status
#     if conv_status == ConversionStatus.SUCCESS:
#         status = "success"
#     elif conv_status == ConversionStatus.PARTIAL_SUCCESS:
#         status = "partial"
#     else:
#         status = "failed"
#     metrics["status"] = status

#     return metrics

def get_metrics_from_exportable_doc(exp_doc: ExportableDocument):
    metrics = {}
    metrics["document_hash"] = exp_doc.document_hash
    metrics["timings_stats"] = reduce_timings(timings=exp_doc.timings)
    metrics["document_stats"] = collect_doc_stats(exp_doc=exp_doc)
    
    # conv_status = exp_doc.status
    # if conv_status == ConversionStatus.SUCCESS:
    #     status = "success"
    # elif conv_status == ConversionStatus.PARTIAL_SUCCESS:
    #     status = "partial"
    # else:
    #     status = "failed"
    metrics["status"] = exp_doc.status

    return metrics