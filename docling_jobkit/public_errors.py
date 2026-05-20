from collections.abc import Sequence

from docling.datamodel.base_models import DoclingComponentType, ErrorItem

INTERNAL_TASK_ERROR_MESSAGE = "Internal processing error."
INTERNAL_DOCUMENT_ERROR_MESSAGE = "Internal document processing error."
INTERNAL_ERROR_MODULE_NAME = "internal_error"


def _exception_text(exc: BaseException) -> str:
    detail = str(exc)
    return detail or exc.__class__.__name__


def build_public_task_error(exc: BaseException, debug_enabled: bool) -> str:
    if debug_enabled:
        return _exception_text(exc)
    return INTERNAL_TASK_ERROR_MESSAGE


def build_public_error_item(exc: BaseException, debug_enabled: bool) -> ErrorItem:
    if debug_enabled:
        return ErrorItem(
            component_type=DoclingComponentType.PIPELINE,
            module_name=exc.__class__.__name__,
            error_message=_exception_text(exc),
        )

    return ErrorItem(
        component_type=DoclingComponentType.PIPELINE,
        module_name=INTERNAL_ERROR_MODULE_NAME,
        error_message=INTERNAL_DOCUMENT_ERROR_MESSAGE,
    )


def render_public_error_list(
    errors: Sequence[ErrorItem],
    debug_enabled: bool,
) -> str | None:
    if not errors:
        return None

    rendered_errors: list[str] = []
    for error in errors:
        if debug_enabled and error.module_name:
            rendered_errors.append(f"{error.module_name}: {error.error_message}")
        else:
            rendered_errors.append(error.error_message)

    return "; ".join(rendered_errors) if rendered_errors else None
