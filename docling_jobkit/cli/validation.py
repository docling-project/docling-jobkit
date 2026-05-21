import typer

from docling.datamodel.service.targets import PresignedUrlTarget


def ensure_legacy_target_supported(target: object) -> None:
    if isinstance(target, PresignedUrlTarget):
        raise typer.BadParameter(
            "The local and multiproc CLIs do not support "
            "`presigned_url` targets. Use an orchestrator/server path with "
            "`target_config.s3_presigned` instead."
        )
