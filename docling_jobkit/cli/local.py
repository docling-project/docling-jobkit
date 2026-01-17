from pathlib import Path
from typing import Annotated, Optional

import typer
import yaml
from pydantic import BaseModel, Field, ValidationError
from rich.console import Console

from docling_jobkit.connectors.source_processor_factory import get_source_processor
from docling_jobkit.connectors.target_processor_factory import get_target_processor
from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.convert.results_processor import ResultsProcessor
from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
from docling_jobkit.datamodel.task_sources import (
    TaskFileSource,
    TaskGoogleDriveSource,
    TaskHttpSource,
    TaskLocalPathSource,
    TaskS3Source,
)
from docling_jobkit.datamodel.task_targets import (
    GoogleDriveTarget,
    LocalPathTarget,
    S3Target,
    ZipTarget,
)

console = Console()
err_console = Console(stderr=True)


app = typer.Typer(
    name="Docling Jobkit Local",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)

JobTaskSource = Annotated[
    TaskFileSource
    | TaskHttpSource
    | TaskLocalPathSource
    | TaskS3Source
    | TaskGoogleDriveSource,
    Field(discriminator="kind"),
]

JobTaskTarget = Annotated[
    ZipTarget | LocalPathTarget | S3Target | GoogleDriveTarget,
    Field(discriminator="kind"),
]


class JobConfig(BaseModel):
    options: ConvertDocumentsOptions = ConvertDocumentsOptions()
    sources: list[JobTaskSource]
    target: JobTaskTarget


@app.command(no_args_is_help=True)
def convert(
    config_file: Annotated[
        Path,
        typer.Argument(
            help="Configuration file of the job", exists=True, readable=True
        ),
    ],
    artifacts_path: Annotated[
        Optional[Path],
        typer.Option(..., help="If provided, the location of the model artifacts."),
    ] = None,
    enable_remote_services: Annotated[
        bool,
        typer.Option(
            ..., help="Must be enabled when using models connecting to remote services."
        ),
    ] = False,
    allow_external_plugins: Annotated[
        bool,
        typer.Option(
            ..., help="Must be enabled for loading modules from third-party plugins."
        ),
    ] = False,
):
    # Open and validate config file
    try:
        with config_file.open("r") as f:
            raw_data = yaml.safe_load(f)
        config = JobConfig(**raw_data)
    except FileNotFoundError:
        typer.echo(f"❌ File not found: {config_file}")
    except ValidationError as e:
        typer.echo("❌ Validation failed:")
        typer.echo(e.json(indent=2))

    cm_config = DoclingConverterManagerConfig(
        artifacts_path=artifacts_path,
        enable_remote_services=enable_remote_services,
        allow_external_plugins=allow_external_plugins,
        options_cache_size=1,
    )
    manager = DoclingConverterManager(config=cm_config)

    results = []
    with get_target_processor(config.target) as target_processor:
        result_processor = ResultsProcessor(
            target_processor=target_processor,
            to_formats=[v.value for v in config.options.to_formats],
            generate_page_images=config.options.include_images,
            generate_picture_images=config.options.include_images,
        )
        for source in config.sources:
            with get_source_processor(source) as source_processor:
                for item in result_processor.process_documents(
                    manager.convert_documents(
                        sources=source_processor.iterate_documents(),
                        options=config.options,
                    )
                ):
                    results.append(item)


if __name__ == "__main__":
    app()
