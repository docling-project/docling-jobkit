import logging
import multiprocessing as mp
import queue
import time
from pathlib import Path
from typing import Annotated, Any, Optional

import typer
import yaml
from pydantic import BaseModel, Field, ValidationError
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)

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
_log = logging.getLogger(__name__)

app = typer.Typer(
    name="Docling Jobkit Multiproc",
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


class BatchResult(BaseModel):
    """Result of processing a single batch"""

    chunk_index: int
    num_documents: int
    num_succeeded: int
    num_failed: int
    failed_documents: list[str]
    processing_time: float
    error_message: Optional[str] = None


def _load_config(config_file: Path) -> JobConfig:
    """Load and validate configuration file."""
    try:
        with config_file.open("r") as f:
            raw_data = yaml.safe_load(f)
        return JobConfig(**raw_data)
    except FileNotFoundError:
        err_console.print(f"[red]❌ File not found: {config_file}[/red]")
        raise typer.Exit(1)
    except ValidationError as e:
        err_console.print("[red]❌ Validation failed:[/red]")
        err_console.print(e.json(indent=2))
        raise typer.Exit(1)


def _process_source(
    source: JobTaskSource,
    source_idx: int,
    total_sources: int,
    config: JobConfig,
    batch_size: int,
    num_processes: int,
    artifacts_path: Optional[Path],
    enable_remote_services: bool,
    allow_external_plugins: bool,
    quiet: bool,
    log_level: int,
    progress_queue: Optional[Any] = None,
) -> list[BatchResult]:
    """Process a single source and return batch results."""
    if not quiet:
        console.print(
            f"[bold]Processing source {source_idx + 1}/{total_sources}[/bold]"
        )

    batch_results: list[BatchResult] = []

    with get_source_processor(source) as source_processor:
        # Check if source supports chunking
        try:
            chunks_iter = source_processor.iterate_document_chunks(batch_size)
        except RuntimeError as e:
            err_console.print(f"[red]❌ Source does not support chunking: {e}[/red]")
            err_console.print(
                "[yellow]Hint: Only S3 and Google Drive sources support batch processing[/yellow]"
            )
            raise typer.Exit(1)

        # Collect all chunks first to know total count
        chunks = list(chunks_iter)
        num_chunks = len(chunks)

        if num_chunks == 0:
            if not quiet:
                console.print("[yellow]No documents found in source[/yellow]")
            return batch_results

        # Calculate total number of documents across all chunks
        total_documents = sum(len(list(chunk.ids)) for chunk in chunks)

        if not quiet:
            console.print(
                f"Found {total_documents} documents in {num_chunks} batches to process"
            )

        # Prepare arguments for each batch
        batch_args = [
            (
                chunk.index,
                list(chunk.ids),
                source,
                config.target,
                config.options,
                artifacts_path,
                enable_remote_services,
                allow_external_plugins,
                log_level,
                progress_queue,
            )
            for chunk in chunks
        ]

        # Process batches in parallel with progress tracking
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            disable=quiet,
        ) as progress:
            task = progress.add_task("Processing documents...", total=total_documents)

            with mp.Pool(processes=num_processes) as pool:
                # Start async processing
                async_results = [
                    pool.apply_async(process_batch, args) for args in batch_args
                ]

                # Track which results have been collected
                collected_indices = set()
                completed_batches = 0

                # Monitor progress queue while batches are processing
                while completed_batches < len(batch_args):
                    try:
                        # Check for progress updates (non-blocking with timeout)
                        if progress_queue:
                            try:
                                msg = progress_queue.get(timeout=0.1)
                                if msg == "document_completed":
                                    progress.update(task, advance=1)
                            except queue.Empty:
                                pass

                        # Check if any batch has completed
                        for idx, async_result in enumerate(async_results):
                            if idx not in collected_indices and async_result.ready():
                                batch_result = async_result.get()
                                batch_results.append(batch_result)
                                collected_indices.add(idx)
                                completed_batches += 1
                    except KeyboardInterrupt:
                        pool.terminate()
                        raise

    return batch_results


def _display_summary(
    all_batch_results: list[BatchResult],
    overall_time: float,
    quiet: bool,
) -> None:
    """Display processing summary and failed documents."""
    total_batches = len(all_batch_results)
    total_documents = sum(r.num_documents for r in all_batch_results)
    total_succeeded = sum(r.num_succeeded for r in all_batch_results)
    total_failed = sum(r.num_failed for r in all_batch_results)

    if not quiet:
        console.print()
        console.print("[bold]Processing Summary[/bold]")
        console.print("=" * 50)

    console.print(f"Total Batches: {total_batches}")
    console.print(f"Total Documents: {total_documents}")
    console.print(f"Successful: {total_succeeded}")
    console.print(f"Failed: {total_failed}")
    console.print(f"Total Processing Time: {overall_time:.2f}s")
    if total_documents > 0:
        console.print(f"Average per Document: {overall_time / total_documents:.2f}s")

    # Display failed documents if any
    if total_failed > 0:
        if not quiet:
            console.print()
            console.print("[bold red]Failed Documents:[/bold red]")
        for batch_result in all_batch_results:
            if batch_result.num_failed > 0:
                if not quiet:
                    console.print(
                        f"\n[yellow]Batch {batch_result.chunk_index}:[/yellow]"
                    )
                if batch_result.error_message:
                    console.print(f"  Batch Error: {batch_result.error_message}")
                for failed_doc in batch_result.failed_documents:
                    console.print(f"  - {failed_doc}")

    if total_failed > 0:
        raise typer.Exit(1)


def process_batch(
    chunk_index: int,
    document_ids: list[Any],
    source: JobTaskSource,
    target: JobTaskTarget,
    options: ConvertDocumentsOptions,
    artifacts_path: Optional[Path],
    enable_remote_services: bool,
    allow_external_plugins: bool,
    log_level: int,
    progress_queue: Optional[Any] = None,
) -> BatchResult:
    """
    Process a single batch of documents in a subprocess.

    This function is executed in a separate process and handles:
    - Initializing source and target processors from config
    - Converting documents in the batch
    - Writing results to target
    - Tracking successes and failures

    Args:
        chunk_index: Index of this batch/chunk
        document_ids: List of document identifiers for this batch
        source: Source configuration
        target: Target configuration
        options: Conversion options
        artifacts_path: Optional path to model artifacts
        enable_remote_services: Whether to enable remote services
        allow_external_plugins: Whether to allow external plugins

    Returns:
        BatchResult with processing statistics and any errors
    """
    # Configure logging for this subprocess
    logging.basicConfig(level=log_level, force=True)
    logging.getLogger().setLevel(log_level)

    start_time = time.time()
    num_succeeded = 0
    num_failed = 0
    failed_documents: list[str] = []

    try:
        # Initialize converter manager
        cm_config = DoclingConverterManagerConfig(
            artifacts_path=artifacts_path,
            enable_remote_services=enable_remote_services,
            allow_external_plugins=allow_external_plugins,
            options_cache_size=1,
        )
        manager = DoclingConverterManager(config=cm_config)

        # Process documents in this batch using factories
        with get_source_processor(source) as source_processor:
            with get_target_processor(target) as target_processor:
                result_processor = ResultsProcessor(
                    target_processor=target_processor,
                    to_formats=[v.value for v in options.to_formats],
                    generate_page_images=options.include_images,
                    generate_picture_images=options.include_images,
                )

                # Get a new chunk with the same document IDs
                # This recreates the chunk in the subprocess context
                chunk = None
                for c in source_processor.iterate_document_chunks(len(document_ids)):
                    # Find the chunk with matching IDs
                    if list(c.ids) == document_ids:
                        chunk = c
                        break

                if chunk is None:
                    raise RuntimeError(
                        f"Could not find documents for batch {chunk_index} with IDs: {document_ids}"
                    )

                # Use the chunk's iter_documents method to get documents
                documents = list(chunk.iter_documents())

                # Convert and process documents
                for item in result_processor.process_documents(
                    manager.convert_documents(
                        sources=documents,
                        options=options,
                    )
                ):
                    if "SUCCESS" in item:
                        num_succeeded += 1
                    else:
                        num_failed += 1
                        failed_documents.append(item)

                    # Send progress update after each document
                    if progress_queue:
                        progress_queue.put("document_completed")

        processing_time = time.time() - start_time

        return BatchResult(
            chunk_index=chunk_index,
            num_documents=len(document_ids),
            num_succeeded=num_succeeded,
            num_failed=num_failed,
            failed_documents=failed_documents,
            processing_time=processing_time,
        )

    except Exception as e:
        processing_time = time.time() - start_time
        _log.error(f"Batch {chunk_index} failed with error: {e}")
        return BatchResult(
            chunk_index=chunk_index,
            num_documents=len(document_ids),
            num_succeeded=num_succeeded,
            num_failed=len(document_ids) - num_succeeded,
            failed_documents=failed_documents or [f"Batch error: {e!s}"],
            processing_time=processing_time,
            error_message=str(e),
        )


@app.command(no_args_is_help=True)
def convert(
    config_file: Annotated[
        Path,
        typer.Argument(
            help="Configuration file of the job", exists=True, readable=True
        ),
    ],
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            "-b",
            help="Number of documents to process in each batch",
        ),
    ] = 10,
    num_processes: Annotated[
        Optional[int],
        typer.Option(
            "--num-processes",
            "-n",
            help="Number of parallel processes (default: 4 or less depending on CPU count)",
        ),
    ] = None,
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
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress progress bar and detailed output",
        ),
    ] = False,
    verbose: Annotated[
        int,
        typer.Option(
            "--verbose",
            "-v",
            count=True,
            help="Set the verbosity level. -v for info logging, -vv for debug logging.",
        ),
    ] = 0,
):
    """
    Convert documents using multiprocessing for parallel batch processing.

    Each batch of documents is processed in a separate subprocess, allowing
    for efficient parallel processing of large document collections.
    """
    # Configure logging based on verbosity level
    # Default: WARNING (no -v flag)
    # -v: INFO level
    # -vv or more: DEBUG level
    if verbose == 0:
        log_level = logging.WARNING
    elif verbose == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level, force=True)
    logging.getLogger().setLevel(log_level)

    # Determine number of processes
    if num_processes is None:
        num_processes = min(mp.cpu_count(), 4)

    if not quiet:
        console.print("[bold blue]Docling Jobkit Multiproc[/bold blue]")
        console.print(f"Batch size: {batch_size}")
        console.print(f"Number of processes: {num_processes}")
        console.print()

    # Load and validate config file
    config = _load_config(config_file)

    # Create a queue for progress updates from worker processes
    manager = mp.Manager()
    progress_queue = manager.Queue()

    # Process each source
    all_batch_results: list[BatchResult] = []
    overall_start_time = time.time()

    for source_idx, source in enumerate(config.sources):
        batch_results = _process_source(
            source=source,
            source_idx=source_idx,
            total_sources=len(config.sources),
            config=config,
            batch_size=batch_size,
            num_processes=num_processes,
            artifacts_path=artifacts_path,
            enable_remote_services=enable_remote_services,
            allow_external_plugins=allow_external_plugins,
            quiet=quiet,
            log_level=log_level,
            progress_queue=progress_queue,
        )
        all_batch_results.extend(batch_results)

    overall_time = time.time() - overall_start_time

    # Display summary
    _display_summary(all_batch_results, overall_time, quiet)


if __name__ == "__main__":
    app()
