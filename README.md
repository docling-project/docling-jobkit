# Docling Jobkit

Running a distributed job processing documents with Docling.


## How to use it

### Local Multiprocessing CLI

The `docling-jobkit-multiproc` CLI enables parallel batch processing of documents using Python's multiprocessing. Each batch of documents is processed in a separate subprocess, allowing efficient parallel processing on a single machine.

#### Usage

```bash
# Basic usage with default settings (batch_size=10, num_processes=CPU count)
docling-jobkit-multiproc config.yaml

# Custom batch size and number of processes
docling-jobkit-multiproc config.yaml --batch-size 20 --num-processes 4

# With model artifacts
docling-jobkit-multiproc config.yaml --artifacts-path /path/to/models

# Quiet mode (suppress progress bar)
docling-jobkit-multiproc config.yaml --quiet

# Full options
docling-jobkit-multiproc config.yaml \
  --batch-size 30 \
  --num-processes 8 \
  --artifacts-path /path/to/models \
  --enable-remote-services \
  --allow-external-plugins
```

#### Configuration

The configuration file format is the same as `docling-jobkit-local`. See example configurations:
- S3 source/target: `dev/configs/run_multiproc_s3_example.yaml`
- Local path source/target: `dev/configs/run_local_folder_example.yaml`

**Note:** Only S3, Google Drive, and local_path sources support batch processing. File and HTTP sources do not support chunking.

#### CLI Options

- `--batch-size, -b`: Number of documents to process in each batch (default: 10)
- `--num-processes, -n`: Number of parallel processes (default: CPU count)
- `--artifacts-path`: Path to model artifacts directory
- `--enable-remote-services`: Enable models connecting to remote services
- `--allow-external-plugins`: Enable loading modules from third-party plugins
- `--quiet, -q`: Suppress progress bar and detailed output

### Local Sequential CLI

The `docling-jobkit-local` CLI processes documents sequentially in a single process.

```bash
docling-jobkit-local config.yaml
```

### Using Local Path Sources and Targets

Both CLIs support local file system sources and targets. Example configuration:

```yaml
sources:
  - kind: local_path
    path: ./input_documents/
    recursive: true  # optional, default true
    pattern: "*.pdf"  # optional glob pattern

target:
  kind: local_path
  path: ./output_documents/
```

See `dev/configs/run_local_folder_example.yaml` for a complete example.

### Extraction jobs

Extraction execution uses the Ray orchestrator. Local/RQ workers do not execute
extraction tasks. Pass `ExtractDocumentsOptions.target` as extraction guidance;
the task's `target` independently selects in-body results or JSON artifact storage.

```python
from docling.datamodel.extraction import ExtractionTarget, ExtractionTemplate
from docling.datamodel.service.options import ExtractDocumentsOptions

options = ExtractDocumentsOptions(
    target=ExtractionTarget(
        template=ExtractionTemplate(format="nuextract", value={"total": "number"}),
        instructions="Extract the invoice total.",
    ),
    extraction_preset="nuextract_2b",
    output_mode="prompt_only",
    page_range=(5, 8),
)
```

Generic models use tagged `example_json` guidance or an explicit `output_schema`.
`schema_constrained` requires a supported model and explicitly configured vLLM API
engine. The service output mode overrides custom model configuration. Jobkit forwards
targets to Docling and caches only stable model/engine/channel/mode configuration.
Docling owns target preparation, automatic page/document requests and schema validation.

In-body `ExtractionTaskResult.documents` and JSON artifacts share the
`ExtractionDocumentResult` envelope: original request `source_index`, expanded
`source_uri`, filename, status/errors and canonical `items`. Each item retains
absolute page or document scope, data/raw answer, validation status and inference
metadata. The runtime input/backend is never serialized. Unreleased bare-template
and service `pages` shapes were replaced directly; the legacy SDK stays compatible.

This branch requires the updated Docling extraction service contract. Downstream
release metadata must raise the Docling minimum to the first published release
containing that contract before release; source `PYTHONPATH` is only a development
validation route. No local path dependency pin is needed.

## Get help and support

Please feel free to connect with us using the [discussion section](https://github.com/docling-project/docling/discussions) of the main [Docling repository](https://github.com/docling-project/docling).

## Contributing

Please read [Contributing to Docling Serve](https://github.com/docling-project/docling-jobkit/blob/main/CONTRIBUTING.md) for details.

## References

If you use Docling in your projects, please consider citing the following:

```bib
@techreport{Docling,
  author = {Deep Search Team},
  month = {1},
  title = {Docling: An Efficient Open-Source Toolkit for AI-driven Document Conversion},
  url = {https://arxiv.org/abs/2501.17887},
  eprint = {2501.17887},
  doi = {10.48550/arXiv.2501.17887},
  version = {2.0.0},
  year = {2025}
}
```

## License

The Docling Serve codebase is under MIT license.

## LF AI & Data

Docling is hosted as a project in the [LF AI & Data Foundation](https://lfaidata.foundation/projects/).

### IBM ❤️ Open Source AI

The project was started by the AI for Knowledge team at IBM Research Zurich.
