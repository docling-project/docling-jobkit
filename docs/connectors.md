# Writing a connector

A **connector** lets docling-jobkit read documents from (source) or write results
to (target) a storage system. Connectors are discovered as plugins, so a new one
works identically when you test it locally with the CLI and when it runs on
distributed compute (Ray / RQ), with no changes to the core dispatch or export
code.

This guide uses a hypothetical **OneDrive** target connector as the example.

## How dispatch works

- Every connector has a **config model** — a small `pydantic` model with a
  `Literal` `kind` field that identifies it in YAML/JSON.
- Every connector has a **processor** — a subclass of `BaseSourceProcessor` or
  `BaseTargetProcessor` that does the actual I/O.
- A processor declares which config models it handles via `get_config_types()`.
- The pluggy-based factory (`docling_jobkit.connectors.connector_factory`) keys a
  registry on the config type and instantiates the right processor from a config
  object — `get_source_processor()` / `get_target_processor()` are thin wrappers
  over it.
- Connectors are found through the `docling_jobkit` setuptools entry-point group.

## Checklist

### 1. Config model with a `kind`

```python
from typing import Literal
from pydantic import BaseModel

class OneDriveTarget(BaseModel):
    kind: Literal["onedrive"] = "onedrive"
    drive_id: str
    folder: str = ""
```

The `kind` must be unique within its registry (source kinds and target kinds are
separate registries, so a source and a target may share a `kind`).

### 2. Processor subclass

Implement the base abstract methods and `get_config_types()`. **Import heavy or
optional SDKs inside methods, not at module top**, so listing the class in a
plugin module stays import-safe even when the SDK isn't installed. This also
matters because chunks are shipped between processes fetcher-stripped: each worker
reconstructs its own processor from the config and fetches lazily.

```python
from pydantic import BaseModel
from docling_jobkit.connectors.target_processor import BaseTargetProcessor

class OneDriveTargetProcessor(BaseTargetProcessor):
    def __init__(self, target: OneDriveTarget):
        super().__init__()
        self._target = target

    @classmethod
    def get_config_types(cls) -> tuple[type[BaseModel], ...]:
        return (OneDriveTarget,)

    def _initialize(self) -> None:
        from onedrive_sdk import Client          # lazy import
        self._client = Client(self._target.drive_id)

    def _finalize(self) -> None:
        ...

    def upload_file(self, filename, target_filename, content_type) -> None:
        self._client.upload(self._target.folder, target_filename, open(filename, "rb"))

    def upload_object(self, obj, target_filename, content_type) -> None:
        self._client.upload(self._target.folder, target_filename, obj)
```

Source connectors instead subclass `BaseSourceProcessor` and implement
`_fetch_documents` (and, for batch/chunked processing, `_list_document_ids` +
`_fetch_document_by_id`). See `s3_source_processor.py` for a full example.

### 3. Plugin module

Expose `source_connectors()` and/or `target_connectors()` callables returning the
classes. Keep the imports inside the functions.

```python
# my_pkg/plugin.py
def target_connectors():
    from my_pkg.onedrive import OneDriveTargetProcessor
    return {"target_connectors": [OneDriveTargetProcessor]}
```

### 4. Entry point

Register the plugin module under the `docling_jobkit` group in your package's
`pyproject.toml`:

```toml
[project.entry-points.docling_jobkit]
my_onedrive = "my_pkg.plugin"
```

After installing your package, the connector is discoverable.

### 5. Enable external plugins

External (non–docling-jobkit) plugins load only when external plugins are allowed:

- **CLI:** `docling-jobkit-local convert config.yaml --allow-external-plugins`
  (and the multiproc CLI likewise). The config then validates `kind: onedrive`
  from YAML.
- **Orchestrators:** set `allow_external_plugins=True` on the converter manager
  config. For the RQ worker, the dynamic union must be installed *before* the
  worker reconstructs `Task` objects — call
  `docling_jobkit.datamodel.dynamic_unions.install_dynamic_unions(allow_external_plugins=True)`
  at worker bootstrap (with the same flag as the submitter).

## Restricting targets per orchestrator

An orchestrator can declare which target kinds it accepts via
`allowed_target_kinds` on its config (e.g. `RayOrchestratorConfig`,
`RQOrchestratorConfig`, `LocalOrchestratorConfig`). A submission with a
disallowed target kind fails fast at `enqueue` with `TargetNotAllowedError`.
`None` (the default) allows every registered target. This is how a distributed
deployment can, for example, forbid `local_path` targets that only make sense on
a single machine.

## Memory note

Documents are fetched one at a time as the converter pulls them (see
`open_chunk_sources`), and each result is released immediately after its artifacts
are uploaded (see `export_documents_to_target`). Keep your source connector's
`fetch_converter_source_by_ref` streaming a single document per call rather than
materializing whole batches, so per-worker memory stays flat regardless of batch
size.
