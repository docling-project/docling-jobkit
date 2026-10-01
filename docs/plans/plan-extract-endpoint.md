# Extraction execution: implemented

Updated 2026-10-01. [Jobkit PR #249](https://github.com/docling-project/docling-jobkit/pull/249) consumes [Docling #4218](https://github.com/docling-project/docling/pull/4218); Serve's HTTP layer is [#695](https://github.com/docling-project/docling-serve/pull/695). The original template-based design is superseded by the implementation below.

- Tasks carry `TaskType.EXTRACT`, `extract_target` and operational `extract_options`.
- The manager forwards `target=` to Docling and caches stable model/pipeline configuration, not caller schema/template/instructions.
- Operator presets respect preset/engine/remote-service policy independently of client custom-config permission.
- Source expansion preserves original request index and expanded source identity.
- Ray executes extraction; Local/RQ retain task plumbing but Serve rejects execution with 501.
- Durable JSON envelopes retain document/item errors, scopes, validation and inference metadata. In-body, presigned and direct artifact result routing are implemented. Target-write failure becomes a document failure.
- Uploads precede document callbacks; independent callback threads do not guarantee arrival order. Item failure reasons are projected into callbacks by the published October 1 fixes.

See [the review/finalization handoff](extract-endpoint-review-handoff.md) for tests and remaining delivery work. No custom Core checkout is required. Raise dependency floors and remove the temporary Docling Git branch source after publication.
