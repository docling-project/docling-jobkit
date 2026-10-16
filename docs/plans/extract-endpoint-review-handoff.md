# Extraction execution finalization

Updated 2026-10-01. Implementation is complete on `cau/extract-endpoint` ([PR #249](https://github.com/docling-project/docling-jobkit/pull/249)). The current Docling service contract is top-level `extraction_target`, operational `options`, and a separate output `target`.

## Done

Source identity/expansion, stable extractor caching, operator presets, Ray dispatch, JSON-safe item/document results, storage/presigned routing, exact document counts and the shared callback lifecycle are implemented. The old S3 result-construction bug is fixed. Debug setting propagation is committed in `af845006`. Published October 1 fixes include scoped item failure reasons in callbacks without mutating durable item errors.

Run `tests/test_extraction_manager.py` and `tests/test_presigned_target_results.py`; they cover manager policy, envelopes, identity, artifact isolation, export failures and callbacks. The October 1 item-reason regression is in the manager suite. Installed published Core is sufficient. Main is integrated, the branch and refreshed Docling lock are pushed, and DCO plus Python 3.10–3.14 CI pass at `6308522e`. The focused extraction/storage/manager selection passed 132 tests.

## Finalize

1. Coordinate with #253's all-failed terminal-status change; retain retrieval of structured failed extraction results and item reasons.
2. After Docling publication, raise the required Docling version and remove the Git branch source; publish Jobkit before Serve updates its dependency floor.
3. Scope extraction work-unit callbacks/metrics separately with the SaaS billing owner. Lifecycle and error reporting exist; billing work fields are still absent.

The cross-repository finalization assessment is in Docling #4218's `docs/plans/extraction-api-finalization.md`.
