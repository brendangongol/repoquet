# repoquet 0.1.0

- Extracted the reusable repository engine from the CECORC workflow.
- Added schema-aware ingestion, generalized Hive partitions, source
  fingerprints, transactional manifests, resumable checkpoints, data
  contracts, repository auditing, and validated DuckDB view registration.
- Added a project scaffold, command-line workflow, reproducible dependency
  lockfile, continuous integration, and a 274-expectation regression suite.
- Added a user-guided schema workflow: detailed source observations are stored
  in Parquet, a compact Excel workbook exposes recommendations and type history,
  and approved decisions finalize into the existing writer catalog.
- Redesigned `SchemaReview.xlsx` around a `StartHere` dashboard. Only unresolved
  column and compatibility decisions remain visible; policy results are clearly
  informational and advanced registries/history are hidden by default.
- Hardened schema finalization against Excel inferring blank decision columns as
  numeric, and added explicit completion messages to empty review worksheets.
- Restored full column, compatibility, and type-history overview tabs after the
  action tabs, with a sheet-by-sheet guide on `StartHere`.
- Cross-database compatibility decisions now supersede narrower
  within-database decisions, and the generic schema-policy registry is an empty
  template rather than a collection of domain assumptions.
- CSV readers now preserve leading-zero fields by default, with a per-source
  `KeepLeadingZeros` reader option for explicit control.
- Enhanced atomic Parquet write operations with robust temp file cleanup: added
  `safe_unlink()` for retry-based file removal, `cleanup_temp_files()` for
  directory-level orphaned temp file recovery, and improved error handling in
  write retry logic to prevent temporary files from persisting after I/O failures
  (addresses file-locking issues on Windows and high-I/O environments).
- Phase 2: Integrated automatic temp file cleanup into `ParquetBackEndCreate()`
  with `AutoCleanup` parameter (default TRUE) and `CleanupAfterPhase` options
  ("all" after all writes, "database" after each batch, "none" to disable).
  This prevents orphaned `.tmp_*.parquet` files from accumulating without
  requiring manual cleanup calls.

