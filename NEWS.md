# repoquet 0.1.0

- Extracted the reusable repository engine from the CECORC workflow.
- Added schema-aware ingestion, generalized Hive partitions, source
  fingerprints, transactional manifests, resumable checkpoints, data
  contracts, repository auditing, and validated DuckDB view registration.
- Added optional direct HTTP/HTTPS source acquisition through `SourceURI`,
  atomic managed caching, refresh policies, offline/manual modes, SHA-256
  verification, and remote provenance in repository metadata. Source files and
  inventory workbooks remain read-only.
- Added a project scaffold, command-line workflow, reproducible dependency
  lockfile, continuous integration, and a 274-expectation regression suite.
- Added a user-guided schema workflow: detailed source observations are stored
  in Parquet, a compact Excel workbook exposes recommendations and type history,
  and approved decisions finalize into the existing writer catalog.
- Added adaptive schema surveys, bounded clean-file `fread` profiling,
  per-source resumable observation caches, bounded low-cardinality previews,
  and configurable schema worker/export limits.
- Added optional conditional HTTP revalidation for `if_changed` sources when
  `curl` is available, while preserving full-download hash comparison as the
  compatibility fallback.
- Redesigned `SchemaReview.xlsx` around a `StartHere` dashboard. Only unresolved
  column and compatibility decisions remain visible; policy results are clearly
  informational and advanced registries/history are hidden by default.
- Hardened schema finalization against Excel inferring blank decision columns as
  numeric, and added explicit completion messages to empty review worksheets.
- Restored full column, compatibility, and type-history overview tabs after the
  action tabs, with a sheet-by-sheet guide on `StartHere`.
- Added partition-aware semantic dictionaries. Labeled source formats retain
  exact variable/value-label evidence in `SchemaObservations.parquet`; stable
  meanings are automatic, conflicts are resolved in `DictionaryReview`, and
  approved mappings are written to `ColumnDictionary` with query helpers.
- Made `PartitionKey` and `PartitionValue` mandatory and authoritative for
  partition placement and checkpoint identity. Removed the legacy `Year`
  fallback and stopped synthesizing physical `YEAR` columns during ingestion.
- Added an atomic `RepositoryMetadata.xlsx` presentation snapshot with table,
  run, issue, field-guide, and paginated raw-manifest sheets. The DuckDB
  manifest remains authoritative and Excel export failures do not invalidate a
  completed repository load.
- Corrected `load_repository_config()` runtime precedence. Users can now
  override file-defined resource settings and required paths with explicit
  arguments or a named `overrides` list without editing the configuration file.
- Reconciled the generic README, generated runner, CLI, and HCUP reference
  around one documented seven-stage production workflow. Every entry point now
  creates and propagates `new_repository_run_id()` for traceable operations;
  HCUP-specific policies, tuning, and analytical extensions remain explicit.
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
* Added curated real-world repository profiles for the MIMIC-III demo, NHANES
  demographic cycles, UCI healthcare datasets, and ClinVar summaries.
* Expanded the comprehensive real-world catalog to all 1,593 public continuous
  NHANES XPT files, all 58 standardized UCI Health and Medicine datasets, and
  all 26 credentialed MIMIC-III 1.4 tables. Access metadata keeps restricted
  MIMIC sources manual and explicit.
* Added `inst/extdata/DBSetupV2WithInternetDownload.xlsx`, preserving the
  current HCUP inventory and appending the complete curated public catalog.
* Remote acquisition now supports explicit ZIP members with atomic extraction,
  path-traversal protection, headerless delimited sources, and sectioned lookup
  files while preserving source files unchanged.
