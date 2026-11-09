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
- Added resumable remote downloads: `MaterializeRemoteSources(..., Resume = TRUE)`
  (the default) now retries an interrupted large download from its last byte
  offset instead of restarting from scratch, when `curl` is available and no
  custom `DownloadFunction` is supplied. Partial bytes are kept in a
  deterministic `<cache file>.part` file distinct from the resolved cache path,
  so an incomplete download is never mistaken for a completed one. If the
  server does not honor the Range request, the partial file is discarded and
  the next attempt restarts cleanly.
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
  NHANES XPT files, all 57 standardized UCI Health and Medicine datasets, and
  all 26 credentialed MIMIC-III 1.4 tables. Access metadata keeps restricted
  MIMIC sources manual and explicit.
* Added `inst/extdata/DBSetupV2WithInternetDownload.xlsx`, preserving the
  current HCUP inventory and appending the complete curated public catalog.
* Remote acquisition now supports explicit ZIP members with atomic extraction,
  path-traversal protection, headerless delimited sources, and sectioned lookup
  files while preserving source files unchanged.
* Added SPSS load programs as column layouts. A source row can set
  `ReaderOptions` `{"LayoutFile": "<program>.SPS"}`: a `GET DATA /TYPE=TXT`
  program names the columns of a headerless delimited file (character versus
  numeric, plus the delimiter), and a `DATA LIST FIXED` program does the same
  for the new fixed-width `asc` file type. `RECODE ... =SYSMIS` lines become
  per-column special codes that load as `NA`, compared numerically. A relative
  `LayoutFile` resolves against `set_layout_root()` (stored in the
  `repoquet.layout_root` option and the `REPOQUET_LAYOUT_ROOT` environment
  variable); an absolute path is used as given. `ColumnNames` and `LayoutFile`
  must agree when both are set.
* The `asc` reader streams records in bounded chunks, checks every record
  against the layout's record length (reporting the line number and both
  lengths), accepts LF or CRLF endings, and returns text fields the way
  `haven::read_sav()` returns SPSS strings. Schema survey, preflight, chunked
  loading, source-defined partition routing, and manifest row counts all
  support it.
* Delimited chunks are now positioned by reading lines, not by `fread(skip =)`,
  which miscounts lines in files whose terminators are not a clean LF or CRLF
  (such as CR CR LF). Added `audit_chunk_positioning()`, which checks an existing
  manifest and Parquet store for sources loaded with the old positioning
  (repeated rows with the same total row count) without exporting data.
* Added retries for transient I/O failures, for loading from a network drive that
  drops out briefly. `ParquetBackEndCreate(IORetryAttempts = 3, IORetryPauseSeconds = 10)`
  (and the same keys in `repository_config.R`) control one shared policy: a failed
  Parquet write, checkpoint save, manifest write, or direct whole-file read is
  repeated after a pause (10 s, then 30 s with the defaults), and the isolated
  direct worker is relaunched, before the loader falls back to chunked reading.
  Memory, size, layout-file and timeout failures are never retried.
* A direct whole-file read is now checked against the independent row count
  (`count_rows`, or the SAV header). A read cut short at a line boundary is
  retried once, then falls back to the chunked reader, or fails clearly in
  direct mode, instead of loading short.
* The chunked delimited reader and the fixed-width stream now read the source as
  bytes and remember the byte offset where each batch of records starts. A batch
  cut short by a dropped connection (a record with too few fields or the wrong
  length, or an end of file before the expected row count) is retried from its
  own offset instead of failing the whole file, and a genuinely malformed file
  still fails clearly once the retries are used up. Batches reach `fread` as a
  temporary file, which is also several times faster on a wide table, and
  `normalize_character_encoding` no longer copies a wide table to rewrite names
  that did not change.
* An end of file that arrives before the file's known size is now a "premature
  end of file" error that is retried from the batch's byte offset, instead of
  being taken for the real end. Before, a read cut short by a dropped connection
  could leave a fragment of a record that `fread` reported as a "single-line
  footer", or, when the total row count was not known (the isolated chunk
  worker), could end the file early without any error. A footer fragment is also
  retried (once more, not three times), while a malformed record inside a batch
  still fails at once. The source is read in 16 MB blocks, since very large reads
  from a network share can come back short on Windows.
* A source that is not visible when a file starts loading is waited for with the
  same retry settings, but only when its root folder is also missing, which is
  what a dropped network drive looks like. A file missing under a visible root
  still fails at once, with no pause.
* The chunked delimited reader now remembers where a source ended. Before, the cursor
  that reached the end of a file was released and forgot its table of batch starts, so
  the read for "more records" after a partial last batch, and the streaming writer's
  probe for an empty batch, each rescanned the whole file from its start. On a 40 GB
  source that was about three minutes each, in the isolated chunk worker, where the
  total row count is unknown. A read at or past a remembered end now returns at once,
  unless the file's size, modification time or encoding changed or the read is a retry.
* The `[PARQUET STREAM RECOVERY]` log line now ends with `Cause:` and the error that
  forced the piece to restart, so an interrupted write can be diagnosed from the log.
* The `hcup` schema profile is now a shipped, ordered rule file,
  `inst/extdata/hcup_schema_rules.csv`, read by `build_default_schema_registry("hcup")`
  (the first matching row wins). It holds 57 rules: the 38 HCUP rules a project had
  been installing by hand, the built-in diagnosis, procedure, CPT and payer-text
  rules, and rules for the Epic variance decomposition tables (date keys as integers,
  other keys as 64-bit integers, ICD and DRG codes as text, `LocationEpicid` as text,
  0/1 flags and whole-number age and length of stay as integers, timestamps), which
  apply only to the `Epic` database. A timestamp rule never turns text into a
  timestamp; a column that was not read as one stays text.
* Two over-broad built-in `hcup` rules were retired: `^HOSP_[A-Z0-9]+$` typed hospital
  characteristics such as `HOSP_TEACH` and `HOSP_CONTROL` as text, and `^.*_CODE$` typed
  integer category codes in NTDB and TQP (for example `ANGIOGRAPHY_CODE`) as text.
  Narrower rules cover the hospital identifiers. A registry that already contains
  either rule keeps it; delete the row to adopt the change.
* `RepositoryInitialize(profile = "hcup")` now adds the shipped rules that an
  existing schema registry lacks. They are appended after the registry's own rows,
  so a person's rows, including an edited copy of a shipped rule, keep precedence.
  The registry is saved once, as `<name>.pre_hcup.<ext>`, before the first change,
  a registry that already holds every shipped rule is not rewritten, and a locked or
  unreadable registry only produces a warning. The `generic` profile is unchanged.
* A schema policy rule now resolves a column that the data supports. Before, a column
  with a matching rule still went to `ColumnDecisions` whenever the data alone was
  doubtful (no usable values, integers beyond exact floating-point precision, mixed
  evidence), even though the rule had declared its type. Such a column is now
  auto-approved and its reason says which rule declared the type. Reader warnings
  still ask for confirmation.
* A rule that the data contradicts (for example `DIED` declared `integer` but read as
  text, or `PAY1` read as numeric) is now a decision in `ColumnDecisions` instead of
  being kept silently. The data-derived type is retained until you choose Accept, or
  choose the rule's type as `ApprovedType` and Override. This surfaces a problem such as
  a categorical code stored as text in some years at survey time, instead of after a load.
* `SchemaReview.xlsx` has a new `RuleCoverage` tab with one row per rule: its order,
  pattern, scope, type, the columns it was the first match for, how many agreed, were
  promoted or were contradicted, and a status (a rule that matched nothing, or one that
  repeats an earlier pattern and scope, is called out). A last row counts the columns no
  rule covers. `PolicyReport` now says which of its rows need a decision.
