# repoquet

`repoquet` is a schema-aware, self-auditing engine that transforms heterogeneous
source files into refreshable, query-ready Parquet repositories for
larger-than-memory analytics.

Its distinguishing feature is that ingestion, schema normalization, refresh
detection, validation, and repository reconciliation are one coordinated
workflow rather than separate scripts. The loader supports SPSS, Stata, SAS,
transport, delimited, Parquet, and RDS sources and exposes validated tables
through DuckDB.

repoquet's distinctive capability is the end-to-end creation of analysis-ready
Parquet repositories from heterogeneous source files that may exceed available
RAM. It processes supported SAV and delimited files in memory-bounded chunks,
preserves source dictionary metadata, reconciles type drift across chunks and
table releases, and writes consistent Hive-partitioned schemas with resumable
checkpoints and validation. The resulting DuckDB-ready backend reduces storage
and memory pressure, accelerates filtering, aggregation, joins, and
cross-release analysis, and remains accessible from both R and Python without
loading the complete repository into memory.


## Why R for database generation?

repoquet uses R because its vectorized data model and explicit coercion behavior
make schema enforcement easier to apply consistently during ingestion. The
pipeline validates and aligns column types before writing, so type drift is
surfaced early instead of propagating across Parquet files and partitions.
Although Python ecosystems can also enforce strict schemas, many common
ingestion patterns are permissive by default and require additional validation
to prevent mixed or inconsistent types. By enforcing a strongly typed Parquet
schema at build time, repoquet produces language-neutral datasets that can be
used reliably from R, Python, DuckDB, Arrow, and other compatible tools.


## Install

```r
install.packages("remotes")
remotes::install_github("brendangongol/repoquet")
```

For a reproducible development environment after cloning the repository, run:

```text
Rscript tools/bootstrap-renv.R
R CMD INSTALL .
```

During active development, the complete implementation lives in
`R/repoquet.R`.

## Create A Project

While the package is under active development, source the current implementation
from the cloned repository so every workflow run uses the latest functions:

```r
source("R/repoquet.R")
paths <- create_repository_project("~/my_repository", profile = "generic")
```

The deployment workflow will switch this line to `library(repoquet)` after the
package is ready to install and version as a release.

The scaffold creates:

- `repository_config.R`: machine-specific paths and resource limits
- `DBSetup.xlsx`: one source file per row
- `formatted/Schema/SchemaRegistry.xlsx`: canonical type policies
- `formatted/Schema/DataContracts.xlsx`: content validation rules
- `formatted/SourceCache`: managed local copies of optional remote sources
- `formatted/Manifest/RepositoryMetadata.duckdb`: transactional manifest
- `formatted/Manifest/RepositoryMetadata.xlsx`: accessible, read-only metadata snapshot
- `run_repository.R`: validation, catalog, loading, DuckDB, and audit workflow

Configuration files provide durable machine defaults, but every setting can be
changed for one run without editing the file. Explicit arguments have the
highest precedence; a named `overrides` list is useful when settings are built
programmatically:

```r
cfg <- load_repository_config("repository_config.R",
                              n_workers = 1L,
                              PartitionBy = "RAMEstimate",
                              overrides = list(RAMThreshold = 16, DuckDB_GB = "6GB"))
```

Precedence is: package defaults, configuration file, `overrides`, then explicit
arguments. Runtime overrides change only the returned list and never modify
`repository_config.R`.


## DBSetup Workbook

Required columns are `Database`, `MDBDir`, `Path`, `TableName`, `FileType`,
`PartitionKey`, and `PartitionValue`. The two partition fields are the sole
partition metadata and must be populated on every row. For example,
`PartitionKey = "year"` and `PartitionValue = "2024"` creates `year=2024`,
and DuckDB exposes `YEAR` from the directory. A legacy workbook `Year` column
is neither required nor used as a fallback.

Optional general-purpose columns include:

- `PartitionType`: canonical type for each partition key, separated by `;`
- `PhysicalTableName`: explicit Parquet directory and DuckDB view name
- `Encoding`, `Delimiter`, `Quote`, `NAStrings`, and `DecimalMark`
- `DateFormat`, `DateTimeFormat`, and `Timezone`
- `MalformedRowPolicy`, `ContinuationColumn`, and `ContinuationJoin` for an
  explicitly verified unquoted continuation line
- `ReaderOptions`: a JSON object passed to the selected reader
- `AcceptPartial`: explicit acceptance of a verified truncated SAV source
- `SourceURI`: optional direct HTTP/HTTPS file or API download URL
- `DownloadPolicy`: `if_missing` (default), `if_changed`, `always`, or `manual`
- `ExpectedSHA256`: optional 64-character hash used to verify downloaded bytes
- `ArchiveType` and `ArchiveMember`: an explicit ZIP member to extract into the
  managed cache without changing the downloaded archive

Nested partitions use matching semicolon-separated values, for example
`PartitionKey = "SITE;YEAR"`, `PartitionValue = "MGH;2024"`, and
`PartitionType = "character;integer"`.

`ValidateMDTPreflight()` checks partition definitions, physical table identity,
output filename collisions, reader options, custom file types, and remote-source
metadata before any Parquet is written.

`SourceURI` is for a direct downloadable object or supported ZIP archive, not a
page that must be scraped. `Path` remains required and supplies the stable logical filename and
extension used by the reader. Before schema discovery,
`MaterializeRemoteSources()` atomically copies remote bytes into
`formatted/SourceCache`; the rest of the pipeline reads that local managed copy
with the same direct or chunked readers used for local data. Original sources
and `DBSetup.xlsx` are never changed. `if_changed` downloads to a temporary
`.part` file, compares SHA-256, and replaces the cache only when content differs.
`manual` and `Offline = TRUE` require an existing cache entry. Do not place
passwords or tokens in the workbook; pass a caller-managed `DownloadFunction`
when authentication is required.

ZIP extraction is opt-in and deterministic: `ArchiveMember` must name one safe
relative member, and both the archive download and extracted file are replaced
atomically. Headerless delimited data can declare `Header` and `ColumnNames` in
`ReaderOptions`; a small multi-section lookup file can declare `SectionHeader`.


## Minimal Runnable Example

`generate_example_repository()` creates a complete synthetic project containing
year-partitioned CSV data, site-partitioned CSV data, a labeled Stata source,
`DBSetup.xlsx`, `repository_config.R`, and the normal repository directory
structure. It does not read or modify any user source files.

Run this from the root of the cloned repository during development:

```r
source("R/repoquet.R")

example <- generate_example_repository("~/repoquet_example")
cfg <- load_repository_config(example$ConfigPath)
paths <- RepositoryInitialize(cfg$FormattedDBPath, profile = "generic")

example
```

The returned `example` list identifies the generated configuration, inventory,
runner, schema, manifest, log, checkpoint, and Parquet paths. Continue with the
seven stages below to survey the synthetic sources, review the recommended
schema, load Parquet, register DuckDB views, and audit the finished repository.
Schema decisions remain an intentional human review step.

### Public Real-World Examples

`generate_real_world_repository()` writes a runnable `DBSetup.xlsx` containing
curated official sources without downloading them by default. Profiles include
the complete open 26-table MIMIC-III demo, metadata for all 26 tables in the
credentialed MIMIC-III 1.4 release, all 1,593 public continuous NHANES transport
files across demographics, dietary, examination, laboratory, and questionnaire
components, all 58 standardized datasets currently returned by UCI's Health and
Medicine API catalog, and current ClinVar summaries useful for atherosclerosis
and cerebral cavernous malformation discovery.

DBSetup.xlsx loaded into `MDT` drives hive partitioning with two columns:
  PartitionKey   -- hive key name(s) for each file's partition dir
  PartitionValue -- the value(s) that file belongs to the PartitionKey
These columns are required on every row. 
Example year partitioning: 
  PartitionKey = "year", PartitionValue = "2019" -> parquet/<DB>_<Table>/year=2019/*.parquet
Example site partitioning:          
  PartitionKey = "SITE", PartitionValue = "MGH" -> parquet/<DB>_<Table>/site=MGH/*.parquet
Nested partitions use ";" in both:
  PartitionKey = "SITE;YEAR", PartitionValue = "MGH;2019" -> parquet/<DB>_<Table>/site=MGH/year=2019/*.parquet

Checkpoint identities are used to monitor unique table writed and are derived from PartitionKey and 
PartitionValue. This needs to be unique to avoid over-writing tables. This is enforced by
ValidateMDTPreflight below before anything is written. 

```r
public_example <- generate_real_world_repository("~/repoquet_public_example", profile = "quick", Download = FALSE)
# Review DBSetup.xlsx, then materialize its declared sources:
MDT <- openxlsx::read.xlsx(public_example$MDTPath, sheet = "Sheet1")
ValidateMDTPreflight(MDT = MDT, strict = TRUE, logStatus = TRUE,
                     ParquetBasePath = paths$ParquetBasePath,
                     MaxFileStemTruncate = TRUE,
                     TerminalHivePartition = FALSE,
                     MasterDBPath = cfg$MasterDBPath,
                     LogPath = paths$LogPath, RunId = RunId)
MDT <- MaterializeRemoteSources(MDT, public_example$DownloadCachePath)
```

Use `profile = "comprehensive"` (or `"all"`) to inventory every source.
Credentialed MIMIC-III rows use `DownloadPolicy = "manual"`: users must obtain
PhysioNet authorization and pre-stage the original `.csv.gz` files in the
managed cache. repoquet never bypasses access controls or modifies source files.
To reveal each deterministic cache destination without downloading, call
`MaterializeRemoteSources(MDT, DownloadCachePath, Offline = TRUE,
Strict = FALSE)` and inspect `ResolvedSourcePath` for the credentialed rows.
Users should review source licenses, citations, download size, and NHANES
survey-design requirements before analysis. The synthetic generator remains the
recommended offline smoke test.


## Canonical Workflow

repoquet follows one seven-step sequence in both the complete generic example
below and the domain-specific production reference script at
`inst/examples/healthcare/CECORC_loader_reference.R`.

The two workflows share the same operational contract:

| Stage | Capability | What makes it useful |
|---|---|---|
| 1. Initialize | Configuration, repository paths, and one `RunId` | Every log, validation, manifest, and audit event is traceable to one execution |
| 2. Validate | Inventory, source, reader, partition, naming, and collision checks | Invalid work is rejected before any repository data is written |
| 3. Survey | Reader-consistent schema and label evidence in Parquet | Recommendations come from the actual sources without rewriting them or loading the entire repository into Excel |
| 4. Finalize | Human review of ambiguous types, compatibility, and dictionaries | Safe coercions are automated while potentially lossy decisions remain under user control |
| 5. Load | Reviewed schemas, generalized Hive partitions, chunking, fingerprints, checkpoints, and transactional metadata | Heterogeneous and larger-than-memory sources become resumable, refresh-aware Parquet tables |
| 6. Register | Strictly validated DuckDB views and declarative data contracts | Cross-table querying fails early on incompatible schemas or violated content rules |
| 7. Reconcile | Non-destructive comparison of inventory, checkpoint, manifest, Parquet, and DuckDB | Repository drift is visible and repairable instead of silently accumulating |

The generic example derives all behavior from synthetic data and an empty
policy profile. The healthcare reference follows the same stages but supplies
HCUP source paths, the opt-in `hcup` policy profile, domain-specific resource
tuning, data dictionaries, and survey-weighted analysis examples.

## Complete Workflow Example

This example uses the built-in synthetic repository, but exercises the same
schema review, Parquet loading, DuckDB registration, and reconciliation path as
a production repository. During active development, source the implementation
from the cloned repository; replace that line with `library(repoquet)` after
installing a released package.

### 1. Initialize The Run

`generate_example_repository()` creates representative source files and a
configured project without requiring external data. `RepositoryInitialize()`
creates and returns every repository path. `new_repository_run_id()` must be
called once per execution and the resulting `RunId` passed through the workflow
so log, manifest, validation, and audit records can be traced to the same run.

```r
source("R/repoquet.R")
example <- generate_example_repository("~/my_repository_example")
cfg <- load_repository_config(example$ConfigPath)
paths <- RepositoryInitialize(cfg$FormattedDBPath, profile = "generic")
RunId <- new_repository_run_id()
MDT <- openxlsx::read.xlsx(cfg$MDTPath, sheet = "Sheet1")
DBLoad <- sort(unique(MDT$Database))
```

### 2. Validate The Source Inventory

`ValidateMDTPreflight()` is the fail-fast structural gate before any network or
repository write. It checks required fields, remote-source declarations, reader
settings, partition definitions,
table naming collisions, output filename collisions, and supported formats.
Partition identity comes only from `PartitionKey` and `PartitionValue`; a
physical or legacy `Year` field is not used as a fallback. `MDTCompleteStatus()`
then reports which inventory rows remain pending according to the checkpoint.
Between those steps, `MaterializeRemoteSources()` resolves optional direct
HTTP/HTTPS sources to the managed cache; local rows pass through unchanged.

```r
ValidateMDTPreflight(
  MDT = MDT,
  strict = TRUE,
  ParquetBasePath = paths$ParquetBasePath,
  MaxFileStemTruncate = TRUE,
  TerminalHivePartition = FALSE,
  MasterDBPath = cfg$MasterDBPath,
  LogPath = paths$LogPath,
  RunId = RunId
)
```

Changing a row's \code{TableName} in the workbook (e.g. after a case-collision preflight error)
changes that row's checkpoint identity, so already-loaded files would be
re-ingested. This helper rewrites the affected checkpoint entries (both the
generalized and legacy key formats) and the manifest's TableName/DuckDBTable
fields in place, so completed files stay completed under the new name.
Run it once on the loading machine after editing the workbook; the MDT you
pass must already carry the NEW TableName. It's recommended to fun with 
DryRun = TRUE first to monitor what will be changed before running DryRun = FALSE
to make changes. 

```r
rename_checkpoint_table(CheckpointPath, MDT, "NRD", "CORE", "Core", RepositoryPaths$ManifestPath, DryRun = TRUE)
# Optionally, once loaded checkpoints are migrated to generalized keys:
migrate_checkpoint_keys(CheckpointPath, MDT, DryRun = TRUE)
```

Optional: remote acquisition. 
Download data from the internet using the SourceURI column in MDT. 
The SourceURI must be a direct HTTP/HTTPS file URL; Path remains its stable logical filename. 
Remote files are copied atomically into SourceCache and every later stage reads that managed
local copy. Repeated `if_missing` runs reuse that cache. When the optional
`curl` package is available, `if_changed` sends a conditional HTTP request and
avoids downloading unchanged content when the server honors modification dates.

```r
MDT <- MaterializeRemoteSources(MDT = MDT,
                                DownloadCachePath = paths$DownloadCachePath,
                                Offline = isTRUE(cfg$RemoteOffline),
                                DefaultDownloadPolicy = cfg$DownloadPolicy %||% "if_missing",
                                TimeoutSeconds = cfg$DownloadTimeout %||% 600,
                                LogPath = paths$LogPath,
                                RunId = RunId)
pending <- MDTCompleteStatus(MDT = MDT,
                             CheckpointPath = paths$CheckpointPath,
                             verbose = TRUE,
                             logStatus = TRUE)
```

### 3. Survey And Recommend Schemas

`PrepareSchemaRegistry()` reads every source through the same configured reader
used by final loading. Detailed per-file and per-column evidence is stored in
`SchemaObservations.parquet`; the compact `SchemaReview.xlsx` presents type
history, safe coercion recommendations, compatibility decisions, source issues,
low-cardinality previews, and semantic labels. Recommendations are data-derived;
an optional `SchemaRegistry.xlsx` policy is shown explicitly rather than applied
invisibly. Source files remain read-only and no cleaned staging copy is created.
A remote source's managed cache contains its original downloaded bytes, not
rewritten records. Schema evidence is also checkpointed one source at a time in
`SchemaObservations_sources`, so interrupted runs resume and unchanged sources
are not scanned again.

`SchemaSurveyMode = "adaptive"` fully profiles bounded clean files with a fast
`fread` path, samples larger clean files, and retains exhaustive logical-record
scanning for sources configured for continuation repair. Use `"full"` when every
delimited row must be inspected before review, or `"sample"` for rapid discovery.
Final loading still enforces the approved schema in every mode. Use a modest
`SchemaWorkers` value (typically 4-6) for network storage; the separate
`n_workers` setting remains available for other repository phases.

```r
DBLoad <- sort(unique(MDT$Database))
prepared <- PrepareSchemaRegistry(MDT = MDT,
                                  DBLoad = DBLoad,
                                  MasterDBPath = cfg$MasterDBPath,
                                  ObservationPath = paths$SchemaObservationPath,
                                  SchemaReviewPath = paths$SchemaReviewPath,
                                  n_workers = cfg$SchemaWorkers,
                                  SourceFingerprintMode = cfg$SourceFingerprintMode,
                                  SchemaSurveyMode = cfg$SchemaSurveyMode,
                                  FastReadMaxBytes = cfg$SchemaFastReadMaxBytes,
                                  SchemaChunkSize = cfg$SchemaChunkSize,
                                  AdaptiveSampleRows = cfg$SchemaAdaptiveSampleRows,
                                  FutureGlobalsMaxSizeMB = cfg$SchemaFutureGlobalsMaxSizeMB,
                                  ReuseObservationCache = cfg$SchemaReuseCache,
                                  StrictReaders = FALSE,
                                  ValuePreviewMaxDistinct = 15L,
                                  ValuePreviewTypes = c("character", "integer", "int64", "logical"),
                                  ValuePreviewIdentifiers = FALSE,
                                  SchemaRegistryPath = paths$SchemaRegistryPath,
                                  SchemaProfile = "generic",
                                  LogPath = paths$LogPath,
                                  RunId = RunId )

# Optional bounded issue preview; this query does not load all observations.
schema_issues <- GetSchemaObservations(ObservationPath = paths$SchemaObservationPath,
                                       IssuesOnly = TRUE, Limit = 100L)
if(nrow(schema_issues) > 0L){ print(schema_issues) }
```

### 4. Review And Finalize The Catalog

Open `SchemaReview.xlsx` at `StartHere`. `ColumnDecisions` contains ambiguous
types or reader warnings: `Accept` keeps `RecommendedType`, while `Override`
requires `ApprovedType`. `CompatibilityDecisions` controls same-named columns:
`Accept` uses the recommended common type, `Override` supplies another common
type, and `Ignore` keeps intentionally unrelated fields apart.

`DictionaryReview` handles code meanings such as `1 = Yes`: `Accept` keeps the
proposal, `Add` supplies a missing label, `Override` replaces it, and `Ignore`
omits the mapping. `FinalizeSchemaRegistry()` refuses unresolved blocking
decisions and writes `TableSchemas.xlsx` in the exact format consumed by the
Parquet writer. A second survey is unnecessary unless source evidence changes.

In summary: 
StartHere identifies every blocking item and its worksheet. 
ColumnDecisions contains only genuinely ambiguous evidence or reader 
warnings. Safe promotions are automatic; PolicyReport records policy 
differences without forcing repetitive decisions. For every visible 
ColumnDecisions row: 
  Decision = "Accept" keeps RecommendedType. 
  Decision = "Override" requires the desired ApprovedType. 
CompatibilityDecisions contains unresolved same-named table conflicts: 
  Accept   = use RecommendedCommonType for that approved merge group. 
  Override = use ApprovedCommonType instead. 
  Ignore   = the similarly named fields are intentionally kept apart. 
DictionaryReview contains code meanings such as 1 = Yes: 
  Accept   = keep a proposed source label for this partition scope. 
  Add      = add an optional label where no source meaning exists. 
  Override = replace a source label with ApprovedLabel. 
  Ignore   = omit a conflicting mapping from the finalized dictionary. 
PolicyPattern/PolicyType show every SchemaRegistry.xlsx match, including 
cases where the observed data makes the policy potentially lossy. 
After review, run FinalizeSchemaRegistry below; a second survey is not 
needed. Rerun the survey after fixing SourceIssues or changing sources. 
Existing decisions survive only while their observation signature is 
unchanged; changed evidence returns to the appropriate decision sheet. 
Finalization stops here until all required decisions are complete, then 
writes TableSchemas.xlsx in the exact format ParquetBackEndCreate uses.

```r
repository_catalog <- FinalizeSchemaRegistry(SchemaReviewPath = paths$SchemaReviewPath,
                                             TableSchemaPath = paths$TableSchemaPath, strict = TRUE)
```

### 5. Load Partitioned Parquet

`ParquetBackEndCreate()` enforces the finalized table-specific catalog, routes
records to the Hive partitions declared in the inventory, and writes
memory-bounded chunks where supported. Progress is checkpointed only after a
successful source write, making reruns resumable. Source fingerprints detect
files replaced in place; coercion damage is bounded by `MaxCoerceNAPct`.

At the beginning of a run, the loader snapshots the checkpoint, manifest,
schema catalog, and policy registry into `StateBackups`. The transactional
DuckDB manifest is authoritative, while `RepositoryMetadata.xlsx` is refreshed
as an accessible presentation copy. Because preflight already ran in step 2,
this call uses `RunPreflight = FALSE`.

```r
result <- ParquetBackEndCreate(MDT = MDT,
                               DBLoad = DBLoad,
                               MasterDBPath = cfg$MasterDBPath,
                               completed_checkpoint = load_checkpoint(paths$CheckpointPath),
                               CheckpointPath = paths$CheckpointPath,
                               ParquetBasePath = paths$ParquetBasePath,
                               LogPath = paths$LogPath,
                               n_workers = cfg$n_workers,
                               PartitionBy = cfg$PartitionBy,
                               RAMThreshold = cfg$RAMThreshold,
                               SAV_ROW_THRESHOLD = cfg$SAV_ROW_THRESHOLD,
                               SAV_CHUNK_SIZE = cfg$SAV_CHUNK_SIZE,
                               MaxFileStemTruncate = TRUE,
                               TerminalHivePartition = FALSE,
                               SchemaRegistryPath = paths$SchemaRegistryPath,
                               TableSchemaPath = paths$TableSchemaPath,
                               ManifestPath = paths$ManifestPath,
                               MetadataWorkbookPath = paths$ManifestWorkbookPath,
                               UseSchemaCatalog = TRUE,
                               StrictPreflight = TRUE,
                               StrictSchemaValidation = TRUE,
                               RunPreflight = FALSE,
                               DownloadCachePath = paths$DownloadCachePath,
                               MaterializeRemote = FALSE, # already resolved before schema survey
                               SourceFingerprintMode = cfg$SourceFingerprintMode,
                               MaxCoerceNAPct = cfg$MaxCoerceNAPct,
                               AutoCleanup = TRUE,
                               CleanupAfterPhase = "database",
                               StopOnFileError = TRUE,
                               ReturnRunResult = TRUE,
                               RunId = RunId)
print(result)

SummaryVerification(
  MDT = MDT,
  CheckpointPath = paths$CheckpointPath,
  LogPath = paths$LogPath,
  logStatus = FALSE,
  RunId = RunId,
  MasterDBPath = cfg$MasterDBPath,
  SourceFingerprintMode = cfg$SourceFingerprintMode
)
```

`RepositoryMetadata.duckdb` remains the authoritative transactional manifest.
For DuckDB manifests, the loader also refreshes `RepositoryMetadata.xlsx` at
the end of each run. The workbook contains navigable table/run summaries and
every manifest record across numbered detail sheets. It can also be regenerated
without loading data:

```r
ExportRepositoryMetadata(paths$ManifestPath, paths$ManifestWorkbookPath)
```

The detailed per-file and per-column evidence remains in Parquet so the Excel
workbook stays navigable. Retrieve only the slice needed for troubleshooting:

```r
GetSchemaObservations(
  paths$SchemaObservationPath,
  Database = "SALES", TableName = "Orders", Column = "ORDER_ID"
)
```

Low-cardinality values appear in `SchemaReview.xlsx` as compact column
summaries and a filterable `ValuePreview` sheet with partition coverage. Full delimited scans are
marked `complete`; bounded previews from other readers are marked `sampled`.
Finalization carries the summaries into `TableSchemas` and writes the detailed
rows to the `ValueDictionary` sheet. Identifier-like values are suppressed by
default, and columns exceeding the configured distinct-value limit retain only
an `exceeds_limit` status.

For labeled formats such as SPSS, Stata, and SAS, the same survey also records
variable labels and exact code-to-label metadata without modifying the source.
`DictionaryReview` auto-approves stable source meanings, requires a decision
when labels change across partitions, and offers blank labels as optional
documentation. Finalization writes approved mappings to the partition-aware
`ColumnDictionary` sheet in `TableSchemas.xlsx`; observed evidence remains
separate in `ValueDictionary`.

`SchemaRegistryPath` is an optional reusable policy-pattern file. A generic
project creates an empty template, so no meaning is inferred from names such as
`ID`, `KEY`, or `CODE`; domain rules are opt-in. `SchemaReviewPath` is the
user-facing proposal, and finalization writes the concrete approved schema to
`TableSchemaPath`, which is the authoritative catalog consumed by the Parquet
writer. When both paths are passed to `ParquetBackEndCreate()`, reviewed columns
come from `TableSchemaPath`; `SchemaRegistryPath` is retained only for policy
metadata and for genuinely new columns absent from the finalized catalog.

### 6. Register And Validate DuckDB

Open the persistent DuckDB database in read-write mode while creating views.
`register_parquet_view_compile()` registers only successfully checkpointed
tables and validates physical and Hive-partition column types against the
finalized catalog. With strict validation enabled, a mismatch stops the step
instead of allowing a subtly incompatible view. `validate_data_contracts()`
then applies optional declarative content rules such as non-null, uniqueness,
range, allowed-value, regex, and foreign-key checks.

```r
TempDirPath <- file.path(cfg$FormattedDBPath, "duckdb_temp")
dir.create(TempDirPath, recursive = TRUE, showWarnings = FALSE)
con <- open_duckdb(FormattedDBPath = cfg$FormattedDBPath,
                   DBName = cfg$DBName,
                   TempDirPath = TempDirPath,
                   GB = cfg$DuckDB_GB,
                   ReadOnly = FALSE)
done <- MDT[checkpoint_completed_mask(MDT, 
                                      result$checkpoint,
                                      MasterDBPath = cfg$MasterDBPath,
                                      SourceFingerprintMode = cfg$SourceFingerprintMode ), ]
register_parquet_view_compile(con = con,
                              ParquetBasePath = paths$ParquetBasePath,
                              tables_written = unique(repository_table_names(done)),
                              TableSchemaPath = paths$TableSchemaPath,
                              SchemaRegistryPath = paths$SchemaRegistryPath,
                              validate = TRUE,
                              strict_validation = TRUE,
                              LogPath = paths$LogPath,
                              RunId = RunId)
contract_results <- validate_data_contracts(con = con,
                                            DataContractPath = paths$DataContractPath,
                                            strict = TRUE,
                                            LogPath = paths$LogPath,
                                            RunId = RunId)
# Reopen read-only after view creation for safer analytical sessions.
if(exists("con") && DBI::dbIsValid(con)){ DBI::dbDisconnect(con, shutdown = TRUE) }
con <- open_duckdb(FormattedDBPath = cfg$FormattedDBPath,
                   DBName = cfg$DBName,
                   TempDirPath = TempDirPath,
                   GB = cfg$DuckDB_GB,
                   ReadOnly = TRUE)
# Optional dictionary-assisted discovery and decoding.
describe_column(paths$TableSchemaPath, "SALES", "Orders", "STATUS")
decoded <- decode_column(con = con,
                         table = "SALES_Orders",
                         column = "STATUS",
                         TableSchemaPath = paths$TableSchemaPath,
                         limit = 1000L)
```

### 7. Reconcile Repository State

`audit_repository()` is a non-destructive consistency check across the source
inventory, checkpoint, transactional manifest, Parquet files on disk, and
DuckDB row counts. It reports stale checkpoints, missing outputs, orphaned
Parquet, manifest divergence, and count mismatches; it does not delete or repair
anything automatically. Review its detail tables (e.g. repo_audit$orphan_parquet) 
before using a targeted reset (e.g. reset_table_for_reload()) or rerunning the loader.

```r
repo_audit <- audit_repository(MDT = MDT,
                               ParquetBasePath = paths$ParquetBasePath,
                               CheckpointPath = paths$CheckpointPath,
                               ManifestPath = paths$ManifestPath,
                               con = con,
                               LogPath = paths$LogPath,
                               RunId = RunId)
repo_audit$issues
DBI::dbDisconnect(con, shutdown = TRUE)
```

### Maintenance And Refresh

The canonical load is resumable, so ordinary reruns process only incomplete or
source-fingerprint-invalidated inventory rows. Two non-destructive helpers cover
common repository maintenance outside the seven-stage run:

new-release onboarding:  
Scans every MDBDir the workbook references for files that have no MDT row yet and proposes 
candidate rows (Database/TableName/year guessed from filenames, flagged. 
This requires manual review where a guess failed. Review the output, correct it, and
paste the rows into DBSetup.xlsx. Nothing is written automatically.

```r
# Propose inventory rows for newly delivered files. Review the workbook output;
# the helper never edits DBSetup.xlsx or source files.
new_files <- scan_for_new_source_files(MasterDBPath = MasterDBPath, MDT = MDT,
                                       OutputPath = file.path(FormattedDBPath, "NewSourceFiles.xlsx"))
```

Rebuild or re-load tables:
Force one table to rebuild under the current schema
Parquet already on disk keeps the column types it was written with; 
changes approved in SchemaReview.xlsx and finalized to 
TableSchemas.xlsx affect only future writes. To apply them to an 
existing table, clear and reload it. DryRun = TRUE (the default) 
only reports what would be removed; once it looks right, set 
DryRun = FALSE and then re-run the ParquetBackEndCreate step above to 
rebuild the table from source.

```r
reset_table_for_reload(MDT = MDT, Database = "NIS", TableName = "Core",
                       ParquetBasePath = ParquetBasePath,
                       CheckpointPath = CheckpointPath,
                       ManifestPath = RepositoryPaths$ManifestPath, DryRun = TRUE)
```

After inspecting a reset preview, use `DryRun = FALSE` and rerun stages 5-7.
State snapshots created by the loader provide an additional recovery record.

The healthcare reference uses the same sequence with domain policies, migration
helpers, advanced diagnostics, and machine-specific tuning. Those additions
are intentionally not hard-coded into the generic package workflow.
