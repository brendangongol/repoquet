# repoquet

`repoquet` is a schema-aware, self-auditing engine that transforms heterogeneous
source files into refreshable, query-ready Parquet repositories for
larger-than-memory analytics.

Its distinguishing feature is that ingestion, schema normalization, refresh
detection, validation, and repository reconciliation are one coordinated
workflow rather than separate scripts. The loader supports SPSS, Stata, SAS,
transport, delimited, Parquet, and RDS sources and exposes validated tables
through DuckDB.

`repoquet` provides a schema-aware, memory-bounded workflow for building large
analytic databases from heterogeneous source files (including SPSS/SAV and CSV)
into hive-partitioned Parquet with DuckDB-ready access. It is designed for
out-of-memory operation on local machines, with chunked ingestion,
deterministic column normalization, cross-release schema/class alignment,
resumable checkpointed loading, and fast consolidation for downstream querying.
In practice, this reduces RAM pressure and storage footprint, improves query
performance, and enables reliable multi-year table unification without
requiring the full dataset to be loaded into memory at once.


## Why R for database generation?

repoquet uses R because its vectorized data model and explicit coercion behavior
make schema enforcement easier to apply consistently during ingestion. In this
pipeline, column classes are validated and aligned before write, so type drift
is surfaced early instead of silently propagating into Parquet files. While
Python ecosystems can also enforce strict schemas, many common ingestion
patterns are permissive by default, which can allow mixed or inconsistent types
to slip through unless additional validation is added. By producing a strongly
typed Parquet backend at build time, repoquet creates datasets that are
reliable for both R and Python downstream users.


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
- `formatted/Manifest/RepositoryMetadata.duckdb`: transactional manifest
- `run_repository.R`: validation, catalog, loading, DuckDB, and audit workflow

The command-line equivalent is:

```text
Rscript inst/scripts/repoquet.R init ~/my_repository generic
Rscript inst/scripts/repoquet.R validate ~/my_repository
Rscript inst/scripts/repoquet.R catalog ~/my_repository
Rscript inst/scripts/repoquet.R load ~/my_repository
Rscript inst/scripts/repoquet.R audit ~/my_repository
```

## DBSetup Workbook

Required columns are `Database`, `MDBDir`, `Path`, `TableName`, and `FileType`.
Use `PartitionKey` and `PartitionValue` to define Hive partitions. A physical
`Year` column is not required: `PartitionKey = "year"` and
`PartitionValue = "2024"` creates `year=2024` and DuckDB exposes `YEAR` from
the directory.

Optional general-purpose columns include:

- `PartitionType`: canonical type for each partition key, separated by `;`
- `PhysicalTableName`: explicit Parquet directory and DuckDB view name
- `Encoding`, `Delimiter`, `Quote`, `NAStrings`, and `DecimalMark`
- `DateFormat`, `DateTimeFormat`, and `Timezone`
- `MalformedRowPolicy`, `ContinuationColumn`, and `ContinuationJoin` for an
  explicitly verified unquoted continuation line
- `ReaderOptions`: a JSON object passed to the selected reader
- `AcceptPartial`: explicit acceptance of a verified truncated SAV source

Nested partitions use matching semicolon-separated values, for example
`PartitionKey = "SITE;YEAR"`, `PartitionValue = "MGH;2024"`, and
`PartitionType = "character;integer"`.

`ValidateMDTPreflight()` checks paths, partition definitions, physical table
identity, output filename collisions, reader options, and custom file types
before any Parquet is written.

Delimited sources are strict by default. If a vendor file contains a verified
one-field line that continues the preceding record, configure that source row
without editing the source itself:

```json
{"MalformedRowPolicy":"append_previous","ContinuationColumn":"DESCRIPTION","ContinuationJoin":" "}
```

The same memory-bounded logical-record reader is then used for schema discovery
and repository loading. Corrected records flow directly to schema inference or
Parquet chunks; repoquet does not create a cleaned CSV or overwrite the source.


It is an example inventory rather than a required package configuration.

## Canonical Workflow (Minimal + Production)

repoquet follows one canonical 7-step sequence in both the minimal example
below and the expanded production reference script at
`inst/examples/healthcare/CECORC_loader_reference.R`.

## Minimal Workflow

For a zero-setup dry run, first generate the built-in dummy repository:

```r
## 1) Initialize paths and a run identifier for audit-traceable logging
example <- generate_example_repository("~/my_repository_example")
cfg <- load_repository_config(example$ConfigPath)
paths <- RepositoryInitialize(cfg$FormattedDBPath, profile = "generic")
RunId <- new_repository_run_id()
MDT <- openxlsx::read.xlsx(cfg$MDTPath, sheet = "Sheet1")
DBLoad <- sort(unique(MDT$Database))

## 2) Preflight gate: validate path/partition/read options before any write
ValidateMDTPreflight(
  MDT = MDT,
  strict = TRUE,
  ParquetBasePath = paths$ParquetBasePath,
  LogPath = paths$LogPath,
  RunId = RunId
)

## 3) Schema survey: collect per-file/per-column evidence and proposals
prepared <- PrepareSchemaRegistry(
  MDT = MDT,
  DBLoad = DBLoad,
  MasterDBPath = cfg$MasterDBPath,
  ObservationPath = paths$SchemaObservationPath,
  SchemaReviewPath = paths$SchemaReviewPath,
  SchemaRegistryPath = paths$SchemaRegistryPath,
  n_workers = cfg$n_workers,
  SourceFingerprintMode = cfg$SourceFingerprintMode,
  LogPath = paths$LogPath,
  RunId = RunId
)

## 4) Review gate: complete StartHere/ColumnDecisions/CompatibilityDecisions
##    in SchemaReview.xlsx, then finalize approved table schemas
FinalizeSchemaRegistry(
  SchemaReviewPath = paths$SchemaReviewPath,
  TableSchemaPath = paths$TableSchemaPath,
  strict = TRUE
)

## 5) Load sources into partitioned Parquet with checkpointed progress
result <- ParquetBackEndCreate(
  MDT = MDT,
  DBLoad = DBLoad,
  MasterDBPath = cfg$MasterDBPath,
  completed_checkpoint = load_checkpoint(paths$CheckpointPath),
  CheckpointPath = paths$CheckpointPath,
  ParquetBasePath = paths$ParquetBasePath,
  LogPath = paths$LogPath,
  PartitionBy = cfg$PartitionBy,
  RAMThreshold = cfg$RAMThreshold,
  SAV_ROW_THRESHOLD = cfg$SAV_ROW_THRESHOLD,
  SAV_CHUNK_SIZE = cfg$SAV_CHUNK_SIZE,
  SchemaRegistryPath = paths$SchemaRegistryPath,
  TableSchemaPath = paths$TableSchemaPath,
  ManifestPath = paths$ManifestPath,
  SourceFingerprintMode = cfg$SourceFingerprintMode,
  StopOnFileError = TRUE,
  ReturnRunResult = TRUE,
  RunId = RunId
)
print(result)
```

The detailed per-file and per-column evidence remains in Parquet so the Excel
workbook stays navigable. Retrieve only the slice needed for troubleshooting:

```r
GetSchemaObservations(
  paths$SchemaObservationPath,
  Database = "SALES", TableName = "Orders", Column = "ORDER_ID"
)
```

`SchemaRegistryPath` is an optional reusable policy-pattern file. A generic
project creates an empty template, so no meaning is inferred from names such as
`ID`, `KEY`, or `CODE`; domain rules are opt-in. `SchemaReviewPath` is the
user-facing proposal, and finalization writes the concrete approved schema to
`TableSchemaPath`, which is the authoritative catalog consumed by the Parquet
writer. When both paths are passed to `ParquetBackEndCreate()`, reviewed columns
come from `TableSchemaPath`; `SchemaRegistryPath` is retained only for policy
metadata and for genuinely new columns absent from the finalized catalog.

Register and validate DuckDB views with the catalog's partition types:

```r
## 6) Register compiled views over Parquet and validate contract rules
con <- open_duckdb(
  FormattedDBPath = cfg$FormattedDBPath,
  DBName = cfg$DBName,
  TempDirPath = file.path(cfg$FormattedDBPath, "duckdb_temp"),
  GB = cfg$DuckDB_GB,
  ReadOnly = FALSE
)
done <- MDT[checkpoint_completed_mask(MDT, result$checkpoint), ]
register_parquet_view_compile(
  con = con,
  ParquetBasePath = paths$ParquetBasePath,
  tables_written = unique(repository_table_names(done)),
  TableSchemaPath = paths$TableSchemaPath,
  SchemaRegistryPath = paths$SchemaRegistryPath,
  strict_validation = TRUE,
  LogPath = paths$LogPath,
  RunId = RunId
)
contract_results <- validate_data_contracts(
  con = con,
  DataContractPath = paths$DataContractPath,
  strict = TRUE,
  LogPath = paths$LogPath,
  RunId = RunId
)
## 7) Reconcile workbook, checkpoint, manifest, and registered tables
repo_audit <- audit_repository(
  MDT = MDT,
  ParquetBasePath = paths$ParquetBasePath,
  CheckpointPath = paths$CheckpointPath,
  ManifestPath = paths$ManifestPath,
  con = con,
  LogPath = paths$LogPath,
  RunId = RunId
)
repo_audit$issues
DBI::dbDisconnect(con, shutdown = TRUE)
```

The healthcare loader reference uses the same step order with additional
operational options (migration helpers, advanced diagnostics, stricter tuning).
Use it when moving from a dry run to production operations.

## Reproducibility And Tests

Run `Rscript tools/bootstrap-renv.R` once to restore the project-specific
`renv.lock`. Run tests with:

```text
Rscript tests/run_tests.R
R CMD check .
```
