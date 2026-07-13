# repoquet

`repoquet` is a schema-aware, self-auditing engine that transforms heterogeneous
source files into refreshable, query-ready Parquet repositories for
larger-than-memory analytics.

Its distinguishing feature is that ingestion, schema normalization, refresh
detection, validation, and repository reconciliation are one coordinated
workflow rather than separate scripts. The loader supports SPSS, Stata, SAS,
transport, delimited, Parquet, and RDS sources and exposes validated tables
through DuckDB.

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

The installed package includes the real-world workbook used while developing
the multi-table healthcare workflow:

```r
system.file("extdata", "DBSetupV2.xlsx", package = "repoquet")
```

It is an example inventory rather than a required package configuration.

## Minimal Workflow

```r
cfg <- load_repository_config("~/my_repository/repository_config.R")
paths <- RepositoryInitialize(cfg$FormattedDBPath, profile = "generic")
MDT <- openxlsx::read.xlsx(cfg$MDTPath, sheet = "Sheet1")

ValidateMDTPreflight(
  MDT, strict = TRUE, ParquetBasePath = paths$ParquetBasePath
)

prepared <- PrepareSchemaRegistry(
  MDT, MasterDBPath = cfg$MasterDBPath,
  ObservationPath = paths$SchemaObservationPath,
  SchemaReviewPath = paths$SchemaReviewPath,
  n_workers = cfg$n_workers
)

# Open StartHere. Complete only the visible rows in ColumnDecisions and
# CompatibilityDecisions. PolicyReport is informational.
FinalizeSchemaRegistry(
  SchemaReviewPath = paths$SchemaReviewPath,
  TableSchemaPath = paths$TableSchemaPath,
  strict = TRUE
)

result <- ParquetBackEndCreate(
  MDT = MDT, DBLoad = sort(unique(MDT$Database)),
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
  ReturnRunResult = TRUE
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

`SchemaRegistryPath` remains the optional reusable policy-pattern file in this
first release of the review workflow. `SchemaReviewPath` is the user-facing
proposal; finalization writes the concrete approved schema to `TableSchemaPath`,
which is the catalog consumed by the Parquet writer.

Register and validate DuckDB views with the catalog's partition types:

```r
con <- DBI::dbConnect(duckdb::duckdb())
done <- MDT[checkpoint_completed_mask(MDT, result$checkpoint), ]
register_parquet_view_compile(
  con, paths$ParquetBasePath, unique(repository_table_names(done)),
  TableSchemaPath = paths$TableSchemaPath,
  SchemaRegistryPath = paths$SchemaRegistryPath,
  strict_validation = TRUE
)
validate_data_contracts(con, paths$DataContractPath, strict = TRUE)
audit_repository(MDT, paths$ParquetBasePath, paths$CheckpointPath,
                 paths$ManifestPath, con = con)
```

## Reproducibility And Tests

Run `Rscript tools/bootstrap-renv.R` once to restore the project-specific
`renv.lock`. Run tests with:

```text
Rscript tests/run_tests.R
R CMD check .
```
