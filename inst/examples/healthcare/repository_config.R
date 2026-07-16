#### Repository configuration -- the only file with machine-specific paths. ####
repository_config <- list(
  MasterDBPath    = "X:/National Databases",   # root of the source data files
  FormattedDBPath = "X:/Brendan/NationalDatabases/formattedDatabases",   # parquet store, checkpoints, logs, catalog
  MDTPath         = "C:/Users/e282219/Downloads/github/repoquet/inst/extdata/DBSetupV2.xlsx",   # the Master Database Table workbook
  PartitionBy       = "FAIL",     # NRows | RAMEstimate | FAIL
  SAV_ROW_THRESHOLD = 4000000L,    # rows above which files stream in chunks
  SAV_CHUNK_SIZE    = 4000000L,    # rows per chunk
  RAMThreshold      = 30,          # GB, for PartitionBy = "RAMEstimate"
  MaxCoerceNAPct    = 25,          # fail a file when coercion destroys more than this % of a column
  SourceFingerprintMode = "metadata", # metadata | sha256 | none
  RemoteOffline      = FALSE,        # TRUE uses only previously cached remote sources
  DownloadPolicy    = "if_missing", # if_missing | if_changed | always | manual
  DownloadTimeout   = 600,          # seconds
  n_workers         = min(15L, max(1L, parallel::detectCores() - 1L)),
  DBName            = "DuckDBRelationalDatabase.duckdb",
  DuckDB_GB         = "48GB"        # DuckDB memory limit (~75% of available RAM)
)
