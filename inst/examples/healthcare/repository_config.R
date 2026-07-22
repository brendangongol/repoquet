#### Repository configuration -- the only file with machine-specific paths. ####
repository_config <- list(
  MasterDBPath    = "X:/National Databases",   # root of the source data files
  FormattedDBPath = "X:/Brendan/NationalDatabases/formattedDatabases",   # parquet store, checkpoints, logs, catalog
  MDTPath         = "C:/Users/e282219/Downloads/github/repoquet/inst/extdata/DBSetupV2.xlsx",   # the Master Database Table workbook
  PartitionBy       = "FAIL",     # NRows | RAMEstimate | FAIL
  SAV_ROW_THRESHOLD = 4000000L,    # rows above which files stream in chunks
  SAV_CHUNK_SIZE    = 4000000L,    # requested rows per chunk for SAV files
  DelimitedChunkMaxMB = 256L,      # NRows/RAMEstimate only: cap estimated CSV/TSV chunk memory; FAIL starts at SAV_CHUNK_SIZE and shrinks on recoverable memory errors
  DelimitedPartitionMaxMB = NULL,  # optional cap for one source-defined partition (e.g. one
                                   # year of a combined multi-year file) in a single pass;
                                   # NULL uses a conservative bounded route budget. Set an
                                   # explicit value only after verifying that the complete
                                   # read, conversion, and Arrow write fit in memory.
  FailProbeMode = "subprocess",  # subprocess | in_process | disabled; isolates FAIL direct reads
  FailProbeTimeoutSeconds = 7200, # after timeout, cleanly fall back to adaptive chunking
  FailProbeSourcePath = Sys.getenv("REPOQUET_SOURCE", unset = ""), # sourced-development worker code
  RAMThreshold      = 30,          # GB, for PartitionBy = "RAMEstimate"
  MaxCoerceNAPct    = 25,          # fail a file when coercion destroys more than this % of a column
  SourceFingerprintMode = "metadata", # metadata | sha256 | none
  SchemaSurveyMode   = "adaptive", # adaptive | full | sample
  SchemaFastReadMaxBytes = 512 * 1024^2, # bounded fread fast path; tune to available RAM
  SchemaChunkSize    = 250000L,      # rows per exhaustive schema chunk
  SchemaAdaptiveSampleRows = 100000L, # rows sampled from large clean files
  SchemaFutureGlobalsMaxSizeMB = 768, # sourced-development worker allowance
  SchemaReuseCache   = TRUE,         # resume and reuse unchanged source evidence
  SchemaWorkers      = 6L,           # schema readers; keep modest on network storage
  RemoteOffline      = FALSE,        # TRUE uses only previously cached remote sources
  DownloadPolicy    = "if_missing", # if_missing | if_changed | always | manual
  DownloadTimeout   = 600,          # seconds
  n_workers         = min(15L, max(1L, parallel::detectCores() - 1L)),
  DBName            = "DuckDBRelationalDatabase.duckdb",
  DuckDB_GB         = "48GB"        # DuckDB memory limit (~75% of available RAM)
)
