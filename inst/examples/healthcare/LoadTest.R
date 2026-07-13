

> RepoquetSourcePath <- Sys.getenv(
  +   "REPOQUET_SOURCE",
  +   unset = "C:/Users/e282219/Downloads/github/repoquet/R/repoquet.R"
  + )
> if (!file.exists(RepoquetSourcePath)) {
  +   stop("repoquet development source not found: ", RepoquetSourcePath,
           +        ". Set REPOQUET_SOURCE to the cloned repository's R/repoquet.R file.")
  + }
> source(RepoquetSourcePath, local = .GlobalEnv)
> RepoquetSourcePath
[1] "C:/Users/e282219/Downloads/github/repoquet/R/repoquet.R"
> ################################################################################
> #### Configuration #############################################################
> ################################################################################
> MasterDBPath <- "X:/National Databases"
> FormattedDBPath <- "X:/Brendan/NationalDatabases/formattedDatabases"
> ParquetBasePath <- file.path(FormattedDBPath, "parquet")
> CheckpointPath <- file.path(FormattedDBPath, "load_checkpoint.rds")
> LogPath <- file.path(FormattedDBPath, "load_log.txt")
> SupportingInfoPath <- "C:/Users/e282219/Downloads/github/CECORC/inst/Misc/DatabaseLoadInfo.xlsx"
> RepositoryPaths <- RepositoryInitialize(FormattedDBPath = FormattedDBPath,
                                          +                                         ParquetBasePath = ParquetBasePath,
                                          +                                         CheckpointPath = CheckpointPath,
                                          +                                         LogPath = LogPath,
                                          +                                         profile = "hcup")
[2026-07-13 13:35:26] [SCHEMA REGISTRY] Created default 'hcup' registry: X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaRegistry.xlsx
> SchemaRegistryPath <- RepositoryPaths$SchemaRegistryPath
> TableSchemaPath <- RepositoryPaths$TableSchemaPath
> SchemaObservationPath <- RepositoryPaths$SchemaObservationPath
> SchemaReviewPath <- RepositoryPaths$SchemaReviewPath
> ManifestPath <- RepositoryPaths$ManifestPath
> DataContractPath <- RepositoryPaths$DataContractPath
> RunId <- new_repository_run_id()
> SAV_CHUNK_SIZE <- 4000000L
> n_workers <- min(15L, max(1L, parallel::detectCores() - 1L))
> dir.create(ParquetBasePath, recursive = TRUE, showWarnings = FALSE)
> ###############################################################################
> #### Cleanup handler ##########################################################
> #### Install once per R session: re-sourcing this script must not wrap the ####
> #### error handler around itself or stack duplicate exit finalizers. ##########
> ###############################################################################
> .cleanup_con <- function() {
  +   if (exists("con", envir = .GlobalEnv) && DBI::dbIsValid(get("con", envir = .GlobalEnv))) {
    +     log_msg("Script exiting. Closing DuckDB connection.")
    +     DBI::dbDisconnect(get("con", envir = .GlobalEnv), shutdown = TRUE)
    +   }
  + }
> if (!isTRUE(getOption("CECORC.cleanup_installed"))) {
  +   .prior_error_handler <- getOption("error")
  +   .cleanup_on_error <- function() {
    +     try(.cleanup_con(), silent = TRUE)
    +     if (is.function(.prior_error_handler)) {
      +       .prior_error_handler()
      +     } else if (!is.null(.prior_error_handler)) {
        +       eval(.prior_error_handler)
        +     }
    +   }
  +   options(error = .cleanup_on_error, CECORC.cleanup_installed = TRUE)
  +   reg.finalizer(.GlobalEnv, function(e) .cleanup_con(), onexit = TRUE)
  + }
NULL
> #### ValidateMDTPreflight before anything is written: all rows of one table  ####
> #### share the same PartitionKey; YEAR-keyed values must be whole years;     ####
> #### no two values may sanitize to the same directory. Legacy workbooks      ####
> #### with a Year column still work (blank spec falls back to YEAR + Year).   ####
> #### Optional column AcceptPartial: set TRUE on a row after verifying that   ####
> #### its file is permanently truncated (rows_written < declared ncases) and  ####
> #### you accept the partial data -- the loader then checkpoints it with a    ####
> #### [WARN] and manifest Status="partial_accepted" instead of failing and    ####
> #### re-reading the whole file on every run. Verified-empty files (declared  ####
> #### and confirmed 0 rows) checkpoint automatically with Status="empty".     ####
> MDT <- openxlsx::read.xlsx("C:/Users/e282219/Downloads/github/repoquet/inst/extdata/DBSetupV2.xlsx", sheet = "Sheet1")
> ###############################################################################
> #### Schema discovery: survey, review, and finalize ###########################
> ###############################################################################
> #### The survey reads every source through its configured reader and stores  ####
> #### detailed per-file/per-column evidence in SchemaObservations.parquet.    ####
> #### Recommendations are derived from those observations rather than HCUP    ####
> #### naming rules. SchemaReview.xlsx stays compact: Review contains only     ####
> #### decisions needing attention; Registry contains every proposed column;   ####
> #### History shows type drift; SourceIssues shows reader errors/warnings.     ####
> #### DBLoad derives from DBSetupV2.xlsx. Override it only for a staged load.  ####
> DBLoad <- sort(unique(MDT$Database))
> PrepareSchemaRegistry(
  +   MDT = MDT,
  +   DBLoad = DBLoad,
  +   MasterDBPath = MasterDBPath,
  +   ObservationPath = SchemaObservationPath,
  +   SchemaReviewPath = SchemaReviewPath,
  +   n_workers = n_workers,
  +   SourceFingerprintMode = "metadata",
  +   StrictReaders = FALSE,
  +   #### This policy workbook is optional and visible. The survey remains     ####
  +   #### data-derived; matched HCUP policies and conflicts appear explicitly  ####
  +   #### in SchemaReview.xlsx instead of being applied invisibly.             ####
  +   SchemaRegistryPath = SchemaRegistryPath,
  +   SchemaProfile = "hcup",
  +   LogPath = LogPath,
  +   RunId = RunId
  + )
[2026-07-13 13:42:13] [run_id=20260713T133539_19440_640680] [PARALLEL FALLBACK] repository schema survey failed for 2 item(s); retrying those item(s) serially in the main R process.
[2026-07-13 13:42:13] [run_id=20260713T133539_19440_640680] [PARALLEL FALLBACK] First worker error(s): Delimited structure error in PUF_AISDES_TQP_2016.csv: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>. The source was not modified. For a verified continuation line, set MalformedRowPolicy='append_previous' and ContinuationColumn in ReaderOptions. | Delimited structure error in TQIP_RDS_AISDES.csv: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>. The source was not modified. For a verified continuation line, set MalformedRowPolicy='append_previous' and ContinuationColumn in ReaderOptions.
[2026-07-13 13:42:15] [run_id=20260713T133539_19440_640680] [SCHEMA SURVEY] 485 source(s), 22511 observation row(s), 2 failed source(s); wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaObservations.parquet
[2026-07-13 13:43:18] [SCHEMA PROPOSAL] Wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx (5334 columns; 478 require review).
[2026-07-13 13:43:18] [SCHEMA READY] 4856 column(s) resolved automatically; 478 require column review; 109 cross-table compatibility conflict(s) require review in X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx.
> #### Optional console preview. The helper queries Parquet with DuckDB, so    ####
> #### it does not pull the full observation store into R.                     ####
> schema_issues <- GetSchemaObservations(
  +   ObservationPath = SchemaObservationPath,
  +   IssuesOnly = TRUE,
  +   Limit = 100L
  + )
> if (nrow(schema_issues) > 0L) print(schema_issues)
Database TableName DuckDBTable
<char>    <char>      <char>
  1:     NTDB    AISDES NTDB_AISDES
2:      TQP    AISDES  TQP_AISDES
SourcePath
<char>
  1: X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
2:                        X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
FileType PartitionKey PartitionValue SourceSize SourceModifiedUTC SourceFingerprint
<char>       <char>         <char>      <num>            <char>            <char>
  1:      csv         YEAR           2016     169791              <NA>              <NA>
  2:      csv         YEAR           2016     170973              <NA>              <NA>
  DeclaredEncoding DetectedEncoding EncodingConfidence EncodingUsed
<char>           <char>              <num>       <char>
  1:             auto            UTF-8                  1        UTF-8
2:             auto            UTF-8                  1        UTF-8
EncodingDetectionMethod EncodingValidationStatus ReaderRepairCount ReaderRepairLines
<char>                   <char>             <num>            <char>
  1:             strict_utf8                    error                NA              <NA>
  2:             strict_utf8                    error                NA              <NA>
  ReaderRepairPolicy ObservationKind Column OriginalColumn IsPartitionColumn
<char>          <char> <char>         <char>            <lgcl>
  1:               <NA>    source_error   <NA>           <NA>             FALSE
2:               <NA>    source_error   <NA>           <NA>             FALSE
InferenceConfidence ReaderWarning ReaderWarningClass ReaderWarningSeverity
<char>        <char>             <char>                <char>
  1:         unavailable          <NA>               <NA>                  <NA>
  2:         unavailable          <NA>               <NA>                  <NA>
  SurveyStatus
<char>
  1:        error
2:        error
SurveyMessage
<char>
  1:     Delimited structure error in TQIP_RDS_AISDES.csv: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>. The source was not modified. For a verified continuation line, set MalformedRowPolicy='append_previous' and ContinuationColumn in ReaderOptions.
2: Delimited structure error in PUF_AISDES_TQP_2016.csv: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>. The source was not modified. For a verified continuation line, set MalformedRowPolicy='append_previous' and ContinuationColumn in ReaderOptions.
ObservedType RowsSampled NonMissingCount MissingPercent IntegerLike FractionalCount
<char>       <num>           <num>          <num>      <lgcl>           <num>
  1:         <NA>          NA              NA             NA          NA              NA
2:         <NA>          NA              NA             NA          NA              NA
LeadingZeroCount NumericParseFailureCount Minimum Maximum MaximumTextLength
<num>                    <num>  <char>  <char>             <num>
  1:               NA                       NA    <NA>    <NA>                NA
2:               NA                       NA    <NA>    <NA>                NA
PrecisionRisk
<lgcl>
  1:            NA
2:            NA
> ####   Override = use ApprovedCommonType instead.                            ####
> ####   Ignore   = the similarly named fields are intentionally kept apart.   ####
> #### PolicyPattern/PolicyType show every SchemaRegistry.xlsx match, including ####
> #### cases where the observed data makes the policy potentially lossy.       ####
> #### After review, run FinalizeSchemaRegistry below; a second survey is not  ####
> #### needed. Rerun the survey after fixing SourceIssues or changing sources. ####
> #### Existing decisions survive only while their observation signature is   ####
> #### unchanged, so changed evidence always returns to the Review sheet.      ####
> #### Finalization stops here until all required decisions are complete, then ####
> #### writes TableSchemas.xlsx in the exact format ParquetBackEndCreate uses. ####
> repository_catalog <- FinalizeSchemaRegistry(
  +   SchemaReviewPath = SchemaReviewPath,
  +   TableSchemaPath = TableSchemaPath,
  +   strict = TRUE
  + )
Error in FinalizeRepositorySchema(SchemaReviewPath, TableSchemaPath, strict = strict) : 
  478 schema decision(s) remain unresolved in the Review sheet.