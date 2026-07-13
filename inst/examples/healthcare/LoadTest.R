
> RepoquetSourcePath <- Sys.getenv(
  +   "REPOQUET_SOURCE",
  +   unset = "C:/Users/e282219/Downloads/github/repoquet/R/repoquet.R"
  + )
> if (!file.exists(RepoquetSourcePath)) {
  +   stop("repoquet development source not found: ", RepoquetSourcePath,
           +        ". Set REPOQUET_SOURCE to the cloned repository's R/repoquet.R file.")
  + }
> source(RepoquetSourcePath, local = .GlobalEnv)
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
[2026-07-13 15:19:39] [SCHEMA REGISTRY] Created default 'hcup' registry: X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaRegistry.xlsx
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
> MDT <- openxlsx::read.xlsx("C:/Users/e282219/Downloads/github/repoquet/inst/extdata/DBSetupV2.xlsx", sheet = "Sheet1")
> ###############################################################################
> #### Schema discovery: survey, review, and finalize ###########################
> ###############################################################################
> #### The survey reads every source through its configured reader and stores  ####
> #### detailed per-file/per-column evidence in SchemaObservations.parquet.    ####
> #### Recommendations are derived from those observations rather than HCUP    ####
> #### naming rules. Open StartHere first. ColumnDecisions and                  ####
> #### CompatibilityDecisions contain only unfinished work; PolicyReport is    ####
> #### informational. Advanced registries/history are hidden but available.     ####
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
[2026-07-13 15:26:03] [run_id=20260713T151940_26108_998893] [SCHEMA SURVEY] 483 source(s), 22509 observation row(s), 0 failed source(s); wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaObservations.parquet
[2026-07-13 15:27:08] [SCHEMA PROPOSAL] Wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx (5334 columns; 0 column decision(s), 109 compatibility decision(s), 0 blocking source error(s)).
[2026-07-13 15:27:08] [SCHEMA READY] Open StartHere in X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx: 0 column decision(s), 109 compatibility decision(s), 0 blocking source error(s).
> ####   Override = use ApprovedCommonType instead.                            ####
> ####   Ignore   = the similarly named fields are intentionally kept apart.   ####
> #### PolicyPattern/PolicyType show every SchemaRegistry.xlsx match, including ####
> #### cases where the observed data makes the policy potentially lossy.       ####
> #### After review, run FinalizeSchemaRegistry below; a second survey is not  ####
> #### needed. Rerun the survey after fixing SourceIssues or changing sources. ####
> #### Existing decisions survive only while their observation signature is   ####
> #### unchanged; changed evidence returns to the appropriate decision sheet. ####
> #### Finalization stops here until all required decisions are complete, then ####
> #### writes TableSchemas.xlsx in the exact format ParquetBackEndCreate uses. ####
> repository_catalog <- FinalizeSchemaRegistry(
  +   SchemaReviewPath = SchemaReviewPath,
  +   TableSchemaPath = TableSchemaPath,
  +   strict = TRUE
  + )
Error in .apply_compatibility_review(registry, compatibility, strict = strict) : 
  Overlapping compatibility decisions assign different types to the same table column.