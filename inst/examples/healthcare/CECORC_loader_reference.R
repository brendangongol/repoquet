
################################################################################
#### Optimized National Database Loader ########################################
################################################################################
library(data.table); library(openxlsx); library(DBI); library(duckdb);
library(haven); library(arrow); library(glue); library(future); library(future.apply)
library(ComplexHeatmap); library(RColorBrewer); library(circlize)
#### Development mode: always execute the current source-tree functions.   ####
#### Set REPOQUET_SOURCE on another machine instead of editing package code. ####
RepoquetSourcePath <- Sys.getenv("REPOQUET_SOURCE",
  unset = "C:/Users/e282219/Downloads/github/repoquet/R/repoquet.R" )
if (!file.exists(RepoquetSourcePath)) {
  stop("repoquet development source not found: ", RepoquetSourcePath,
       ". Set REPOQUET_SOURCE to the cloned repository's R/repoquet.R file.")
}
source(RepoquetSourcePath, local = .GlobalEnv)

################################################################################
#### 1. Initialize the run #####################################################
################################################################################
cfg <- load_repository_config(path = "C:/Users/e282219/Downloads/github/repoquet/inst/examples/healthcare/repository_config.R")
# This example loads heterogeneous public-health sources, so start with no
# healthcare-specific policy overrides. Use profile = "hcup" for an HCUP-only run.
RepositoryPaths <- RepositoryInitialize(FormattedDBPath = cfg$FormattedDBPath, profile = "generic")
SupportingInfoPath <- "C:/Users/e282219/Downloads/github/CECORC/inst/Misc/DatabaseLoadInfo.xlsx"
DuckDBTempPath <- file.path(cfg$FormattedDBPath, "duckdb_temp")
RunId <- new_repository_run_id()
dir.create(DuckDBTempPath, recursive = TRUE, showWarnings = FALSE)

###############################################################################
#### Cleanup handler ##########################################################
#### Install once per R session: re-sourcing this script must not wrap the ####
#### error handler around itself or stack duplicate exit finalizers. ##########
###############################################################################
.cleanup_con <- function(){
  if(exists("con", envir = .GlobalEnv) && DBI::dbIsValid(get("con", envir = .GlobalEnv))) {
    log_msg("Script exiting. Closing DuckDB connection.")
    DBI::dbDisconnect(get("con", envir = .GlobalEnv), shutdown = TRUE)
  } }
if(!isTRUE(getOption("CECORC.cleanup_installed"))) {
  .prior_error_handler <- getOption("error")
  .cleanup_on_error <- function() {
    try(.cleanup_con(), silent = TRUE)
    if (is.function(.prior_error_handler)) {
      .prior_error_handler()
    } else if (!is.null(.prior_error_handler)) {
      eval(.prior_error_handler)
    }}
  options(error = .cleanup_on_error, CECORC.cleanup_installed = TRUE)
  reg.finalizer(.GlobalEnv, function(e) .cleanup_con(), onexit = TRUE)
}

################################################################################
#### 2. Validate the source inventory ##########################################
################################################################################
MDT <- openxlsx::read.xlsx(cfg$MDTPath, sheet = "Sheet1")
ValidateMDTPreflight(MDT = MDT, strict = TRUE, logStatus = TRUE,
                     ParquetBasePath = RepositoryPaths$ParquetBasePath,
                     MaxFileStemTruncate = TRUE,
                     TerminalHivePartition = FALSE,
                     MasterDBPath = cfg$MasterDBPath,
                     LogPath = RepositoryPaths$LogPath, RunId = RunId)

################################################################################
#### Update a row's TableName if necessary: ####################################
################################################################################
# rename_checkpoint_table(CheckpointPath, MDT, "NRD", "CORE", "Core", RepositoryPaths$ManifestPath, DryRun = TRUE)
# # Optionally, once loaded checkpoints are migrated to generalized keys:
# migrate_checkpoint_keys(CheckpointPath, MDT, DryRun = TRUE)

################################################################################
#### Optional: new-release onboarding. Scans every MDBDir the workbook #########
#### references for files that have no MDT row yet #############################
################################################################################
# new_files <- scan_for_new_source_files(MasterDBPath = MasterDBPath, MDT = MDT,
#                                        OutputPath = file.path(FormattedDBPath, "NewSourceFiles.xlsx"))

################################################################################
#### Optional remote acquisition ###############################################
################################################################################
MDT <- MaterializeRemoteSources(
  MDT = MDT, DownloadCachePath = RepositoryPaths$DownloadCachePath,
  Offline = FALSE, DefaultDownloadPolicy = "if_missing",
  LogPath = RepositoryPaths$LogPath, RunId = RunId)
pending <- MDTCompleteStatus(MDT = MDT, CheckpointPath = RepositoryPaths$CheckpointPath,
                             verbose = TRUE, logStatus = TRUE)

###############################################################################
#### 3. Survey sources and recommend schemas ##################################
###############################################################################
DBLoad <- sort(unique(MDT$Database))
# c("NEDS", "NIS", "NISBishoy", "NRD", "NSQIP", "NTDB", "TQP")
# DBLoad <- c("NHANES", "CLINVAR", "MIMICIII_DEMO", "UCI_BREAST_CANCER", "UCI_DIABETES")
prepared <- PrepareSchemaRegistry(MDT = MDT,
                                  DBLoad = DBLoad,
                                  MasterDBPath = cfg$MasterDBPath,
                                  ObservationPath = RepositoryPaths$SchemaObservationPath,
                                  SchemaReviewPath = RepositoryPaths$SchemaReviewPath,
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
                                  SchemaRegistryPath = RepositoryPaths$SchemaRegistryPath,
                                  SchemaProfile = "generic",
                                  LogPath = RepositoryPaths$LogPath,
                                  RunId = RunId)
# Optional bounded issue preview; this query does not load all observations.
schema_issues <- GetSchemaObservations(ObservationPath = RepositoryPaths$SchemaObservationPath,
                                       IssuesOnly = TRUE, Limit = 100L)
if(nrow(schema_issues) > 0L){ print(schema_issues) }

###############################
#### To load Schema values ####
###############################
obs <- GetSchemaObservations(ObservationPath = RepositoryPaths$SchemaObservationPath, Column = "AGE", IssuesOnly = FALSE)
obs[, .(Database, TableName, ObservedType, ValueCount, ValueProfileStatus)]
obs[, .(Database, TableName, ObservedType, NumericParseFailureCount, Minimum, Maximum, MaximumTextLength)][TableName == "DBTable",]

###############################################################################
#### 4. Review decisions and finalize the catalog #############################
###############################################################################
repository_catalog <- FinalizeSchemaRegistry(SchemaReviewPath = RepositoryPaths$SchemaReviewPath,
                                             TableSchemaPath = RepositoryPaths$TableSchemaPath, strict = TRUE)

################################################################################
#### 5. Load reviewed schemas to partitioned Parquet ###########################
################################################################################
run_result <- ParquetBackEndCreate(MDT = MDT,
                                   DBLoad = DBLoad,
                                   MasterDBPath = cfg$MasterDBPath,
                                   completed_checkpoint = load_checkpoint(RepositoryPaths$CheckpointPath),
                                   CheckpointPath = RepositoryPaths$CheckpointPath,
                                   ParquetBasePath = RepositoryPaths$ParquetBasePath,
                                   LogPath = RepositoryPaths$LogPath,
                                   n_workers = cfg$n_workers,
                                   PrintStatus = TRUE,
                                   PartitionBy = cfg$PartitionBy,
                                   RAMThreshold = cfg$RAMThreshold,
                                   SAV_ROW_THRESHOLD = cfg$SAV_ROW_THRESHOLD,
                                   SAV_CHUNK_SIZE = cfg$SAV_CHUNK_SIZE,
                                   MaxFileStemTruncate = TRUE,
                                   chunk_size_decrement = NULL,
                                   min_chunk_size = NULL,
                                   TerminalHivePartition = FALSE,
                                   SchemaRegistryPath = RepositoryPaths$SchemaRegistryPath,
                                   TableSchemaPath = RepositoryPaths$TableSchemaPath,
                                   ManifestPath = RepositoryPaths$ManifestPath,
                                   MetadataWorkbookPath = RepositoryPaths$ManifestWorkbookPath,
                                   UseSchemaCatalog = TRUE,
                                   StrictPreflight = TRUE,
                                   StrictSchemaValidation = TRUE,
                                   RunPreflight = FALSE,
                                   DownloadCachePath = RepositoryPaths$DownloadCachePath,
                                   MaterializeRemote = FALSE, # already resolved before schema survey
                                   SourceFingerprintMode = cfg$SourceFingerprintMode,
                                   MaxCoerceNAPct = cfg$MaxCoerceNAPct,
                                   AutoCleanup = TRUE,
                                   CleanupAfterPhase = "database",
                                   StopOnFileError = TRUE,
                                   ReturnRunResult = TRUE,
                                   RunId = RunId)
print(run_result)
log_msg(sprintf("Checkpoint after load: %d files recorded", length(completed_checkpoint)))

################################################################################
#### Summary & Verification ####################################################
################################################################################
SummaryVerification(MDT = MDT, CheckpointPath = CheckpointPath, LogPath = LogPath,
                    logStatus = FALSE, RunId = run_result$run_id,
                    MasterDBPath = MasterDBPath,
                    SourceFingerprintMode = SourceFingerprintMode)

################################################################################
#### Regenerate RepositoryMetadata.xlsx without reloading data #################
################################################################################
# ExportRepositoryMetadata(RepositoryPaths$ManifestPath,
#                          RepositoryPaths$ManifestWorkbookPath)

################################################################################
#### Optional: force one table to rebuild under the current schema #############
################################################################################
# reset_table_for_reload(MDT = MDT, Database = "NIS", TableName = "Core",
#                        ParquetBasePath = ParquetBasePath,
#                        CheckpointPath = CheckpointPath,
#                        ManifestPath = RepositoryPaths$ManifestPath, DryRun = TRUE)

################################################################################
#### 6. Register and strictly validate DuckDB views ############################
################################################################################
con <- open_duckdb(FormattedDBPath = cfg$FormattedDBPath, DBName = cfg$DuckDBName, 
                   TempDirPath = cfg$DuckDBTempPath, GB = cfg$DuckDB_GB, ReadOnly = FALSE)
completed_checkpoint <- load_checkpoint(path = CheckpointPath)
completed_mdt <- MDT[checkpoint_completed_mask(MDT, completed_checkpoint,
                                               MasterDBPath = MasterDBPath, 
                                               SourceFingerprintMode = SourceFingerprintMode),]
register_parquet_view_compile(con = con, ParquetBasePath = ParquetBasePath, verbose = TRUE, logStatus = TRUE,
                               SchemaRegistryPath = RepositoryPaths$SchemaRegistryPath,
                               TableSchemaPath = RepositoryPaths$TableSchemaPath,
                               validate = TRUE, strict_validation = TRUE,
                               tables_written = unique(repository_table_names(completed_mdt)),
                               LogPath = LogPath, RunId = RunId )
contract_results <- validate_data_contracts(con, RepositoryPaths$DataContractPath, strict = TRUE,
                                            LogPath = LogPath, RunId = RunId)

#####################################################################
#### Adjust connection to read only, which is faster for queries ####
#####################################################################
if(exists("con") && DBI::dbIsValid(con)){ DBI::dbDisconnect(con, shutdown = TRUE) }
con <- open_duckdb(FormattedDBPath = cfg$FormattedDBPath, DBName = cfg$DuckDBName,
                   TempDirPath = cfg$DuckDBTempPath, GB = cfg$DuckDB_GB, ReadOnly = TRUE)

################################################################################
#### Optional dictionary-assisted discovery and decoding. ######################
################################################################################
describe_column(paths$TableSchemaPath, "SALES", "Orders", "STATUS")
decoded <- decode_column(con = con,
                         table = "SALES_Orders",
                         column = "STATUS",
                         TableSchemaPath = paths$TableSchemaPath,
                         limit = 1000L)

################################################################################
#### 7. Reconcile repository state #############################################
################################################################################
repo_audit <- audit_repository(MDT = MDT,
                               ParquetBasePath = ParquetBasePath,
                               CheckpointPath = CheckpointPath,
                               ManifestPath = RepositoryPaths$ManifestPath,
                               con = con, verbose = TRUE,
                               LogPath = LogPath, RunId = RunId)
repo_audit$issues

################################################################################
#### HCUP-specific post-load capabilities ######################################
################################################################################
#### The canonical seven-stage repository build is complete above. The #########
#### remaining examples demonstrate healthcare dictionaries, survey-weighted ###
#### estimates, repository summaries, and HCUP-oriented analytical queries. ####
################################################################################

################################################################################
#### Data dictionary: find variables and validate content against it ###########
################################################################################
#### Schema discovery harvests available variable and value labels from ########
#### labeled sources. Finalization writes approved mappings to #################
#### TableSchemas.xlsx for search, decoding, and content validation. ###########
# search_labels("payer", TableSchemaPath = RepositoryPaths$TableSchemaPath, ####
#               ParquetBasePath = ParquetBasePath) #############################
# search_labels("^DIED$", TableSchemaPath = RepositoryPaths$TableSchemaPath, ###
#               search_in = "column") ##########################################
#### validate_against_dictionary() checks stored values against each ###########
#### labeled column's code domain (e.g. DIED must be 0/1) and reports the ######
#### out-of-domain share per column -- content integrity, worst-first. #########
#### Caveat: continuous HCUP variables often label only special codes ##########
#### (e.g. 999 = missing), so interpret high percentages with the ##############
#### DomainSize column in view rather than as automatic errors. ################
################################################################################
# dict_check <- validate_against_dictionary(con, TableSchemaPath = RepositoryPaths$TableSchemaPath,
#                                           tables = c("NIS_Core"))
# head(dict_check, 25)

###############################################################################
#### Survey-weighted national estimates #######################################
###############################################################################
#### HCUP records are a sample; national estimates need the survey weight  ####
#### (DISCWT). These helpers handle missing weights/values correctly and   ####
#### return point estimates. For standard errors use the survey package    ####
#### with the full design (NIS_STRATUM strata, HOSP_NIS clusters).         ####
###############################################################################
# hcup_weighted_count(con, "NIS_Core", by = "YEAR")
# hcup_weighted_mean(con, "NIS_Core", value_col = "LOS", by = "YEAR", where = "AGE >= 65")

################################################
#### Number of tables in the final database ####
################################################
tables <- DBViewSummary(con = con, verbose = TRUE, logStatus = FALSE)

############################################
#### Obtain number of records per table ####
############################################
(CountTable <- DBDimPerTable(con = con, verbose = TRUE, logStatus = TRUE, orderByMemBurden = TRUE))
WorkbookUpdateopenxlsx(WBPath = SupportingInfoPath, DTAdd = CountTable, SheetName = "CountTable")
CountTable <- openxlsx::read.xlsx(SupportingInfoPath, sheet = "CountTable")

#########################
#### Example queries ####
#########################
#### Examples loading all records (unbounded pulls into R memory -- keep    ####
#### commented; use the LIMIT variants below to inspect structure)          ####
# testquery <- dbGetQuery(con, "SELECT * FROM TQP_HOSPITALEVENTS"); dim(testquery)
# head(testquery)
# testquery <- dbGetQuery(con, "SELECT * FROM TQP_PREEXISTINGCONDITIONS"); dim(testquery)
# head(testquery)
#### Examples loading only top 5 records ####
testquery <- dbGetQuery(con, "SELECT * FROM TQP_HOSPITALEVENTS LIMIT 5"); dim(testquery)
testquery
testquery <- dbGetQuery(con, "SELECT * FROM TQP_PREEXISTINGCONDITIONS LIMIT 5"); dim(testquery)
testquery
#### Example filtering by diagnosis ####
testquery <- dbGetQuery(con, "SELECT * FROM TQP_ICDDIAGNOSIS WHERE ICDDIAGNOSISCODE = 'S02.402A' "); dim(testquery)
head(testquery)
#### Aggregating NIS age by YEAR ####
testquery <- dbGetQuery(con, "SELECT YEAR,
                              avg(AGE) AS ave_age
                              FROM NIS_Core
                              GROUP BY YEAR
                              ORDER BY YEAR"); dim(testquery)
testquery
#### Aggregating NRD Records by YEAR >= 2015 ####
testquery <- dbGetQuery(con, "SELECT YEAR as YEAR,
                                    COUNT(*) AS n
                              FROM NRD_Core
                              WHERE YEAR >= 2015
                              GROUP BY YEAR
                              ORDER BY YEAR"); dim(testquery)
testquery
#### NSQIP specific CPT code count by YEAR >= 2018 ####
testquery <- dbGetQuery(con, "SELECT YEAR,
                                     COUNT(*) AS n_cases
                              FROM NSQIP_DBTable
                              WHERE CPT = '44950' AND YEAR >= 2018
                              GROUP BY YEAR
                              ORDER BY YEAR"); dim(testquery)
testquery
#### NIS_Core sampling records ####
testquerySample <- dbGetQuery(con, "SELECT YEAR,
                                  COUNT(*) AS n_cases_Sample
                              FROM NIS_Core
                              TABLESAMPLE BERNOULLI(0.1%)
                              GROUP BY YEAR"); dim(testquerySample)
testquery <- dbGetQuery(con, "SELECT YEAR,
                                  COUNT(*) AS n_cases
                              FROM NIS_Core
                              GROUP BY YEAR"); dim(testquery)
merge(testquerySample, testquery, by = "YEAR")

#### Screening multiple columns in large table queries ####
###########################################################
testquery <- dbGetQuery(con, "SELECT * FROM NIS_DX_PR_GRPS LIMIT 5"); dim(testquery)
testquery[1:5,1:5]

#### Complex queries screening tables too large to fit into memory ####
#######################################################################
tableAvail <- ColumnAvailabilityView(SupportingInfoPath = SupportingInfoPath, table_name = "NIS_DX_PR_GRPS")
CountTable <- tableAvail[["table"]];
CountTable[grepl("^DXCCSR_", CountTable$column),]

#### Screen for a 3 in both the DXCCSR_EXT003 or DXCCSR_END002 columns
testquery <- dbGetQuery(con, "SELECT YEAR, HOSPID, KEY, DXMCCS1, E_MCCS1, PRMCCS1, HOSP_NIS, KEY_NIS, DXCCSR_EXT003, DXCCSR_END002
FROM NIS_DX_PR_GRPS
WHERE COLUMNS('^(DXCCSR_EXT003|DXCCSR_END002)$') IN (3)")
table(testquery$DXCCSR_END002); table(testquery$DXCCSR_EXT003)

#### Screen for a 3 in either the DXCCSR_EXT003 or DXCCSR_END002 columns
testquery <- dbGetQuery(con, "
  SELECT YEAR, HOSPID, KEY, DXMCCS1, E_MCCS1, PRMCCS1, HOSP_NIS, KEY_NIS, DXCCSR_EXT003, DXCCSR_END002
  FROM NIS_DX_PR_GRPS
  WHERE len(list_filter([DXCCSR_EXT003, DXCCSR_END002, DXCCSR_BLD001, DXCCSR_BLD002], x -> x = 3)) > 0
")
table(testquery$DXCCSR_END002); table(testquery$DXCCSR_EXT003)


#### Screen for a 1, 2, or 3 across all numeric columns that start with DXCCSR_ ####
####################################################################################
#### Determine which DXCCSR_* columns are numeric ####
testquery <- dbGetQuery(con, "SELECT COLUMNS('^DXCCSR_')
                              FROM NIS_DX_PR_GRPS
                              WHERE YEAR IN (2018, 2020)
                              LIMIT 1000")
Classes <- sapply(testquery, class); table(Classes)
#### Determine which DXCCSR_* columns are numeric ####
numeric_cols <- colnames(testquery)[Classes == "numeric"]
varchar_cols  <- colnames(testquery)[Classes == "character"]
message(sprintf("%d numeric DXCCSR_ columns, %d character DXCCSR_ columns",
                length(numeric_cols), length(varchar_cols)))
#### build the SQL column set to filter on ####
col_list <- paste0("[", paste(paste0('"', numeric_cols, '"'), collapse = ", "), "]")
#### Create the columns that are returned in the SQL query ####
select_cols <- paste(c("YEAR", "HOSPID", "KEY", "DXMCCS1", "E_MCCS1", "PRMCCS1", "HOSP_NIS", "KEY_NIS",
                    paste0('"', numeric_cols, '"')), collapse = ", ")
#### Assemble and run the full query that queries only years 2018 and 2020 ####
full_sql <- glue('SELECT {select_cols}
                 FROM NIS_DX_PR_GRPS
                 WHERE YEAR IN (2018, 2020)
                   AND len(list_filter({col_list}, x -> x IN (1, 2, 3))) > 0', .open = "{", .close = "}")
#### Run the query ####
testquery_all <- dbGetQuery(con, full_sql)
message(sprintf("Rows returned: %s", formatC(nrow(testquery_all), format = "d", big.mark = ",")))
dim(testquery_all)
#### retrieve the columns in the first row of the table that do not contain a zero ####
temp <- c(rep(TRUE, 8), !t(testquery_all[1,numeric_cols])[,1] == 0); temp[is.na(temp)] <- FALSE
testquery_all[1,temp]

#### Example using a pivot approach ####
#### Note: this approach will take longer since it will screen a melted table that has far greater rows.
#### but will load all results into memory since a more refined dataset is returned
col_list <- glue::glue_collapse( paste0('"', numeric_cols, '"'), sep = ", ")
full_sql <- glue::glue("
    WITH unpivoted AS (
      UNPIVOT NIS_DX_PR_GRPS
      ON {col_list}
      INTO NAME ccsr_column VALUE ccsr_value
    )
    SELECT DISTINCT YEAR, HOSPID, KEY, DXMCCS1, E_MCCS1, PRMCCS1, HOSP_NIS, KEY_NIS, ccsr_column, ccsr_value
    FROM unpivoted
    WHERE CAST(ccsr_value AS INTEGER) IN (1, 2, 3)
  ", .open = "{", .close = "}")
testquery_unpivot <- dbGetQuery(con, full_sql)
message(sprintf("UNPIVOT approach: %d rows", nrow(testquery_unpivot)))
#### counts by YEAR that are returned ####
table(testquery_unpivot$YEAR)


##########################################################################################################
#### Build a column by YEAR table so that one can understand what columns are available for each YEAR ####
##########################################################################################################
availability <- column_availability(con = con, table_name = "NIS_DX_PR_GRPS", ParquetBasePath = ParquetBasePath) # "NIS_Core"
(availabilityNonNA <- availability[["PercentageNonNA"]])
#### Perform counts for each table and save results to workbook ####
ColumnAvailabilityCompile(con = con, tables = dbListTables(con), ParquetBasePath = ParquetBasePath,
                          StartAt = 18, verbose = TRUE, logStatus = TRUE, SupportingInfoPath = SupportingInfoPath)

###########################################################
#### Obtain column availability across different years ####
###########################################################
tableAvail <- ColumnAvailabilityView(SupportingInfoPath = SupportingInfoPath, table_name = "NEDS_Core_MISC"); names(tableAvail)
tableAvail[["table"]]; tableAvail[["heatmap"]]

#############################################
#### Create a relational database scheme ####
#############################################


###################################
#### Test diagnosis code query ####
###################################
DiagnosisCodesDF <- openxlsx::read.xlsx("C:/Users/e282219/Downloads/github/CECORC/inst/Misc/ExampleICDCodes.xlsx", sheet = "Sheet1")
DiagnosisCodesDF$Code <- gsub(" ", "", DiagnosisCodesDF$Code) # library(tictoc); tic()
DiagnosisSearchExact <- search_diagnosis_codes(con = con, codes = unique(DiagnosisCodesDF$Code),
                                       col_filter = "dx|diag|icd|ecode|dcode|pcode|ais|code|ccs|drg|cpt|proc", # If NULL, will search the entire database as opposed to only columns that are likely to contain the ICD codes
                                       match_type = "exact", tables = NULL, min_rows = 1L, verbose = TRUE); #toc() # 1783 sec elapsed, 29.71667 minutes
(DiagnosisSearch <- DiagnosisSearchExact[["DiagnosisSearch"]])
WorkbookUpdateopenxlsx(WBPath = "C:/Users/e282219/Downloads/github/CECORC/inst/Misc/ExampleICDCodes.xlsx",
                       DTAdd = DiagnosisSearch, SheetName = "DiagnosisSearchExact")
(DiagnosisSearchSummary <- DiagnosisSearchExact[["DiagnosisSearchSummary"]])
WorkbookUpdateopenxlsx(WBPath = "C:/Users/e282219/Downloads/github/CECORC/inst/Misc/ExampleICDCodes.xlsx",
                       DTAdd = DiagnosisSearchSummary, SheetName = "DiagnosisSearchSummary")
openxlsx::read.xlsx("C:/Users/e282219/Downloads/github/CECORC/inst/Misc/ExampleICDCodes.xlsx", sheet = "DiagnosisSearchExact")
openxlsx::read.xlsx("C:/Users/e282219/Downloads/github/CECORC/inst/Misc/ExampleICDCodes.xlsx", sheet = "DiagnosisSearchSummary")
