
################################################################################
#### Optimized National Database Loader ########################################
################################################################################
library(data.table); library(openxlsx); library(DBI); library(duckdb);
library(haven); library(arrow); library(glue); library(future); library(future.apply)
library(ComplexHeatmap); library(RColorBrewer); library(circlize)

################################################################################
#### Configuration #############################################################
################################################################################
DBSourceFilePath <- "C:/Users/e282219/Downloads/github/CECORC/R/DBFunctions_PackageDevelopmentV18.R"
source(DBSourceFilePath)
MasterDBPath <- "X:/National Databases"
FormattedDBPath <- "X:/Brendan/NationalDatabases/formattedDatabases"
ParquetBasePath <- file.path(FormattedDBPath, "parquet")
CheckpointPath <- file.path(FormattedDBPath, "load_checkpoint.rds")
LogPath <- file.path(FormattedDBPath, "load_log.txt")
SupportingInfoPath <- "C:/Users/e282219/Downloads/github/CECORC/inst/Misc/DatabaseLoadInfo.xlsx"
RepositoryPaths <- RepositoryInitialize(FormattedDBPath = FormattedDBPath,
                                        ParquetBasePath = ParquetBasePath,
                                        CheckpointPath = CheckpointPath,
                                        LogPath = LogPath,
                                        profile = "hcup")
SchemaRegistryPath <- RepositoryPaths$SchemaRegistryPath
TableSchemaPath <- RepositoryPaths$TableSchemaPath
ManifestPath <- RepositoryPaths$ManifestPath
DataContractPath <- RepositoryPaths$DataContractPath
RunId <- new_repository_run_id()
SAV_CHUNK_SIZE <- 4000000L
n_workers <- min(15L, max(1L, parallel::detectCores() - 1L))
dir.create(ParquetBasePath, recursive = TRUE, showWarnings = FALSE)

###############################################################################
#### Cleanup handler ##########################################################
#### Install once per R session: re-sourcing this script must not wrap the ####
#### error handler around itself or stack duplicate exit finalizers. ##########
###############################################################################
.cleanup_con <- function() {
  if (exists("con", envir = .GlobalEnv) && DBI::dbIsValid(get("con", envir = .GlobalEnv))) {
    log_msg("Script exiting. Closing DuckDB connection.")
    DBI::dbDisconnect(get("con", envir = .GlobalEnv), shutdown = TRUE)
  }
}
if (!isTRUE(getOption("CECORC.cleanup_installed"))) {
  .prior_error_handler <- getOption("error")
  .cleanup_on_error <- function() {
    try(.cleanup_con(), silent = TRUE)
    if (is.function(.prior_error_handler)) {
      .prior_error_handler()
    } else if (!is.null(.prior_error_handler)) {
      eval(.prior_error_handler)
    }
  }
  options(error = .cleanup_on_error, CECORC.cleanup_installed = TRUE)
  reg.finalizer(.GlobalEnv, function(e) .cleanup_con(), onexit = TRUE)
}

##############################
#### Load Master DB Table ####
##############################
#### DBSetupV2.xlsx drives hive partitioning with two columns (the legacy    ####
#### Year column has been retired -- its values now live in PartitionValue): ####
####   PartitionKey   -- hive key name(s) for each file's partition dir      ####
####   PartitionValue -- the value(s) that file belongs to                   ####
#### Classic year partitioning:  PartitionKey = "year", PartitionValue =     ####
#### "2019"  -> parquet/<DB>_<Table>/year=2019/*.parquet                     ####
#### Site partitioning:          PartitionKey = "SITE", PartitionValue =     ####
#### "MGH"   -> parquet/<DB>_<Table>/site=MGH/*.parquet                      ####
#### Nested partitions use ";" in both:                                      ####
####   PartitionKey = "SITE;YEAR", PartitionValue = "MGH;2019"               ####
####   -> parquet/<DB>_<Table>/site=MGH/year=2019/*.parquet                  ####
#### Checkpoint identity includes PartitionKey for generalized partitions;   ####
#### classic year rows remain bit-identical to old Year-based keys, so       ####
#### previously loaded files stay recognized. Rules enforced by              ####
#### ValidateMDTPreflight before anything is written: all rows of one table  ####
#### share the same PartitionKey; YEAR-keyed values must be whole years;     ####
#### no two values may sanitize to the same directory. Legacy workbooks      ####
#### with a Year column still work (blank spec falls back to YEAR + Year).   ####
#### Optional column AcceptPartial: set TRUE on a row after verifying that   ####
#### its file is permanently truncated (rows_written < declared ncases) and  ####
#### you accept the partial data -- the loader then checkpoints it with a    ####
#### [WARN] and manifest Status="partial_accepted" instead of failing and    ####
#### re-reading the whole file on every run. Verified-empty files (declared  ####
#### and confirmed 0 rows) checkpoint automatically with Status="empty".     ####
MDT <- openxlsx::read.xlsx("C:/Users/e282219/Downloads/github/CECORC/inst/Misc/DBSetupV2.xlsx", sheet = "Sheet1")

#### ONE-TIME MIGRATION (2026-07-11): the case-collision preflight found     ####
#### three tables whose workbook spellings differed only by case, and the    ####
#### workbook was normalized to: NRD/Core (was CORE), NIS/Misc (was MISC),   ####
#### NEDS/Core_Misc (was Core_MISC). A renamed TableName changes checkpoint  ####
#### identity, so run the migration below ONCE on the loading machine        ####
#### (DryRun = TRUE first) to keep already-loaded files recognized. Remove   ####
#### this block after it has been run.                                       ####
# rename_checkpoint_table(CheckpointPath, MDT, "NRD",  "CORE",      "Core",      ManifestPath, DryRun = TRUE)
# rename_checkpoint_table(CheckpointPath, MDT, "NIS",  "MISC",      "Misc",      ManifestPath, DryRun = TRUE)
# rename_checkpoint_table(CheckpointPath, MDT, "NEDS", "Core_MISC", "Core_Misc", ManifestPath, DryRun = TRUE)
#### REVIEW NEEDED: the workbook also contains NRD/CostChargeRatio (3 rows)  ####
#### and NRD/CosttoChargeRatios (6 rows) -- two DuckDB tables that look like ####
#### one dataset split by naming drift. If they are the same product, pick   ####
#### one name, update the rows, and migrate with rename_checkpoint_table();  ####
#### if genuinely different, delete this note.                               ####
#### Optionally, once loaded checkpoints are migrated to generalized keys:   ####
# migrate_checkpoint_keys(CheckpointPath, MDT, DryRun = TRUE)

#### Optional: new-release onboarding. Scans every MDBDir the workbook       ####
#### references for .sav/.csv files that have no MDT row yet and proposes    ####
#### candidate rows (Database/TableName/year guessed from filenames, flagged ####
#### NeedsReview where a guess failed). Review the output, correct it, and   ####
#### paste the rows into DBSetupV2.xlsx -- nothing is written automatically. ####
# new_files <- scan_for_new_source_files(MasterDBPath = MasterDBPath, MDT = MDT,
#                                        OutputPath = file.path(FormattedDBPath, "NewSourceFiles.xlsx"))
ValidateMDTPreflight(MDT = MDT, strict = TRUE, logStatus = TRUE,
                     ParquetBasePath = ParquetBasePath,
                     MaxFileStemTruncate = TRUE,
                     TerminalHivePartition = FALSE,
                     MasterDBPath = MasterDBPath, LogPath = LogPath, RunId = RunId)
MDTCompleteStatus(MDT = MDT, CheckpointPath = CheckpointPath, verbose = TRUE, logStatus = TRUE)

###############################################################################
#### Preflight: catalogue all table schemas across all databases #############
###############################################################################
#### Establishes every column's type up front (inference + schema registry)  ####
#### and writes the full cross-database catalogue to TableSchemas.xlsx,      ####
#### where it can be reviewed and curated. To pin a column type manually:    ####
#### edit its CanonicalType in the TableSchemas sheet, set that row's Source ####
#### to "manual", and save -- manual rows survive re-runs of this preflight  ####
#### and outrank both inference and the registry. ParquetBackEndCreate below ####
#### reads this catalogue instead of re-inferring types, so resumed runs     ####
#### also skip the expensive per-file sampling pass. Note a manual edit only ####
#### affects future writes: to apply it to a table already on disk, use the  ####
#### reset_table_for_reload() step after ParquetBackEndCreate.               ####
#### DBLoad derives from the workbook so a database added to DBSetupV2.xlsx  ####
#### is catalogued and loaded automatically. To stage a partial load,        ####
#### override with an explicit subset, e.g. DBLoad <- c("NIS", "NRD").       ####
DBLoad <- sort(unique(MDT$Database))
repository_catalog <- BuildRepositoryCatalog(MDT = MDT,
                                             DBLoad = DBLoad,
                                             MasterDBPath = MasterDBPath,
                                             n_workers = n_workers,
                                             SchemaRegistryPath = SchemaRegistryPath,
                                             TableSchemaPath = TableSchemaPath,
                                             StrictSchemaValidation = TRUE,
                                             LogPath = LogPath, RunId = RunId)

##############################################################################################
#### Convert Datasets to parquet files and store in a formal database directory structure ####
##############################################################################################
run_result <- ParquetBackEndCreate(MDT = MDT,
                                              DBLoad = DBLoad,
                                              MasterDBPath = MasterDBPath,
                                              completed_checkpoint = load_checkpoint(path = CheckpointPath),
                                              CheckpointPath = CheckpointPath,
                                              ParquetBasePath = ParquetBasePath,
                                              LogPath = LogPath,
                                              n_workers = n_workers,
                                              MaxFileStemTruncate = TRUE,
                                              PrintStatus = TRUE,
                                              PartitionBy = "FAIL",
                                              TerminalHivePartition = FALSE,
                                              RAMThreshold = 30,
                                              SAV_ROW_THRESHOLD = 4000000L,
                                              SAV_CHUNK_SIZE = SAV_CHUNK_SIZE,
                                              chunk_size_decrement = NULL,
                                              min_chunk_size = NULL,
                                              SchemaRegistryPath = SchemaRegistryPath,
                                              ManifestPath = ManifestPath,
                                              TableSchemaPath = TableSchemaPath,
                                              StrictPreflight = TRUE,
                                              StrictSchemaValidation = TRUE,
                                              RunPreflight = FALSE, # already validated explicitly above -- skips a second full network scan
                                              SourceFingerprintMode = "metadata",
                                              StopOnFileError = TRUE,
                                              ReturnRunResult = TRUE,
                                              RunId = RunId,
                                              MaxCoerceNAPct = 25) # fail a file when type coercion destroys >25% of a column's present values;
                                                                   # per-column damage totals land in CoercionReport.csv next to the manifest
#### Note: the loader snapshots the four state files (checkpoint, manifest,  ####
#### catalog, registry) to <FormattedDBPath>/StateBackups/<timestamp>/ at    ####
#### the start of every run (SnapshotState = TRUE, keep-last-20), so any     ####
#### divergence audit_repository() finds later is recoverable.               ####
completed_checkpoint <- run_result$checkpoint
print(run_result)
log_msg(sprintf("Checkpoint after load: %d files recorded", length(completed_checkpoint)))

###############################################################################
#### Optional: force one table to rebuild under the current schema ###########
###############################################################################
#### Parquet already on disk keeps the column types it was written with;     ####
#### changes to the schema registry or manual TableSchemas.xlsx edits only   ####
#### affect future writes. To apply them to an existing table, clear that    ####
#### table and reload it. DryRun = TRUE (the default) deletes nothing and    ####
#### only reports what would be removed; once it looks right, set            ####
#### DryRun = FALSE and then re-run the ParquetBackEndCreate step above to   ####
#### rebuild the table from source.                                          ####
# reset_table_for_reload(MDT = MDT, Database = "NIS", TableName = "Core",
#                        ParquetBasePath = ParquetBasePath,
#                        CheckpointPath = CheckpointPath,
#                        ManifestPath = ManifestPath, DryRun = TRUE)

################################################################################
#### Summary & Verification ####################################################
################################################################################
SummaryVerification(MDT = MDT, CheckpointPath = CheckpointPath, LogPath = LogPath,
                    logStatus = FALSE, RunId = run_result$run_id,
                    MasterDBPath = MasterDBPath, SourceFingerprintMode = "metadata")

##########################################################################################
#### Create a single persistent connection to DuckDB #####################################
##########################################################################################
dir.create(file.path(FormattedDBPath, "duckdb_temp"), recursive = TRUE, showWarnings = FALSE)
con <- open_duckdb(FormattedDBPath = FormattedDBPath,                 # To create a new view, change the directory
                   DBName = "DuckDBRelationalDatabase.duckdb",        # To create a new view, change the name
                   TempDirPath = 'X:/Brendan/NationalDatabases/formattedDatabases/duckdb_temp',
                   GB = '48GB', ProgressBar = TRUE, ReadOnly = FALSE) # adjust RAM to ~75% of your RAM: 64 * 0.75 = 48 GB

##########################
#### Build viwes to DB ###
##########################
completed_checkpoint <- load_checkpoint(path = CheckpointPath)
completed_mdt <- MDT[checkpoint_completed_mask(MDT, completed_checkpoint),]
register_parquet_view_compile(con = con, ParquetBasePath = ParquetBasePath, verbose = TRUE, logStatus = FALSE,
                               SchemaRegistryPath = SchemaRegistryPath,
                               TableSchemaPath = TableSchemaPath,
                               validate = TRUE, strict_validation = TRUE,
                              tables_written = unique(repository_table_names(completed_mdt)),
                              LogPath = LogPath, RunId = RunId )
contract_results <- validate_data_contracts(con, DataContractPath, strict = TRUE,
                                            LogPath = LogPath, RunId = RunId)

#####################################################################
#### Adjust connection to read only, which is faster for queries ####
#####################################################################
if(exists("con") && DBI::dbIsValid(con)){ DBI::dbDisconnect(con, shutdown = TRUE) }
con <- open_duckdb(FormattedDBPath = FormattedDBPath, DBName = "DuckDBRelationalDatabase.duckdb",
                   TempDirPath = 'X:/Brendan/NationalDatabases/formattedDatabases/duckdb_temp',
                   GB = '48GB', ProgressBar = TRUE, ReadOnly = TRUE)

###############################################################################
#### Repository reconciliation (fsck) ########################################
###############################################################################
#### Cross-checks the four sources of truth -- checkpoint, manifest, Parquet ####
#### on disk, and DuckDB row counts -- and reports any divergence (files the ####
#### manifest claims but disk lost, orphan parquet no manifest row claims,   ####
#### checkpointed rows with no output, per-table count mismatches). Purely   ####
#### a report: it deletes and changes nothing. Inspect the returned detail   ####
#### tables (e.g. repo_audit$orphan_parquet) and fix via                     ####
#### reset_table_for_reload() or by re-running the loader.                   ####
repo_audit <- audit_repository(MDT = MDT,
                               ParquetBasePath = ParquetBasePath,
                               CheckpointPath = CheckpointPath,
                               ManifestPath = ManifestPath,
                               con = con, verbose = TRUE,
                               LogPath = LogPath, RunId = RunId)
repo_audit$issues

###############################################################################
#### Data dictionary: find variables and validate content against it #########
###############################################################################
#### search_labels() queries the Labels sheet BuildRepositoryCatalog         ####
#### harvested from the SPSS headers: find variables across all tables by    ####
#### label text, column name, or value-label text.                           ####
# search_labels("payer", TableSchemaPath = TableSchemaPath, ParquetBasePath = ParquetBasePath)
# search_labels("^DIED$", TableSchemaPath = TableSchemaPath, search_in = "column")
#### validate_against_dictionary() checks stored values against each         ####
#### labeled column's code domain (e.g. DIED must be 0/1) and reports the    ####
#### out-of-domain share per column -- content integrity, worst-first.       ####
#### Caveat: continuous HCUP variables often label only special codes        ####
#### (e.g. 999 = missing), so interpret high percentages with the            ####
#### DomainSize column in view rather than as automatic errors.              ####
# dict_check <- validate_against_dictionary(con, TableSchemaPath = TableSchemaPath,
#                                           tables = c("NIS_Core"))
# head(dict_check, 25)

###############################################################################
#### Survey-weighted national estimates #######################################
###############################################################################
#### HCUP records are a sample; national estimates need the survey weight    ####
#### (DISCWT). These helpers handle missing weights/values correctly and     ####
#### return point estimates. For standard errors use the survey package      ####
#### with the full design (NIS_STRATUM strata, HOSP_NIS clusters).           ####
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
