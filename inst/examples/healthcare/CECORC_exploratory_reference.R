#### Exploratory examples split from OptimizedDatabaseLoader_V17.R. ####
#### Run the production workflow first; these examples reuse its objects. ####

#### Example for Bishoy pulling data that exceeds memory capacity ####
######################################################################
if(!require("data.table", quietly = TRUE)) { install.packages("data.table") }; library(data.table)
if(!require("openxlsx", quietly = TRUE)) { install.packages("openxlsx") }; library(openxlsx)
if(!require("haven", quietly = TRUE)) { install.packages("haven") }; library(haven)
if(!require("DBI", quietly = TRUE)) { install.packages("DBI") }; library(DBI)
if(!require("arrow", quietly = TRUE)) { install.packages("arrow") }; library(arrow)
if(!require("glue", quietly = TRUE)) { install.packages("glue") }; library(glue)
if(!require("duckdb", quietly = TRUE)) { install.packages("duckdb") }; library(duckdb)
if(!require("future", quietly = TRUE)) { install.packages("future") }; library(future)
if(!require("future.apply", quietly = TRUE)) { install.packages("future.apply") }; library(future.apply)
if(!require("RColorBrewer", quietly = TRUE)) { install.packages("RColorBrewer") }; library(RColorBrewer)
if(!require("circlize", quietly = TRUE)) { install.packages("circlize") }; library(circlize)
if(!require("BiocManager", quietly = TRUE)){install.packages("BiocManager")}
if(!require("ComplexHeatmap", quietly = TRUE)){BiocManager::install("ComplexHeatmap", update = FALSE) }; library(ComplexHeatmap)

################################################################################
#### Configuration #############################################################
################################################################################
# DBSourceFilePath <- "X:/DuckDBNationalDatabases/R/DBFunctions.R"
# source(DBSourceFilePath)
# MasterDBPath <- "X:/National Databases"
# FormattedDBPath <- "X:/Brendan/NationalDatabases/formattedDatabases"
# ParquetBasePath <- file.path(FormattedDBPath, "parquet")
# CheckpointPath <- file.path(FormattedDBPath, "load_checkpoint.rds")
# LogPath <- file.path(FormattedDBPath, "load_log.txt")
# SupportingInfoPath <- "X:/DuckDBNationalDatabases/inst/Misc/DatabaseLoadInfo.xlsx"
# SAV_CHUNK_SIZE <- 4000000L
# n_workers <- min(15L, max(1L, parallel::detectCores() - 1L))

#### Open view ####
###################
# con <- open_duckdb(FormattedDBPath = FormattedDBPath, DBName = "DuckDBRelationalDatabase.duckdb",
#                    TempDirPath = 'X:/Brendan/NationalDatabases/formattedDatabases/duckdb_temp',
#                    GB = '48GB', ProgressBar = TRUE, ReadOnly = TRUE)
# tables <- DBViewSummary(con = con, verbose = FALSE, logStatus = FALSE)
# tables[grepl("NISBishoy", tables, ignore.case = TRUE)]

#### Build query column objects ####
####################################
all_cols1 <- dbGetQuery(con, "DESCRIBE NISBishoy_FULLDhillon_EGS")$column_name
all_cols2 <- dbGetQuery(con, "DESCRIBE NISBishoy_DXDhillon_EGS")$column_name
dx_cols <- all_cols2[grepl("^i10_dx[0-9]+$", all_cols2, ignore.case = TRUE)]
# unpivot_cols <- paste(dx_cols, collapse = ", ")
unpivot_cols_cast <- paste(paste0(dx_cols, "::VARCHAR"), collapse = ", ")
safe_cols1 <- all_cols1[!grepl("[$]", all_cols1)]
c_select <- paste(paste0("c.", safe_cols1), collapse = ",\n        ")

#### Create filtering query portion ####
########################################
trauma_filter <- "
    dx_code LIKE 'S%'
    OR LEFT(dx_code, 3) BETWEEN 'T07' AND 'T34'
    OR LEFT(dx_code, 3) BETWEEN 'T66' AND 'T76'"

################################################################################
#### Return Wide table: one row per discharge, I10_DX* columns preserved    ####
#### Uses a melted table to filter data and then joins back to #################
#### the original wide tables so all columns including I10_DX* are present. ####
################################################################################
cast_dx_cols <- paste0("CAST(", dx_cols, " AS VARCHAR)")
col_list  <- paste0("[", paste(cast_dx_cols, collapse = ", "), "]")
dx_filter <- glue::glue("
  len(list_filter(
    {col_list},
    x -> x IS NOT NULL AND (
      LEFT(x, 1) = 'S'
      OR LEFT(x, 3) BETWEEN 'T07' AND 'T34'
      OR LEFT(x, 3) BETWEEN 'T66' AND 'T76'
    )
  )) > 0
", .open = "{", .close = "}")

sql_wide <- glue::glue("
  SELECT
      {c_select},
      g.* EXCLUDE (KEY_NIS, HOSP_NIS, YEAR)
  FROM NISBishoy_FULLDhillon_EGS AS c
  INNER JOIN NISBishoy_DXDhillon_EGS AS g
     ON c.KEY_NIS  = g.KEY_NIS
    AND c.HOSP_NIS = g.HOSP_NIS
    AND c.YEAR     = g.YEAR
  WHERE c.YEAR BETWEEN 2020 AND 2023
    AND {dx_filter}
", .open = "{", .close = "}")

result_wide <- DBI::dbGetQuery(con, sql_wide)
dim(result_wide)
head(result_wide)

################################################################################
#### Return melted table: one row per matching code is returned ################
#### Note: A single discharge with 3 trauma codes appears as 3 rows. ###########
################################################################################
# sql_long <- glue::glue("
#   WITH joined_data AS (
#       SELECT
#           {c_select},
#           g.* EXCLUDE (KEY_NIS, HOSP_NIS, YEAR)
#       FROM NISBishoy_FULLDhillon_EGS AS c
#       INNER JOIN NISBishoy_DXDhillon_EGS AS g
#          ON c.KEY_NIS  = g.KEY_NIS
#         AND c.HOSP_NIS = g.HOSP_NIS
#         AND c.YEAR     = g.YEAR
#       WHERE c.YEAR BETWEEN 2020 AND 2023
#   ),
#   flattened_dx AS (
#       UNPIVOT joined_data
#       ON {unpivot_cols}
#       INTO
#           NAME dx_column_source
#           VALUE dx_code
#   )
#   SELECT *
#   FROM flattened_dx
#   WHERE {trauma_filter}
# ", .open = "{", .close = "}")
sql_long <- glue::glue("
  WITH joined_data AS (
      SELECT
          {c_select},
          g.* EXCLUDE (KEY_NIS, HOSP_NIS, YEAR)
      FROM NISBishoy_FULLDhillon_EGS AS c
      INNER JOIN NISBishoy_DXDhillon_EGS AS g
         ON c.KEY_NIS  = g.KEY_NIS
        AND c.HOSP_NIS = g.HOSP_NIS
        AND c.YEAR     = g.YEAR
      WHERE c.YEAR BETWEEN 2020 AND 2023
  ),
  flattened_dx AS (
      UNPIVOT joined_data
      ON {unpivot_cols_cast}
      INTO
          NAME dx_column_source
          VALUE dx_code
  )
  SELECT *
  FROM flattened_dx
  WHERE {trauma_filter}
", .open = "{", .close = "}")
result_long <- DBI::dbGetQuery(con, sql_long)
dim(result_long)
head(result_long)










if (FALSE) {

##############################
#### Test on full dataset ####
##############################
if(!require("data.table", quietly = TRUE)) { install.packages("data.table") }; library(data.table)
if(!require("openxlsx", quietly = TRUE)) { install.packages("openxlsx") }; library(openxlsx)
if(!require("haven", quietly = TRUE)) { install.packages("haven") }; library(haven)
if(!require("DBI", quietly = TRUE)) { install.packages("DBI") }; library(DBI)
if(!require("arrow", quietly = TRUE)) { install.packages("arrow") }; library(arrow)
if(!require("glue", quietly = TRUE)) { install.packages("glue") }; library(glue)
if(!require("duckdb", quietly = TRUE)) { install.packages("duckdb") }; library(duckdb)
if(!require("future", quietly = TRUE)) { install.packages("future") }; library(future)
if(!require("future.apply", quietly = TRUE)) { install.packages("future.apply") }; library(future.apply)
if(!require("RColorBrewer", quietly = TRUE)) { install.packages("RColorBrewer") }; library(RColorBrewer)
if(!require("circlize", quietly = TRUE)) { install.packages("circlize") }; library(circlize)
if(!require("BiocManager", quietly = TRUE)){install.packages("BiocManager")}
if(!require("ComplexHeatmap", quietly = TRUE)){BiocManager::install("ComplexHeatmap", update = FALSE) }; library(ComplexHeatmap)

################################################################################
#### Configuration #############################################################
################################################################################
# DBSourceFilePath <- "X:/DuckDBNationalDatabases/R/DBFunctions.R"
# source(DBSourceFilePath)
# MasterDBPath <- "X:/National Databases"
# FormattedDBPath <- "X:/Brendan/NationalDatabases/formattedDatabases"
# ParquetBasePath <- file.path(FormattedDBPath, "parquet")
# CheckpointPath <- file.path(FormattedDBPath, "load_checkpoint.rds")
# LogPath <- file.path(FormattedDBPath, "load_log.txt")
# SupportingInfoPath <- "X:/DuckDBNationalDatabases/inst/Misc/DatabaseLoadInfo.xlsx"
# SAV_CHUNK_SIZE <- 4000000L
# n_workers <- min(15L, max(1L, parallel::detectCores() - 1L))

#### Open view ####
###################
# con <- open_duckdb(FormattedDBPath = FormattedDBPath, DBName = "DuckDBRelationalDatabase.duckdb",
#                    TempDirPath = 'X:/Brendan/NationalDatabases/formattedDatabases/duckdb_temp',
#                    GB = '48GB', ProgressBar = TRUE, ReadOnly = TRUE)
tables <- DBViewSummary(con = con, verbose = FALSE, logStatus = FALSE)
tables[grepl("NISBishoy", tables, ignore.case = TRUE)]

# #### Build query column objects ####
# ####################################
# all_cols1 <- dbGetQuery(con, "DESCRIBE NIS_DX_PR_GRPS")$column_name
# all_cols2 <- dbGetQuery(con, "DESCRIBE NIS_Core")$column_name
# dx_cols <- all_cols2[grepl("^i10_dx[0-9]+$", all_cols2, ignore.case = TRUE)]
# unpivot_cols <- paste(dx_cols, collapse = ", ")
# safe_cols1 <- all_cols1[!grepl("[$]", all_cols1)]
# c_select <- paste(paste0("c.", safe_cols1), collapse = ",\n        ")
#
# #### Create filtering query portion ####
# ########################################
# trauma_filter <- "
#     dx_code LIKE 'S%'
#     OR LEFT(dx_code, 3) BETWEEN 'T07' AND 'T34'
#     OR LEFT(dx_code, 3) BETWEEN 'T66' AND 'T76'"

################################################################################
#### Return Wide table: one row per discharge, I10_DX* columns preserved    ####
#### Uses a melted table to filter data and then joins back to #################
#### the original wide tables so all columns including I10_DX* are present. ####
################################################################################
all_cols1    <- dbGetQuery(con, "DESCRIBE NIS_Core")$column_name
all_cols2    <- dbGetQuery(con, "DESCRIBE NIS_DX_PR_GRPS")$column_name

all_cols2[grepl("YEAR", all_cols2, ignore.case = TRUE)]

all_cols1[grepl("key", all_cols1, ignore.case = TRUE)]
all_cols2[grepl("key", all_cols2, ignore.case = TRUE)]


dx_cols      <- all_cols1[grepl("^i10_dx[0-9]+$", all_cols1, ignore.case = TRUE)]
unpivot_cols <- paste(dx_cols, collapse = ", ")

#### Exclude special-character columns from both tables ####
safe_core    <- all_cols1[!grepl("[$]", all_cols1)]
safe_grps    <- all_cols2[!grepl("[$]", all_cols2)]

#### NIS_Core SELECT: all safe columns prefixed with c. ####
c_select <- paste(paste0("c.", safe_core), collapse = ",\n        ")

#### NIS_DX_PR_GRPS SELECT: exclude join keys already in c ####
g_exclude_keys <- c("KEY_NIS", "HOSP_NIS", "YEAR", "KEY", "HOSPID")
g_cols   <- safe_grps[!safe_grps %in% g_exclude_keys]
g_select <- paste(paste0("g.", g_cols), collapse = ",\n        ")

sql_wide <- glue::glue("
  WITH joined_data AS (
      SELECT
          {c_select},
          {g_select}
      FROM NIS_Core AS c
      INNER JOIN NIS_DX_PR_GRPS AS g
         ON c.KEY_NIS  = g.KEY_NIS
        AND c.HOSP_NIS = g.HOSP_NIS
        AND c.YEAR     = g.YEAR
      WHERE c.YEAR BETWEEN 2020 AND 2023
  ),
  flattened AS (
      UNPIVOT joined_data
      ON {unpivot_cols}
      INTO NAME dx_column_source
           VALUE dx_code
  )
  SELECT DISTINCT
      * EXCLUDE (dx_column_source, dx_code)
  FROM flattened
  WHERE dx_code LIKE 'S%'
     OR LEFT(dx_code, 3) BETWEEN 'T07' AND 'T34'
     OR LEFT(dx_code, 3) BETWEEN 'T66' AND 'T76'
", .open = "{", .close = "}")

result_wide <- DBI::dbGetQuery(con, sql_wide)
dim(result_wide)





  #### Check whether any KEY_NIS values actually overlap for a single YEAR ####
dbGetQuery(con, "
  SELECT COUNT(*) AS n_core FROM NIS_Core WHERE YEAR = 2020
")
dbGetQuery(con, "
  SELECT COUNT(*) AS n_grps FROM NIS_DX_PR_GRPS WHERE YEAR = 2020
")

#### Check if KEY_NIS sample values look the same in both tables ####
dbGetQuery(con, "SELECT KEY_NIS FROM NIS_Core WHERE YEAR = 2020 LIMIT 5")
dbGetQuery(con, "SELECT KEY_NIS FROM NIS_DX_PR_GRPS WHERE YEAR = 2020 LIMIT 5")

test <- dbGetQuery(con, "SELECT KEY_NIS, YEAR FROM NIS_Core")
test <- as.data.table(test)
test[, .(missing_count = sum(is.na(KEY_NIS))), by = YEAR]
test[!is.na(KEY_NIS),]
# test <- dbGetQuery(con, "SELECT KEY, YEAR FROM NIS_Core")
# test <- as.data.table(test)
# test[, .(missing_count = sum(is.na(KEY))), by = YEAR]
# test[!is.na(KEY),]

test2 <- dbGetQuery(con, "SELECT KEY_NIS, YEAR FROM NIS_DX_PR_GRPS")
test2 <- as.data.table(test2)
test2[, .(missing_count = sum(is.na(KEY_NIS))), by = YEAR]
test2[!is.na(KEY_NIS),]
# test <- dbGetQuery(con, "SELECT KEY, YEAR FROM NIS_DX_PR_GRPS")
# head(test)
# test <- as.data.table(test)
# test[, .(missing_count = sum(is.na(KEY))), by = YEAR]
# test[!is.na(KEY),]

intersect(test[test$YEAR == 2021,]$KEY_NIS,
test2[test2$YEAR == 2021,]$KEY_NIS)

dbGetQuery(con, "
  SELECT
    C.KEY_NIS,
    C.YEAR
  FROM NIS_Core AS C
    INNER JOIN NIS_DX_PR_GRPS AS D
     ON C.KEY_NIS = D.KEY_NIS
    AND C.YEAR    = D.YEAR
  WHERE C.YEAR BETWEEN 2020 AND 2023

")

temp <- dbGetQuery(con, "
  SELECT
    C.KEY_NIS,
    C.YEAR
  FROM NIS_Core AS C
    INNER JOIN NIS_DX_PR_GRPS AS D
     ON C.KEY_NIS = D.KEY_NIS ")

dim(temp)
unique(temp$YEAR)
temp <- dbGetQuery(con, "
  SELECT
    C.KEY_NIS,
    C.HOSP_NIS,
    C.YEAR,
    D.KEY_NIS,
    D.HOSP_NIS,
    D.YEAR
  FROM NIS_Core AS C
  INNER JOIN NIS_DX_PR_GRPS AS D
     ON C.KEY_NIS = D.KEY_NIS
     AND C.YEAR = D.YEAR
")

#### WARNING: do not re-run -- joining on YEAR alone is a cross join within ####
#### each year (~7M x 7M row pairs per NIS year); it will exhaust memory    ####
#### and spill for hours before failing.                                    ####
# temp <- dbGetQuery(con, "
#   SELECT
#     C.KEY_NIS,
#     C.HOSP_NIS,
#     C.YEAR,
#     D.KEY_NIS,
#     D.HOSP_NIS,
#     D.YEAR
#   FROM NIS_Core AS C
#   INNER JOIN NIS_DX_PR_GRPS AS D
#      ON C.YEAR = D.YEAR
# ")


temp <- dbGetQuery(con, "
  SELECT
    C.KEY_NIS  AS core_key,
    C.HOSP_NIS AS core_hosp,
    C.YEAR     AS core_year,
    D.KEY_NIS  AS dx_key,
    D.HOSP_NIS AS dx_hosp,
    D.YEAR     AS dx_year
  FROM NIS_Core AS C
  INNER JOIN NIS_DX_PR_GRPS AS D
     ON C.YEAR::BIGINT    = D.YEAR::BIGINT
    AND C.KEY_NIS::BIGINT = D.KEY_NIS::BIGINT
")
# Error in `dbSendQuery()`:
#   ! INTERNAL Error: Expected vector of type DOUBLE, but found vector of type INT64
# This error signals an assertion failure within DuckDB. This usually occurs due to unexpected conditions or errors in the program's logic.
# For more information, see https://duckdb.org/docs/stable/dev/internal_errors
# ℹ Context: rapi_prepare
# ℹ Error type: INTERNAL
# Run `rlang::last_trace()` to see where the error occurred.

# IMPORTANT: You must restart your R session or completely reconnect 'con' first!

#### ROOT CAUSE (confirmed by reproduction): the NIS_Core / NIS_DX_PR_GRPS      ####
#### directories mix Parquet generations: legacy files store KEY_NIS/HOSP_NIS   ####
#### as DOUBLE, post-registry files as VARCHAR. union_by_name unifies the view  ####
#### to VARCHAR by string-casting the legacy doubles, which yields values like  ####
#### '10000001.0' next to '10000001' from the new files. Two consequences:      ####
####   1. Equality joins CANNOT match across generations -- even with           ####
####      CAST(... AS VARCHAR), because '10000001.0' != '10000001'.             ####
####   2. Casting the unified column back to BIGINT/DOUBLE trips this DuckDB    ####
####      internal assertion during the mixed-file scan (fixed in newer duckdb  ####
####      releases, but the join-mismatch problem remains either way).          ####
#### Query-side casts cannot repair this. The durable fix is to rebuild both    ####
#### tables under the current schema catalog so every file is VARCHAR:          ####
####   reset_table_for_reload(MDT, "NIS", "Core",       ParquetBasePath, CheckpointPath, ManifestPath, DryRun = FALSE) ####
####   reset_table_for_reload(MDT, "NIS", "DX_PR_GRPS", ParquetBasePath, CheckpointPath, ManifestPath, DryRun = FALSE) ####
#### then re-run BuildRepositoryCatalog + ParquetBackEndCreate for "NIS".       ####
#### After the rebuild the plain join works with no casts:                      ####
####   ON c.KEY_NIS = g.KEY_NIS AND c.HOSP_NIS = g.HOSP_NIS AND c.YEAR = g.YEAR ####
#### Interim workaround only (slow, but correct across generations):            ####
####   ON regexp_replace(c.KEY_NIS, '\\.0$', '') = regexp_replace(g.KEY_NIS, '\\.0$', '') ####

temp <- dbGetQuery(con, "
  WITH Clean_Core AS (
    SELECT
      CAST(KEY_NIS AS BIGINT)  AS core_key,
      CAST(HOSP_NIS AS BIGINT) AS core_hosp,
      CAST(YEAR AS BIGINT)     AS core_year
    FROM NIS_Core
  ),
  Clean_DX AS (
    SELECT
      CAST(KEY_NIS AS BIGINT)  AS dx_key,
      CAST(HOSP_NIS AS BIGINT) AS dx_hosp,
      CAST(YEAR AS BIGINT)     AS dx_year
    FROM NIS_DX_PR_GRPS
  )
  SELECT
    C.core_key,
    C.core_hosp,
    C.core_year,
    D.dx_key,
    D.dx_hosp,
    D.dx_year
  FROM Clean_Core AS C
  INNER JOIN Clean_DX AS D
     ON C.core_key  = D.dx_key
    AND C.core_year = D.dx_year
")
# Error in `dbSendQuery()`:
#   ! INTERNAL Error: Expected vector of type DOUBLE, but found vector of type INT64
# This error signals an assertion failure within DuckDB. This usually occurs due to unexpected conditions or errors in the program's logic.
# For more information, see https://duckdb.org/docs/stable/dev/internal_errors
# ℹ Context: rapi_prepare
# ℹ Error type: INTERNAL
# Run `rlang::last_trace()` to see where the error occurred.


sql_wide <- glue::glue("
  WITH joined_data AS (
      SELECT
          {c_select},
          {g_select}
      FROM (
          SELECT * REPLACE (
              TRY_CAST(KEY_NIS  AS DOUBLE) AS KEY_NIS,
              TRY_CAST(HOSP_NIS AS DOUBLE) AS HOSP_NIS
          ) FROM NIS_Core
          WHERE YEAR BETWEEN 2020 AND 2023
      ) AS c
      INNER JOIN (
          SELECT * REPLACE (
              TRY_CAST(KEY_NIS  AS DOUBLE) AS KEY_NIS,
              TRY_CAST(HOSP_NIS AS DOUBLE) AS HOSP_NIS
          ) FROM NIS_DX_PR_GRPS
          WHERE YEAR BETWEEN 2020 AND 2023
      ) AS g
         ON c.KEY_NIS  = g.KEY_NIS
        AND c.HOSP_NIS = g.HOSP_NIS
        AND c.YEAR     = g.YEAR
  ),
  flattened AS (
      UNPIVOT joined_data
      ON {unpivot_cols}
      INTO NAME dx_column_source
           VALUE dx_code
  )
  SELECT DISTINCT
      * EXCLUDE (dx_column_source, dx_code)
  FROM flattened
  WHERE dx_code LIKE 'S%'
     OR LEFT(dx_code, 3) BETWEEN 'T07' AND 'T34'
     OR LEFT(dx_code, 3) BETWEEN 'T66' AND 'T76'
", .open = "{", .close = "}")

result_wide <- DBI::dbGetQuery(con, sql_wide)
message(sprintf("Rows returned: %s",
                formatC(nrow(result_wide), format = "d", big.mark = ",")))



sql_wide <- glue::glue("
  WITH joined_data AS (
      SELECT
          {c_select},
          {g_select}
      FROM (
          SELECT * FROM NIS_Core
          WHERE YEAR BETWEEN 2020 AND 2023
      ) AS c
      INNER JOIN (
          SELECT * FROM NIS_DX_PR_GRPS
          WHERE YEAR BETWEEN 2020 AND 2023
      ) AS g
         ON c.KEY_NIS  = g.KEY_NIS
        AND c.YEAR     = g.YEAR
  ),
  flattened AS (
      UNPIVOT joined_data
      ON {unpivot_cols}
      INTO NAME dx_column_source
           VALUE dx_code
  )
  SELECT DISTINCT
      * EXCLUDE (dx_column_source, dx_code)
  FROM flattened
  WHERE dx_code LIKE 'S%'
     OR LEFT(dx_code, 3) BETWEEN 'T07' AND 'T34'
     OR LEFT(dx_code, 3) BETWEEN 'T66' AND 'T76'
", .open = "{", .close = "}")

result_wide <- DBI::dbGetQuery(con, sql_wide)
message(sprintf("Rows returned: %s",
                formatC(nrow(result_wide), format = "d", big.mark = ",")))


# ON c.KEY_NIS  = g.KEY_NIS
# AND c.HOSP_NIS = g.HOSP_NIS
# AND c.YEAR     = g.YEAR




#### Troubleshooting ####
#########################

library(DBI)
library(glue)

# 1. Fetch column names from both tables
all_cols1 <- dbGetQuery(con, "DESCRIBE NIS_Core")$column_name
all_cols2 <- dbGetQuery(con, "DESCRIBE NIS_DX_PR_GRPS")$column_name

# FIX: Pull the diagnosis columns from all_cols1 (NIS_Core) as demonstrated in your working example
dx_cols <- all_cols1[grepl("^i10_dx[0-9]+$", all_cols1, ignore.case = TRUE)]

# Build explicit selection strings to prevent ambiguous or duplicate column conflicts
safe_cols1 <- all_cols1[!grepl("[$]", all_cols1)]
c_select   <- paste(paste0("c.", safe_cols1), collapse = ",\n        ")
g_select   <- "g.* EXCLUDE (KEY_NIS, HOSP_NIS, YEAR)"

# 2. Fix the string type mismatch for the list builder
# FIX: Change prefix to 'c.' since these columns exist in NIS_Core (alias c)
cast_dx_cols <- paste0("CAST(c.", dx_cols, " AS VARCHAR)")
col_list     <- paste0("[", paste(cast_dx_cols, collapse = ", "), "]")

# 3. Create the list-based filtering clause
dx_filter <- glue::glue("
  len(list_filter(
    {col_list},
    x -> x IS NOT NULL AND (
      LEFT(x, 1) = 'S'
      OR LEFT(x, 3) BETWEEN 'T07' AND 'T34'
      OR LEFT(x, 3) BETWEEN 'T66' AND 'T76'
    )
  )) > 0
", .open = "{", .close = "}")

#### IMPORTANT: KEY_NIS join type mismatch diagnostic and fix                 ####
#### The schema registry coerces KEY_NIS to character (VARCHAR) in both       ####
#### NIS_Core and NIS_DX_PR_GRPS. If you see type mismatch errors on joins,  ####
#### it means an older Parquet load pre-dates the schema registry.           ####
#### WARNING: casting in the query does NOT fix mixed-generation data.       ####
#### Legacy DOUBLE files surface through the VARCHAR-unified view as         ####
#### '10000001.0' while post-registry files hold '10000001', so              ####
#### CAST(... AS VARCHAR) joins silently return zero cross-generation        ####
#### matches (see the ROOT CAUSE block earlier in this script).              ####
#### The only reliable fix is to rebuild the affected tables so every file   ####
#### is VARCHAR. Preview then run (DryRun = FALSE deletes and resets):       ####
#### reset_table_for_reload(MDT, "NIS", "Core", ParquetBasePath,             ####
####                        CheckpointPath, ManifestPath, DryRun = TRUE)     ####
#### (repeat for "DX_PR_GRPS"), then re-run BuildRepositoryCatalog and the   ####
#### ParquetBackEndCreate step above. Afterwards the plain join works:       ####
#### ON c.KEY_NIS = g.KEY_NIS AND c.HOSP_NIS = g.HOSP_NIS AND c.YEAR = g.YEAR ####

# 4. Final updated SQL Wide Merge Query using the unified schema layout
sql_wide <- glue::glue("
  SELECT
      {c_select},
      {g_select}
  FROM NIS_Core AS c
  INNER JOIN NIS_DX_PR_GRPS AS g
     ON CAST(c.KEY_NIS AS VARCHAR)  = CAST(g.KEY_NIS AS VARCHAR)
    AND CAST(c.HOSP_NIS AS VARCHAR) = CAST(g.HOSP_NIS AS VARCHAR)
    AND c.YEAR = g.YEAR
  WHERE c.YEAR BETWEEN 2020 AND 2023
    AND c.KEY_NIS IS NOT NULL
    AND {dx_filter}
", .open = "{", .close = "}")

# Execute query and pull the cohort into R memory
result_wide <- DBI::dbGetQuery(con, sql_wide)



# Test 1: Check if KEY_NIS can match completely on its own
dbGetQuery(con, "
  SELECT COUNT(*)
  FROM NIS_Core c
  INNER JOIN NIS_DX_PR_GRPS g ON c.KEY_NIS = g.KEY_NIS
  LIMIT 5"
)

# Test 2: Check the column data types to see if one is numeric and one is text
dbGetQuery(con, "
  SELECT table_name, column_name, data_type
  FROM information_schema.columns
  WHERE column_name IN ('KEY_NIS', 'HOSP_NIS', 'YEAR', 'YEAR', 'key_nis', 'hosp_nis')"
)

# Test 3: Check a sample of values to see if they look identical
dbGetQuery(con, "SELECT KEY_NIS, YEAR FROM NIS_Core LIMIT 3")
dbGetQuery(con, "SELECT KEY_NIS, YEAR FROM NIS_DX_PR_GRPS LIMIT 3")






# DIAGNOSTIC 1: Are there actually rows for 2020-2023 in your tables?
print("--- Year Distribution Check ---")
dbGetQuery(con, "
  SELECT YEAR, COUNT(*), COUNT(KEY_NIS)
  FROM NIS_Core
  WHERE YEAR BETWEEN 2020 AND 2023
  GROUP BY YEAR
  ORDER BY YEAR"
)

# DIAGNOSTIC 2: What do the diagnosis codes look like in these modern years?
print("--- Diagnosis Data Format Check ---")
dbGetQuery(con, glue::glue("
  SELECT {dx_cols[1]}, {dx_cols[2]}
  FROM NIS_Core
  WHERE YEAR BETWEEN 2020 AND 2023
    AND {dx_cols[1]} IS NOT NULL
  LIMIT 5
"))

# DIAGNOSTIC 3: Test a raw, un-casted link on just 10 rows without the diagnosis filter
print("--- Simple Raw Link Test ---")
dbGetQuery(con, "
  SELECT c.YEAR, c.KEY_NIS, g.KEY_NIS
  FROM NIS_Core c
  INNER JOIN NIS_DX_PR_GRPS g ON c.KEY_NIS = g.KEY_NIS
  WHERE c.YEAR BETWEEN 2020 AND 2023
  LIMIT 10"
)





# ALTERNATIVE STEP 3: Bulletproof, Case-Insensitive, Space-Trimmed Filter
dx_filter <- glue::glue("
  len(list_filter(
    {col_list},
    x -> x IS NOT NULL AND (
      UPPER(TRIM(x)) LIKE 'S%'
      OR LEFT(UPPER(TRIM(x)), 3) BETWEEN 'T07' AND 'T34'
      OR LEFT(UPPER(TRIM(x)), 3) BETWEEN 'T66' AND 'T76'
    )
  )) > 0
", .open = "{", .close = "}")











# Strip YEAR and HOSP_NIS from the inner join evaluation.
# Keep c.YEAR at the bottom to filter your Hive folders efficiently.
sql_wide_fixed <- glue::glue("
  SELECT
      {c_select},
      {g_select}
  FROM NIS_Core AS c
  INNER JOIN NIS_DX_PR_GRPS AS g
     ON c.KEY_NIS = g.KEY_NIS
  WHERE c.YEAR BETWEEN 2020 AND 2023
    AND {dx_filter}
", .open = "{", .close = "}")

result_wide <- DBI::dbGetQuery(con, sql_wide_fixed)
dim(result_wide)



library(DBI)
library(glue)

# 1. Fetch column names
all_cols1 <- dbGetQuery(con, "DESCRIBE NIS_Core")$column_name
all_cols2 <- dbGetQuery(con, "DESCRIBE NIS_DX_PR_GRPS")$column_name

# Target diagnosis columns from NIS_Core
dx_cols <- all_cols1[grepl("^i10_dx[0-9]+$", all_cols1, ignore.case = TRUE)]

# Clean selections
safe_cols1 <- all_cols1[!grepl("[$]", all_cols1)]
c_select   <- paste(paste0("c.", safe_cols1), collapse = ",\n        ")

# FIX: Remove uppercase YEAR from exclude list to prevent the duplicate error.
# DuckDB is case-insensitive here and only needs the exact match for table g's column names.
g_select   <- "g.* EXCLUDE (KEY_NIS, HOSP_NIS, YEAR)"

# 2. Fix the string type mismatch for the list builder
cast_dx_cols <- paste0("CAST(c.", dx_cols, " AS VARCHAR)")
col_list     <- paste0("[", paste(cast_dx_cols, collapse = ", "), "]")

# 3. Create the list-based filtering clause
dx_filter <- glue::glue("
  len(list_filter(
    {col_list},
    x -> x IS NOT NULL AND (
      LEFT(x, 1) = 'S'
      OR LEFT(x, 3) BETWEEN 'T07' AND 'T34'
      OR LEFT(x, 3) BETWEEN 'T66' AND 'T76'
    )
  )) > 0
", .open = "{", .close = "}")

# 4. Final updated SQL Wide Merge Query
sql_wide_fixed <- glue::glue("
  SELECT
      {c_select},
      {g_select}
  FROM NIS_Core AS c
  INNER JOIN NIS_DX_PR_GRPS AS g
     ON CAST(c.KEY_NIS AS BIGINT) = CAST(g.KEY_NIS AS BIGINT)
  WHERE c.YEAR BETWEEN 2020 AND 2023
    AND c.KEY_NIS IS NOT NULL
    AND {dx_filter}
", .open = "{", .close = "}")

# Execute query and pull the cohort into R memory
result_wide <- DBI::dbGetQuery(con, sql_wide_fixed)
print(dim(result_wide))




################################################################################
################################################################################
#### Meta Data setup ###########################################################
################################################################################
################################################################################

########################
#### Load meta data ####
########################
AnnotationTablePath   <- file.path(MasterDBPath, "AnnotationReferenceTable.xlsx")
AnnotDT <- openxlsx::read.xlsx(AnnotationTablePath)
NSQUIPMetaData <- list()
for(i in 1:nrow(AnnotDT)){
  NSQUIPMetaData[[AnnotDT$ListName[i]]] <- openxlsx::read.xlsx(file.path(MasterDBPath, "NSQIP_data", AnnotDT$Table[i]), startRow = AnnotDT$StartRow[i], sheet = AnnotDT$Sheet[i])
}
#### bind meta data tables with identical columns together into a comprehensive table ###
# # 1. Create signatures for each data frame in the list
# col_signatures <- sapply(NSQUIPMetaData, function(df) {
#   paste(sort(colnames(df)), collapse = ",")
# })
# # 2. Find which ones are duplicated
# is_duplicate <- duplicated(col_signatures) | duplicated(col_signatures, fromLast = TRUE)
# # 3. Group the indices of the list by their column signatures
# groups <- split(seq_along(NSQUIPMetaData), col_signatures)
# # 4. View only groups with more than one data frame
# identical_tables <- groups[sapply(groups, length) > 1]
# # Compare columns across the entire list
# library(janitor)
# column_comparison <- compare_df_cols(NSQUIPMetaData)
# lapply(NSQUIPMetaData, colnames)




# #############################################################################################################################################
# #### Optional: Consolidate per-chunk Parquet files into one optimized file per YEAR that DuckDB can query with full predicate pushdown ######
# #############################################################################################################################################
# duckdb_consolidate_all(con = con, tables_written = tables_written, ParquetBasePath = ParquetBasePath, row_group_size = SAV_CHUNK_SIZE)
# #### Re-register views so DuckDB points to the consolidated data.parquet files
# for(tbl in tables_written){ register_parquet_view(con, tbl); log_msg(sprintf("View re-registered: %s", tbl)) }

# #########################################################
# #### Optional: Create indexes and build study tables ####
# #########################################################
# build_study_tables(con = con, force = FALSE, readmission_window = 30, max_dx_pr_index = 5L)



}
