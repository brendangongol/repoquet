test_that("the scaffold creates a valid, parseable project", {
  dir <- tempfile("scaffold_"); on.exit(unlink(dir, recursive = TRUE))
  out <- utils::capture.output(paths <- create_repository_project(dir, profile = "generic"))
  expect_true(file.exists(paths$ConfigPath))
  expect_true(file.exists(paths$MDTPath))
  expect_true(file.exists(paths$RunnerPath))
  expect_true(file.exists(paths$SchemaRegistryPath))
  #### The generated runner must at least be valid R.                       ####
  expect_silent(parse(paths$RunnerPath))
  cfg <- load_repository_config(paths$ConfigPath)
  expect_identical(cfg$MasterDBPath, gsub("\\\\", "/", paths$MasterDBPath))
  mdt <- openxlsx::read.xlsx(paths$MDTPath, sheet = "Sheet1")
  expect_true(all(c("Database", "MDBDir", "Path", "TableName", "FileType",
                    "PartitionKey", "PartitionValue") %in% names(mdt)))
  #### Scaffold refuses to clobber without overwrite.                       ####
  expect_error(create_repository_project(dir), "already exists")
  #### Generic profile: no HCUP patterns in the registry.                   ####
  reg <- load_schema_registry(paths$SchemaRegistryPath, create_if_missing = FALSE)
  expect_false(any(grepl("HOSP_NIS|DISCWT", reg$ColumnPattern)))
})

test_that("the synthetic example repository runs the full pipeline end-to-end", {
  skip_if_not_installed("duckdb")
  dir <- tempfile("example_"); on.exit(unlink(dir, recursive = TRUE))
  out <- utils::capture.output(paths <- generate_example_repository(dir))
  cfg <- load_repository_config(paths$ConfigPath)
  MDT <- openxlsx::read.xlsx(cfg$MDTPath, sheet = "Sheet1")
  expect_identical(sort(unique(MDT$Database)), c("SALES", "SENSORS", "STUDY"))

  out <- utils::capture.output(iss <- ValidateMDTPreflight(MDT, strict = FALSE,
      ParquetBasePath = paths$ParquetBasePath))
  expect_false(any(iss$Severity == "error"))
  out <- utils::capture.output(PrepareSchemaRegistry(
      MDT, MasterDBPath = cfg$MasterDBPath,
      ObservationPath = paths$SchemaObservationPath,
      SchemaReviewPath = paths$SchemaReviewPath, n_workers = 1,
      SchemaRegistryPath = paths$SchemaRegistryPath))
  review_sheets <- openxlsx::getSheetNames(paths$SchemaReviewPath)
  review_wb <- stats::setNames(lapply(review_sheets, function(sheet) {
    openxlsx::read.xlsx(paths$SchemaReviewPath, sheet = sheet)
  }), review_sheets)
  if ("Decision" %in% names(review_wb$ColumnDecisions)) review_wb$ColumnDecisions$Decision <- "Accept"
  if ("Decision" %in% names(review_wb$CompatibilityDecisions)) review_wb$CompatibilityDecisions$Decision <- "Accept"
  openxlsx::write.xlsx(review_wb, paths$SchemaReviewPath, overwrite = TRUE)
  out <- utils::capture.output(FinalizeSchemaRegistry(
      paths$SchemaReviewPath, paths$TableSchemaPath, strict = TRUE))
  out <- utils::capture.output(completed <- ParquetBackEndCreate(MDT = MDT,
      DBLoad = sort(unique(MDT$Database)), MasterDBPath = cfg$MasterDBPath,
      completed_checkpoint = load_checkpoint(paths$CheckpointPath),
      CheckpointPath = paths$CheckpointPath, ParquetBasePath = paths$ParquetBasePath,
      LogPath = paths$LogPath, n_workers = 1, PartitionBy = "NRows",
      RAMThreshold = 30, SAV_ROW_THRESHOLD = 1000000L, SAV_CHUNK_SIZE = 1000000L,
      SchemaRegistryPath = paths$SchemaRegistryPath, TableSchemaPath = paths$TableSchemaPath,
      ManifestPath = paths$ManifestPath, RunPreflight = FALSE))
  expect_length(completed, nrow(MDT))

  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  done <- MDT[checkpoint_completed_mask(MDT, load_checkpoint(paths$CheckpointPath)), ]
  out <- utils::capture.output(register_parquet_view_compile(con,
      ParquetBasePath = paths$ParquetBasePath,
      tables_written = unique(paste(done$Database, done$TableName, sep = "_")),
      SchemaRegistryPath = paths$SchemaRegistryPath, TableSchemaPath = paths$TableSchemaPath))

  q1 <- DBI::dbGetQuery(con, "SELECT YEAR, COUNT(*) n FROM SALES_Orders GROUP BY YEAR ORDER BY YEAR")
  expect_identical(nrow(q1), 3L)
  expect_true(all(q1$n == 40))
  # hive columns carry the directory's case ("site"); SQL matching is case-insensitive
  q2 <- DBI::dbGetQuery(con, "SELECT SITE AS site_name, COUNT(*) n FROM SENSORS_Readings GROUP BY SITE ORDER BY SITE")
  expect_identical(q2$site_name, c("Alpha", "Beta"))
  # generic identifier registry: ORDER_ID / SENSOR_ID resolve to VARCHAR
  d <- DBI::dbGetQuery(con, "DESCRIBE SALES_Orders")
  expect_identical(d$column_type[d$column_name == "ORDER_ID"], "VARCHAR")

  # generic weighted estimator over the synthetic weight column
  wm <- weighted_mean(con, "SALES_Orders", value_col = "AMOUNT", weight_col = "WEIGHT", by = "YEAR")
  expect_identical(nrow(wm), 3L)
  expect_true(all(is.finite(wm$mean_weighted)))
  expect_error(weighted_mean(con, "SALES_Orders", value_col = "AMOUNT"), "weight_col is required")

  # data dictionary harvested from the Stata file
  hits <- search_labels("Randomization", TableSchemaPath = paths$TableSchemaPath)
  expect_identical(hits$Column, "ARM")
  expect_match(hits$ValueLabels, "1 = Treatment; 2 = Control")

  # the repository reconciles clean
  a <- audit_repository(MDT, paths$ParquetBasePath, paths$CheckpointPath,
                        paths$ManifestPath, con = con, verbose = FALSE)
  expect_identical(nrow(a$issues), 0L)
})
