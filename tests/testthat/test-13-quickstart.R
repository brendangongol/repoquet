test_that("the scaffold creates a valid, parseable project", {
  dir <- tempfile("scaffold_"); on.exit(unlink(dir, recursive = TRUE))
  out <- utils::capture.output(paths <- create_repository_project(dir, profile = "generic"))
  expect_true(file.exists(paths$ConfigPath))
  expect_true(file.exists(paths$MDTPath))
  expect_true(file.exists(paths$RunnerPath))
  expect_true(file.exists(paths$SchemaRegistryPath))
  expect_identical(paths$ManifestWorkbookPath,
                   file.path(paths$ManifestDir, "RepositoryMetadata.xlsx"))
  #### The generated runner must at least be valid R.                       ####
  expect_silent(parse(paths$RunnerPath))
  runner_lines <- readLines(paths$RunnerPath, warn = FALSE)
  expect_true(any(grepl("RunId <- new_repository_run_id()", runner_lines, fixed = TRUE)))
  expect_true(all(vapply(1:7, function(i) {
    any(grepl(paste0("#### ", i, "."), runner_lines, fixed = TRUE))
  }, logical(1))))
  canonical_calls <- c(
    "RunId <- new_repository_run_id()",
    "ValidateMDTPreflight(",
    "PrepareSchemaRegistry(",
    "repository_catalog <- FinalizeSchemaRegistry(",
    "run_result <- ParquetBackEndCreate(",
    "register_parquet_view_compile(",
    "repo_audit <- audit_repository("
  )
  call_positions <- vapply(canonical_calls, function(call) {
    hit <- grep(call, runner_lines, fixed = TRUE)
    if (length(hit) == 0L) NA_integer_ else hit[1]
  }, integer(1))
  expect_false(anyNA(call_positions))
  expect_true(all(diff(call_positions) > 0L))
  cfg <- load_repository_config(paths$ConfigPath)
  expect_identical(cfg$MasterDBPath, gsub("\\\\", "/", paths$MasterDBPath))
  original_config <- readLines(paths$ConfigPath, warn = FALSE)
  overridden <- load_repository_config(
    paths$ConfigPath,
    n_workers = 1L,
    PartitionBy = "FAIL",
    FormattedDBPath = file.path(dir, "runtime-formatted"),
    overrides = list(n_workers = 2L, RAMThreshold = 12)
  )
  expect_identical(overridden$n_workers, 1L)
  expect_identical(overridden$RAMThreshold, 12)
  expect_identical(overridden$PartitionBy, "FAIL")
  expect_identical(overridden$FormattedDBPath, file.path(dir, "runtime-formatted"))
  expect_identical(readLines(paths$ConfigPath, warn = FALSE), original_config)
  expect_error(load_repository_config(paths$ConfigPath, overrides = list(1L)),
               "named list")
  expect_error(load_repository_config(paths$ConfigPath,
                                      overrides = list(n_wokers = 1L)),
               "Unknown configuration override")
  expect_error(load_repository_config(paths$ConfigPath,
                                      overrides = list(n_workers = NULL)),
               "cannot be NULL")
  mdt <- openxlsx::read.xlsx(paths$MDTPath, sheet = "Sheet1")
  expect_true(all(c("Database", "MDBDir", "Path", "TableName", "FileType",
                    "PartitionKey", "PartitionValue") %in% names(mdt)))
  #### Scaffold refuses to clobber without overwrite.                       ####
  expect_error(create_repository_project(dir), "already exists")
  #### Generic profile: no HCUP patterns in the registry.                   ####
  reg <- load_schema_registry(paths$SchemaRegistryPath, create_if_missing = FALSE)
  expect_identical(nrow(reg), 0L)
  expect_false(any(grepl("HOSP_NIS|DISCWT", reg$ColumnPattern)))
})

test_that("the HCUP reference follows the canonical seven-stage contract", {
  installed_reference <- system.file(
    "examples", "healthcare", "CECORC_loader_reference.R",
    package = "repoquet"
  )
  relative_reference <- file.path("inst", "examples", "healthcare",
                                  "CECORC_loader_reference.R")
  candidates <- c(installed_reference, relative_reference,
                  file.path("..", "..", relative_reference))
  reference_path <- candidates[file.exists(candidates)][1]
  expect_true(file.exists(reference_path))
  expect_silent(parse(reference_path))
  reference_lines <- readLines(reference_path, warn = FALSE)
  expect_true(all(vapply(1:7, function(i) {
    any(grepl(paste0("#### ", i, "."), reference_lines, fixed = TRUE))
  }, logical(1))))

  canonical_calls <- c(
    "RunId <- new_repository_run_id()",
    "ValidateMDTPreflight(",
    "PrepareSchemaRegistry(",
    "repository_catalog <- FinalizeSchemaRegistry(",
    "run_result <- ParquetBackEndCreate(",
    "register_parquet_view_compile(",
    "repo_audit <- audit_repository("
  )
  call_positions <- vapply(canonical_calls, function(call) {
    hit <- grep(call, reference_lines, fixed = TRUE)
    if (length(hit) == 0L) NA_integer_ else hit[1]
  }, integer(1))
  expect_false(anyNA(call_positions))
  expect_true(all(diff(call_positions) > 0L))
})

test_that("configuration defaults fill omitted file settings and paths can be supplied at runtime", {
  config_path <- tempfile(fileext = ".R")
  on.exit(unlink(config_path), add = TRUE)
  writeLines(c(
    "repository_config <- list(",
    "  MasterDBPath = 'source',",
    "  FormattedDBPath = 'formatted'",
    ")"
  ), config_path)

  cfg <- load_repository_config(config_path, MDTPath = "DBSetup.xlsx")
  expect_identical(cfg$SAV_CHUNK_SIZE, 1000000L)
  expect_identical(cfg$PartitionBy, "NRows")
  expect_identical(cfg$MDTPath, "DBSetup.xlsx")
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
  # Label harvesting is optional metadata work and remains an explicit
  # catalog operation; it does not belong to the generic schema survey.
  out <- utils::capture.output(BuildRepositoryCatalog(
      MDT, DBLoad = sort(unique(MDT$Database)), MasterDBPath = cfg$MasterDBPath,
      n_workers = 1, SchemaRegistryPath = paths$SchemaRegistryPath,
      TableSchemaPath = paths$TableSchemaPath, HarvestLabels = TRUE))
  out <- utils::capture.output(completed <- ParquetBackEndCreate(MDT = MDT,
      DBLoad = sort(unique(MDT$Database)), MasterDBPath = cfg$MasterDBPath,
      completed_checkpoint = load_checkpoint(paths$CheckpointPath),
      CheckpointPath = paths$CheckpointPath, ParquetBasePath = paths$ParquetBasePath,
      LogPath = paths$LogPath, n_workers = 1, PartitionBy = "NRows",
      RAMThreshold = 30, SAV_ROW_THRESHOLD = 1000000L, SAV_CHUNK_SIZE = 1000000L,
      SchemaRegistryPath = paths$SchemaRegistryPath, TableSchemaPath = paths$TableSchemaPath,
      ManifestPath = paths$ManifestPath, RunPreflight = FALSE))
  expect_length(completed, nrow(MDT))
  expect_true(file.exists(paths$ManifestWorkbookPath))
  expect_true(all(c("StartHere", "Tables", "Runs", "Issues", "ColumnGuide", "Manifest_001") %in%
                    openxlsx::getSheetNames(paths$ManifestWorkbookPath)))

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

test_that("real-world source profiles are complete and network-free", {
  all_sources <- real_world_source_catalog("all")
  expect_equal(sum(all_sources$Database == "MIMICIII_DEMO"), 26L)
  expect_equal(sum(all_sources$Database == "NHANES"), 12L)
  expect_equal(sum(grepl("^UCI_", all_sources$Database)), 5L)
  expect_equal(sum(all_sources$Database == "CLINVAR"), 2L)
  expect_true(all(c("ArchiveType", "ArchiveMember", "SourceProvider",
                    "CitationURL") %in% names(all_sources)))
  expect_true(any(all_sources$ArchiveMember == "wdbc.data"))
  expect_true(any(grepl("SectionHeader", all_sources$ReaderOptions, fixed = TRUE)))

  root <- tempfile("public_example_")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  paths <- generate_real_world_repository(root, profile = "quick", Download = FALSE)
  expect_true(file.exists(paths$MDTPath))
  workbook_rows <- openxlsx::read.xlsx(paths$MDTPath, sheet = "Sheet1")
  expect_equal(nrow(workbook_rows), nrow(real_world_source_catalog("quick")))
  expect_false(dir.exists(file.path(paths$DownloadCachePath, "_archives")))
})
