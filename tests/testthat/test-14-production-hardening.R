test_that("UTF-8 text is not reinterpreted as Latin-1", {
  x <- data.table::data.table(CITY = c("Muenchen", "München", "Zürich"))
  got <- normalize_character_encoding(data.table::copy(x))
  expect_identical(got$CITY, x$CITY)
})

test_that("case-only duplicate columns fail when populated values conflict", {
  x <- data.table::data.table(a = c(1L, 2L), b = c(1L, 9L))
  data.table::setnames(x, c("year", "YEAR"))
  expect_error(canonicalize_dataframe_names(x), "conflicting columns")

  y <- data.table::data.table(a = c(1L, NA_integer_), b = c(NA_integer_, 2L))
  data.table::setnames(y, c("code", "CODE"))
  expect_identical(canonicalize_dataframe_names(y)$CODE, c(1L, 2L))
})

test_that("direct delimited reads preserve leading zeros from the resolved schema", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(CPT = c("00123", "00007"), LABEL = c("München", "Zürich")),
                     file.path(fx$src, "codes.csv"), quote = TRUE)
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Codes", Path = "codes.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2024")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  p <- arrow::read_parquet(list.files(file.path(fx$pq, "REG_Codes", "year=2024"),
                                      pattern = "parquet$", full.names = TRUE)[1])
  expect_identical(p$CPT, c("00123", "00007"))
  expect_identical(p$LABEL, c("München", "Zürich"))
})

test_that("source fingerprints invalidate a completed checkpoint after mutation", {
  skip_if_not_installed("digest")
  root <- tempfile("fingerprint_"); dir.create(file.path(root, "D"), recursive = TRUE)
  on.exit(unlink(root, recursive = TRUE))
  path <- file.path(root, "D", "a.csv")
  writeLines("x\n1", path, useBytes = TRUE)
  M <- data.frame(Database = "D", MDBDir = "D", TableName = "T", Path = "a.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2024")
  old <- repository_checkpoint_key(M, root, "sha256")
  expect_true(checkpoint_completed_mask(M, old, accept_legacy = FALSE,
                                        MasterDBPath = root, SourceFingerprintMode = "sha256"))
  writeLines("x\n2", path, useBytes = TRUE)
  expect_false(checkpoint_completed_mask(M, old, accept_legacy = FALSE,
                                         MasterDBPath = root, SourceFingerprintMode = "sha256"))
})

test_that("ambiguous fallback physical table names are rejected", {
  M <- data.frame(Database = c("A_B", "A"), MDBDir = c("A", "A"),
                  TableName = c("C", "B_C"), Path = c("a.csv", "b.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2023", "2024"))
  out <- utils::capture.output(issues <- ValidateMDTPreflight(M, strict = FALSE, logStatus = FALSE))
  expect_true("ambiguous_physical_table" %in% issues$Check)
  M$PhysicalTableName <- c("AB_C", "A_BC")
  out <- utils::capture.output(issues2 <- ValidateMDTPreflight(M, strict = FALSE, logStatus = FALSE))
  expect_false("ambiguous_physical_table" %in% issues2$Check)
})

test_that("refreshing a table prunes columns removed from its sources", {
  old <- data.table::data.table(Database = "D", TableName = "T", DuckDBTable = "D_T",
                                Column = c("A", "REMOVED"), CanonicalType = "character",
                                Source = c("manual", "manual"))
  fresh <- data.table::data.table(Database = "D", TableName = "T", DuckDBTable = "D_T",
                                  Column = "A", CanonicalType = "integer", Source = "resolved")
  merged <- merge_table_schema_catalog(fresh, old)
  expect_identical(merged$Column, "A")
  expect_identical(merged$CanonicalType, "character")
  expect_identical(merged$Source, "manual")

  old$Source[old$Column == "A"] <- "user_approved"
  approved <- merge_table_schema_catalog(fresh, old)
  expect_identical(approved$CanonicalType, "character")
  expect_identical(approved$Source, "user_approved")
})

test_that("mixed source formats can populate one logical table", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(CPT = "00001", VALUE = 1), file.path(fx$src, "a.csv"))
  saveRDS(data.frame(CPT = "A002", VALUE = 2), file.path(fx$src, "b.rds"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Mixed",
                  Path = c("a.csv", "b.rds"), FileType = c("csv", "rds"),
                  PartitionKey = "year", PartitionValue = c("2023", "2024"))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 2L)
  expect_true(dir.exists(file.path(fx$pq, "REG_Mixed", "year=2023")))
  expect_true(dir.exists(file.path(fx$pq, "REG_Mixed", "year=2024")))
})

test_that("custom chunkable readers use their own read_chunk callback", {
  fx <- new_repo_fixture()
  registry <- get(".reader_registry", envir = environment(get_file_reader))
  on.exit({ unlink(fx$root, recursive = TRUE); rm("chunkr", envir = registry) })
  saveRDS(data.frame(CPT = sprintf("%05d", 1:25), VALUE = 1:25), file.path(fx$src, "a.chunkr"))
  register_file_reader("chunkr",
    read_full = function(p) readRDS(p),
    read_header = function(p) names(readRDS(p)),
    read_sample = function(p) utils::head(readRDS(p), 10),
    count_rows = function(p) nrow(readRDS(p)),
    read_chunk = function(p, offset, n_max, ...) readRDS(p)[seq.int(offset + 1L, min(nrow(readRDS(p)), offset + n_max)), , drop = FALSE],
    chunkable = TRUE)
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Custom", Path = "a.chunkr",
                  FileType = "chunkr", PartitionKey = "year", PartitionValue = "2024")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  expect_length(list.files(file.path(fx$pq, "REG_Custom", "year=2024"), pattern = "parquet$"), 3L)
})

test_that("explicit numeric non-year partitions stay numeric in DuckDB", {
  skip_if_not_installed("duckdb")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(VALUE = 1:2), file.path(fx$src, "b.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Batch", Path = "b.csv",
                  FileType = "csv", PartitionKey = "BATCH", PartitionValue = "7",
                  PartitionType = "integer")
  r <- run_loader(fx, M, "REG")
  con <- DBI::dbConnect(duckdb::duckdb()); on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  catalog <- load_table_schema_catalog(fx$ts, strict = TRUE)$table_schema
  register_parquet_view(con, fx$pq, "REG_Batch", table_schema = catalog, validate = FALSE)
  desc <- DBI::dbGetQuery(con, 'DESCRIBE "REG_Batch"')
  expect_match(desc$column_type[desc$column_name == "BATCH"], "INTEGER")
  expect_identical(DBI::dbGetQuery(con, 'SELECT DISTINCT BATCH FROM "REG_Batch"')$BATCH, 7L)
})

test_that("DuckDB manifests are transactional and retain large row counts", {
  skip_if_not_installed("duckdb")
  path <- tempfile(fileext = ".duckdb"); on.exit(unlink(path))
  update_parquet_manifest(path, "D", "T", "D_T", 2024, "a.csv", "a.parquet",
                          NRows = 3000000000, PartitionKey = "YEAR", PartitionValue = "2024")
  manifest <- read_parquet_manifest(path)
  expect_equal(manifest$NRows, 3000000000)
  expect_true(all(c("RunId", "SourceFingerprint", "ManifestSchemaVersion") %in% names(manifest)))
})

test_that("repository metadata exports a complete paginated Excel snapshot", {
  skip_if_not_installed("duckdb")
  root <- tempfile("metadata_export_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  manifest_path <- file.path(root, "RepositoryMetadata.duckdb")
  workbook_path <- file.path(root, "RepositoryMetadata.xlsx")
  update_parquet_manifest(
    manifest_path, "D", "T", "D_T", 2024, "001.csv", "year=2024/a.parquet",
    NRows = 3000000000, Status = "written", PartitionKey = "YEAR",
    PartitionValue = "2024", RunId = "run_1")
  update_parquet_manifest(
    manifest_path, "D", "T", "D_T", 2025, "002.csv", "year=2025/b.parquet",
    NRows = 10, Status = "written", PartitionKey = "YEAR",
    PartitionValue = "2025", RunId = "run_2")
  update_parquet_manifest(
    manifest_path, "D", "T", "D_T", 2026, "003.csv", "year=2026",
    NRows = 5, Status = "partial_accepted", PartitionKey = "YEAR",
    PartitionValue = "2026", RunId = "run_2")

  exported <- ExportRepositoryMetadata(
    manifest_path, workbook_path, MaxRowsPerSheet = 2L)
  expect_true(file.exists(workbook_path))
  expect_identical(exported$manifest_rows, 3L)
  expect_identical(exported$detail_sheets, 2L)
  sheets <- openxlsx::getSheetNames(workbook_path)
  expect_true(all(c("StartHere", "Tables", "Runs", "Issues", "ColumnGuide",
                    "Manifest_001", "Manifest_002") %in% sheets))
  detail <- data.table::rbindlist(list(
    openxlsx::read.xlsx(workbook_path, sheet = "Manifest_001"),
    openxlsx::read.xlsx(workbook_path, sheet = "Manifest_002")), fill = TRUE)
  expect_identical(nrow(detail), 3L)
  expect_equal(max(detail$NRows, na.rm = TRUE), 3000000000)
  expect_true("001.csv" %in% detail$SourcePath)
  issues <- openxlsx::read.xlsx(workbook_path, sheet = "Issues")
  expect_identical(issues$Status, "partial_accepted")
  start <- openxlsx::read.xlsx(workbook_path, sheet = "StartHere", startRow = 4)
  expect_true(any(grepl("authoritative", start$Value, ignore.case = TRUE)))
})

test_that("data contracts report and stop on content violations", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb()); on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
  DBI::dbWriteTable(con, "T", data.frame(ID = c("A", "A", NA), AGE = c(5, 200, 10)))
  path <- tempfile(fileext = ".csv"); on.exit(unlink(path), add = TRUE)
  data.table::fwrite(data.table::data.table(
    ContractName = c("id_unique", "age_range"), DuckDBTable = "T", Column = c("ID", "AGE"),
    Rule = c("unique", "range"), Value = c(NA, "0;120"), Severity = "error", Enabled = TRUE), path)
  result <- validate_data_contracts(con, path, strict = FALSE, logStatus = FALSE)
  expect_identical(result$Status, c("fail", "fail"))
  expect_error(validate_data_contracts(con, path, strict = TRUE, logStatus = FALSE), "failed for 2 rule")
})

test_that("strict file failure is surfaced after run bookkeeping", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "missing.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2024")
  expect_error(ParquetBackEndCreate(
    MDT = M, DBLoad = "REG", MasterDBPath = fx$root, completed_checkpoint = character(),
    CheckpointPath = fx$cp, ParquetBasePath = fx$pq, PartitionBy = "NRows", RAMThreshold = 1,
    SAV_ROW_THRESHOLD = 10, SAV_CHUNK_SIZE = 10, LogPath = fx$log, n_workers = 1,
    SchemaRegistryPath = fx$reg, TableSchemaPath = fx$ts, ManifestPath = fx$mf,
    StrictPreflight = FALSE, StopOnFileError = TRUE, SourceFingerprintMode = "none",
    LockRepository = FALSE, SnapshotState = FALSE, UseSchemaCatalog = FALSE),
    "failed for 1 source file")
  expect_true(dir.exists(file.path(dirname(fx$mf), "RunSummaries")))
})

test_that("catalog mode cannot silently fall back to fresh inference", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(ID = 1L), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "a.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2024")
  expect_error(ParquetBackEndCreate(
    MDT = M, DBLoad = "REG", MasterDBPath = fx$root, completed_checkpoint = character(),
    CheckpointPath = fx$cp, ParquetBasePath = fx$pq, PartitionBy = "NRows", RAMThreshold = 1,
    SAV_ROW_THRESHOLD = 10, SAV_CHUNK_SIZE = 10, LogPath = fx$log, n_workers = 1,
    SchemaRegistryPath = fx$reg, TableSchemaPath = fx$ts, ManifestPath = fx$mf,
    RunPreflight = FALSE, LockRepository = FALSE, SnapshotState = FALSE,
    UseSchemaCatalog = TRUE), "no finalized table schema catalog")
})

test_that("parallel metadata scans retry worker-only failures serially", {
  runner <- get(".parallel_scan_with_serial_retry",
                envir = environment(build_col_classes))
  main_pid <- Sys.getpid()
  scan_one <- function(i) {
    if (Sys.getpid() != main_pid) {
      return(list(ok = FALSE, value = NULL, error = "worker cannot access source"))
    }
    list(ok = TRUE, value = i, error = NA_character_)
  }

  results <- runner(
    as.list(1:2), scan_one, n_workers = 2,
    future_packages = character(),
    is_failure = function(x) !isTRUE(x$ok),
    context = "test metadata scan"
  )

  expect_true(all(vapply(results, function(x) isTRUE(x$ok), logical(1))))
  expect_equal(vapply(results, `[[`, integer(1), "value"), 1:2)
})

test_that("a genuinely unrecoverable item names its source file in the fallback log", {
  runner <- get(".parallel_scan_with_serial_retry",
                envir = environment(build_col_classes))
  scan_one <- function(item) {
    if (identical(item$path, "bad.csv")) {
      return(list(ok = FALSE, path = item$path, error = "input string 1 is invalid UTF-8"))
    }
    list(ok = TRUE, path = item$path, error = NA_character_)
  }
  items <- list(list(path = "good.csv"), list(path = "bad.csv"))
  out <- utils::capture.output(
    results <- runner(items, scan_one, n_workers = 2, future_packages = character(),
                      is_failure = function(x) !isTRUE(x$ok), context = "test metadata scan"))
  expect_true(any(grepl("bad.csv: input string 1 is invalid UTF-8", out, fixed = TRUE)))
  expect_false(isTRUE(results[[2]]$ok))
})

test_that("ParquetBackEndCreate defaults fread to non-mmap reads without leaking the setting", {
  had_prior <- !is.na(Sys.getenv("R_DATATABLE_NOMMAP", unset = NA))
  prior <- Sys.getenv("R_DATATABLE_NOMMAP", unset = NA)
  if (had_prior) Sys.unsetenv("R_DATATABLE_NOMMAP")
  on.exit({
    if (had_prior) Sys.setenv(R_DATATABLE_NOMMAP = prior) else Sys.unsetenv("R_DATATABLE_NOMMAP")
  }, add = TRUE)

  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T",
                  Path = "a.csv", FileType = "csv", PartitionKey = "year", PartitionValue = "2020")
  run_loader(fx, M, "REG")
  expect_true(is.na(Sys.getenv("R_DATATABLE_NOMMAP", unset = NA)))

  #### An explicit caller preference must not be overridden or cleared.     ####
  Sys.setenv(R_DATATABLE_NOMMAP = "false")
  run_loader(fx, M, "REG", completed = load_checkpoint(fx$cp))
  expect_identical(Sys.getenv("R_DATATABLE_NOMMAP", unset = NA), "false")
  Sys.unsetenv("R_DATATABLE_NOMMAP")
})

test_that("reset_table_for_reload logs to an explicit LogPath even when called standalone", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2)), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T",
                  Path = "a.csv", FileType = "csv", PartitionKey = "year", PartitionValue = "2020")
  run_loader(fx, M, "REG")
  expect_true(dir.exists(file.path(fx$pq, "REG_T", "year=2020")))

  #### Simulate calling reset_table_for_reload() standalone (e.g. a fresh   ####
  #### interactive session after a crash) -- no LogPath in scope anywhere   ####
  #### the way there would be inside a ParquetBackEndCreate() run.          ####
  standalone_log <- file.path(fx$root, "standalone.log")
  expect_false(file.exists(standalone_log))
  out <- utils::capture.output(
    result <- reset_table_for_reload(M, Database = "REG", TableName = "T",
                                     ParquetBasePath = fx$pq, CheckpointPath = fx$cp,
                                     ManifestPath = fx$mf, DryRun = FALSE,
                                     LogPath = standalone_log))
  expect_false(dir.exists(file.path(fx$pq, "REG_T", "year=2020")))
  expect_true(file.exists(standalone_log))
  lines <- readLines(standalone_log)
  expect_true(any(grepl("\\[RESET\\].*removing", lines)))
  expect_true(any(grepl("\\[RESET\\].*cleared", lines)))
  expect_true(any(grepl("\\[LOCK\\] Acquired", lines)))
  expect_true(any(grepl("\\[LOCK\\] Released", lines)))
})
