#### Checking Parquet files against the catalog, converting what is safe, and setting aside what is not ####

skip_if_not_installed("arrow")
skip_if_not_installed("openxlsx")

write_table_file <- function(root, table, file, data, year = "2024") {
  directory <- file.path(root, "parquet", table, paste0("year=", year))
  dir.create(directory, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(directory, file)
  arrow::write_parquet(data, path)
  path
}

conformance_fixture <- function() {
  root <- tempfile("conformance_"); dir.create(root)
  write_table_file(root, "REG_OK", "OK_00001.parquet", data.frame(ID = c("a", "b"), N = c(1.5, 2.5)))
  #### Two files that need a conversion: an integer to text, and whole doubles to integer.
  write_table_file(root, "REG_CAST", "CAST_00001.parquet", data.frame(ID = 1:3, SCORE = c(1, 2, 3)))
  write_table_file(root, "REG_CAST", "CAST_00002.parquet", data.frame(ID = 4:5, SCORE = c(4, 5)), year = "2025")
  #### A code stored as a number: 812.1 cannot become text without being rewritten as 812.
  write_table_file(root, "REG_RELOAD", "RELOAD_00001.parquet", data.frame(ID = 1:3, ECODE = c(812.1, 5, 7)))
  #### A catalog type this check does not convert.
  write_table_file(root, "REG_REVIEW", "REVIEW_00001.parquet", data.frame(ID = 1:2, PRICE = c(1.25, 2.5)))
  catalog <- data.frame(
    Database = "REG",
    TableName = c("OK", "OK", "CAST", "CAST", "RELOAD", "RELOAD", "REVIEW", "REVIEW", "NONE", "OK"),
    DuckDBTable = c("REG_OK", "REG_OK", "REG_CAST", "REG_CAST", "REG_RELOAD", "REG_RELOAD", "REG_REVIEW", "REG_REVIEW",
                    "REG_NONE", "REG_OK"),
    Column = c("ID", "N", "ID", "SCORE", "ID", "ECODE", "ID", "PRICE", "ID", "YEAR"),
    CanonicalType = c("character", "numeric", "character", "integer", "integer", "character", "integer",
                      "decimal(10,2)", "integer", "integer"),
    Role = c("data", "data", "data", "data", "data", "data", "data", "data", "data", "partition"))
  path <- file.path(root, "TableSchemas.xlsx")
  openxlsx::write.xlsx(list(TableSchemas = catalog), path)
  list(root = root, pq = file.path(root, "parquet"), catalog = path)
}

read_file <- function(path) as.data.frame(arrow::read_parquet(path))

test_that("the check gives each table one action and tests whether a conversion is value-safe", {
  fx <- conformance_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  out <- utils::capture.output(plan <- check_schema_conformance(fx$catalog, fx$pq))
  action <- stats::setNames(plan$tables$Action, plan$tables$DuckDBTable)
  expect_identical(action[["REG_OK"]], "OK")
  expect_identical(action[["REG_CAST"]], "CAST")
  expect_identical(action[["REG_RELOAD"]], "RELOAD")
  expect_identical(action[["REG_REVIEW"]], "REVIEW")
  expect_identical(action[["REG_NONE"]], "NO_PARQUET")
  expect_identical(plan$tables$FilesToChange[plan$tables$DuckDBTable == "REG_CAST"], 2L)
  #### The unsafe value is named, and the partition column is never compared.
  bad <- plan$columns[Action == "RELOAD"]
  expect_identical(bad$Column, "ECODE")
  expect_gt(bad$BadValues, 0)
  expect_match(bad$Samples, "812.1", fixed = TRUE)
  expect_false("YEAR" %in% plan$columns$Column)
  #### Reading changes nothing.
  expect_identical(read_file(file.path(fx$pq, "REG_CAST", "year=2024", "CAST_00001.parquet"))$SCORE, c(1, 2, 3))
  expect_length(list.files(fx$pq, pattern = "schema_rewrite|pre_schema_update", recursive = TRUE), 0L)
  #### Databases narrows the check, and a database the catalog lacks is an error.
  expect_error(check_schema_conformance(fx$catalog, fx$pq, Databases = "NOPE"), "no columns for: NOPE")
  expect_error(check_schema_conformance(file.path(fx$root, "missing.xlsx"), fx$pq), "Schema catalog not found")
})

test_that("conversions rewrite every file safely, and the result is checked", {
  fx <- conformance_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  out <- utils::capture.output(plan <- check_schema_conformance(fx$catalog, fx$pq))
  out <- utils::capture.output(result <- apply_schema_casts(plan, fx$pq))
  expect_identical(nrow(result), 2L)
  expect_true(all(result$Status == "rewritten"))
  expect_equal(sort(result$Rows), c(2, 3))
  expect_true(all(result$Columns == "ID,SCORE" | result$Columns == "SCORE,ID"))
  #### Values survive the conversion: integers became text, whole doubles became integers.
  first <- read_file(file.path(fx$pq, "REG_CAST", "year=2024", "CAST_00001.parquet"))
  expect_identical(first$ID, c("1", "2", "3"))
  expect_identical(first$SCORE, 1:3)
  #### No temporary or backup file is left, and the lock folder is gone.
  expect_length(list.files(fx$pq, pattern = "schema_rewrite|pre_schema_update", recursive = TRUE), 0L)
  expect_false(dir.exists(file.path(fx$root, ".schema_rewrite.lock")))
  #### The table that cannot be converted was not touched, and the check now agrees.
  expect_identical(read_file(file.path(fx$pq, "REG_RELOAD", "year=2024", "RELOAD_00001.parquet"))$ECODE, c(812.1, 5, 7))
  out <- utils::capture.output(after <- check_schema_conformance(fx$catalog, fx$pq))
  expect_identical(after$tables$Action[after$tables$DuckDBTable == "REG_CAST"], "OK")
  expect_identical(after$tables$Action[after$tables$DuckDBTable == "REG_RELOAD"], "RELOAD")
  #### With nothing left to convert there is an empty, correctly typed result.
  out <- utils::capture.output(none <- apply_schema_casts(after, fx$pq))
  expect_identical(nrow(none), 0L)
  expect_true(all(c("Status", "Message") %in% names(none)))
  expect_identical(nrow(none[Status == "FAILED"]), 0L)
})

test_that("a file whose values stopped being convertible is kept, and the failure is reported", {
  fx <- conformance_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  out <- utils::capture.output(plan <- check_schema_conformance(fx$catalog, fx$pq))
  #### After the check, one file is replaced with a fractional value in a column that is to become integer.
  target <- file.path(fx$pq, "REG_CAST", "year=2024", "CAST_00001.parquet")
  arrow::write_parquet(data.frame(ID = 1:3, SCORE = c(1.5, 2, 3)), target)
  out <- utils::capture.output(result <- apply_schema_casts(plan, fx$pq))
  failed <- result[Status == "FAILED"]
  expect_identical(nrow(failed), 1L)
  expect_match(failed$Message, "no longer convertible for: SCORE")
  expect_identical(read_file(target)$SCORE, c(1.5, 2, 3))              # the original is untouched
  expect_identical(sum(result$Status == "rewritten"), 1L)             # the other file still converted
  expect_length(list.files(fx$pq, pattern = "schema_rewrite|pre_schema_update", recursive = TRUE), 0L)
})

test_that("an interrupted rewrite is recovered, and a second rewrite cannot start meanwhile", {
  fx <- conformance_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  file <- file.path(fx$pq, "REG_CAST", "year=2024", "CAST_00001.parquet")
  #### Simulate a crash after the original was set aside and before the new file was installed.
  file.rename(file, paste0(file, ".pre_schema_update"))
  writeLines("partial", paste0(file, ".schema_rewrite_x.parquet"))
  out <- utils::capture.output(plan <- check_schema_conformance(fx$catalog, fx$pq))
  expect_identical(plan$tables$Action[plan$tables$DuckDBTable == "REG_CAST"], "CAST")
  expect_match(plan$tables$ColumnsToChange[plan$tables$DuckDBTable == "REG_CAST"], "interrupted rewrite")
  out <- utils::capture.output(apply_schema_casts(plan, fx$pq))
  expect_true(file.exists(file))
  expect_length(list.files(fx$pq, pattern = "schema_rewrite|pre_schema_update", recursive = TRUE), 0L)
  #### A lock folder from another run stops a second rewrite.
  lock <- file.path(fx$root, ".schema_rewrite.lock"); dir.create(lock)
  out <- utils::capture.output(plan <- check_schema_conformance(fx$catalog, fx$pq))
  expect_error(apply_schema_casts(plan, fx$pq), "A rewrite lock exists")
  expect_true(dir.exists(lock))
})

test_that("tables that cannot be converted are set aside only when confirmed", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(ID = 1:3, ECODE = c(812.1, 5, 7)), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "A", Path = "a.csv", FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2024")
  loaded <- run_loader(fx, M, "REG")
  expect_length(loaded$checkpoint, 1L)
  catalog_path <- file.path(fx$root, "TableSchemas.xlsx")
  openxlsx::write.xlsx(list(TableSchemas = data.frame(
    Database = "REG", TableName = "A", DuckDBTable = "REG_A", Column = c("ID", "ECODE"),
    CanonicalType = c("integer", "character"), Role = "data")), catalog_path)
  out <- utils::capture.output(plan <- check_schema_conformance(catalog_path, fx$pq))
  expect_identical(plan$tables$Action, "RELOAD")
  #### Without confirmation nothing moves.
  expect_error(reset_nonconforming_tables_for_reload(plan, M, fx$root, fx$pq, fx$cp, fx$mf),
               "nothing was changed")
  expect_true(dir.exists(file.path(fx$pq, "REG_A")))
  expect_length(load_checkpoint(fx$cp), 1L)
  #### A missing source stops it before anything moves.
  gone <- M; gone$Path <- "gone.csv"
  expect_error(reset_nonconforming_tables_for_reload(plan, gone, fx$root, fx$pq, fx$cp, fx$mf, Confirm = TRUE),
               "Source file\\(s\\) not found")
  expect_true(dir.exists(file.path(fx$pq, "REG_A")))
  #### Confirmed: the Parquet folder is moved, not deleted, and the checkpoint entry is cleared.
  backup <- file.path(fx$root, "backups")
  out <- utils::capture.output(moved <- reset_nonconforming_tables_for_reload(
    plan, M, fx$root, fx$pq, fx$cp, fx$mf, BackupDir = backup, Confirm = TRUE))
  expect_identical(moved$reset, "REG_A")
  expect_false(dir.exists(file.path(fx$pq, "REG_A")))
  expect_length(list.files(file.path(backup, "REG_A"), pattern = "\\.parquet$", recursive = TRUE), 1L)
  expect_length(load_checkpoint(fx$cp), 0L)
  #### The normal load call rebuilds it.
  reloaded <- run_loader(fx, M, "REG", completed = load_checkpoint(fx$cp))
  expect_length(reloaded$checkpoint, 1L)
  expect_true(dir.exists(file.path(fx$pq, "REG_A")))
  #### With nothing marked RELOAD or NO_PARQUET there is nothing to do.
  out <- utils::capture.output(idle <- reset_nonconforming_tables_for_reload(
    list(tables = plan$tables[0]), M, fx$root, fx$pq, fx$cp, fx$mf, Confirm = TRUE))
  expect_length(idle$reset, 0L)
})
