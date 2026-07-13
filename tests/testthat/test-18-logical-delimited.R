continuation_reader_options <- function(column = "DESC", join = " ") {
  list(MalformedRowPolicy = "append_previous",
       ContinuationColumn = column,
       ContinuationJoin = join)
}

test_that("strict delimited reads reject short records without modifying the source", {
  path <- tempfile(fileext = ".csv")
  on.exit(unlink(path), add = TRUE)
  writeLines(c("DESC,CODE,N", "first,A,1", "continued words", "second,B,2"),
             path, useBytes = TRUE)
  before <- readBin(path, "raw", n = file.info(path)$size)
  before_mtime <- file.info(path)$mtime

  expect_error(read_delimited_full(path), "Delimited structure error")
  expect_identical(readBin(path, "raw", n = file.info(path)$size), before)
  expect_identical(file.info(path)$mtime, before_mtime)
})

test_that("schema survey and full loading share continuation repair", {
  skip_if_not_installed("arrow")
  root <- tempfile("logical_reader_")
  dir.create(file.path(root, "REG"), recursive = TRUE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  path <- file.path(root, "REG", "descriptions.csv")
  writeLines(c("DESC,CODE,N", "first,A,1", "continued words",
               "second,B,2", "tail text"), path, useBytes = TRUE)
  before <- digest::digest(file = path, algo = "sha256")
  options <- continuation_reader_options()

  full <- read_delimited_full(path, reader_options = options)
  expect_identical(full$DESC, c("first continued words", "second tail text"))
  diagnostics <- attr(full, "repoquet_delimited_diagnostics")
  expect_identical(diagnostics$RepairCount, 2)
  expect_identical(diagnostics$RepairLines, c(3L, 5L))

  MDT <- data.frame(
    Database = "REG", MDBDir = "REG", Path = "descriptions.csv",
    TableName = "Descriptions", FileType = "csv",
    PartitionKey = "year", PartitionValue = "2024",
    ReaderOptions = jsonlite::toJSON(options, auto_unbox = TRUE)
  )
  observation_path <- file.path(root, "observations.parquet")
  survey <- SurveyRepositorySchema(MDT, root, observation_path, n_workers = 1,
                                   SourceFingerprintMode = "none", StrictReaders = TRUE)
  rows <- survey$observations[ObservationKind == "source_column"]
  expect_true(all(rows$SurveyStatus == "ok"))
  expect_true(all(rows$ReaderWarningClass == "continuation_repaired"))
  expect_true(all(rows$ReaderWarningSeverity == "info"))
  expect_true(all(rows$ReaderRepairCount == 2))
  expect_identical(digest::digest(file = path, algo = "sha256"), before)
  expect_identical(sort(list.files(dirname(path))), "descriptions.csv")
})

test_that("large repaired delimited sources stream directly into Parquet chunks", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  source <- file.path(fx$src, "continued_2019.csv")
  lines <- c("ID,DESC,CODE", unlist(lapply(1:25, function(i) {
    row <- sprintf("%d,text %02d,C%02d", i, i, i)
    if (i %in% c(1L, 11L, 21L)) c(row, sprintf("continued %02d", i)) else row
  }), use.names = FALSE))
  writeLines(lines, source, useBytes = TRUE)
  before <- digest::digest(file = source, algo = "sha256")
  options <- continuation_reader_options("DESC")
  M <- data.frame(
    Database = "REG", MDBDir = "REG", TableName = "Text",
    Path = "continued_2019.csv", FileType = "csv",
    PartitionKey = "year", PartitionValue = "2019",
    ReaderOptions = jsonlite::toJSON(options, auto_unbox = TRUE)
  )

  result <- run_loader(fx, M, "REG")
  expect_length(result$checkpoint, 1L)
  files <- list.files(file.path(fx$pq, "REG_Text", "year=2019"),
                      pattern = "parquet$", full.names = TRUE)
  expect_length(files, 3L)
  stored <- data.table::rbindlist(lapply(files, function(path) {
    data.table::as.data.table(arrow::read_parquet(path))
  }))
  data.table::setorder(stored, ID)
  expected <- sprintf("text %02d", 1:25)
  expected[c(1L, 11L, 21L)] <- paste0(expected[c(1L, 11L, 21L)], " continued ",
                                      sprintf("%02d", c(1L, 11L, 21L)))
  expect_identical(stored$DESC, expected)
  expect_identical(digest::digest(file = source, algo = "sha256"), before)
  expect_identical(sort(list.files(fx$src)), "continued_2019.csv")
})

test_that("quoted multiline records and repaired continuations share bounded streaming", {
  path <- tempfile(fileext = ".csv")
  on.exit(unlink(path), add = TRUE)
  writeLines(c("ID,DESC,CODE", "1,\"quoted", "multiline\",A",
               "2,plain,B", "unquoted continuation", "3,last,C"),
             path, useBytes = TRUE)
  chunks <- list()
  diagnostics <- get(".stream_delimited_logical_records",
                     envir = environment(PrepareSchemaRegistry))(
    path, reader_options = continuation_reader_options("DESC"), chunk_size = 1L,
    callback = function(df) chunks[[length(chunks) + 1L]] <<- df)
  out <- data.table::rbindlist(chunks)

  expect_length(chunks, 3L)
  expect_identical(out$DESC,
                   c("quoted\nmultiline", "plain unquoted continuation", "last"))
  expect_identical(diagnostics$LogicalRows, 3)
  expect_identical(diagnostics$RepairCount, 1)
})
