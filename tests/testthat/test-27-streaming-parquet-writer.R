test_that("streaming writer appends bounded delimited batches into SAV_CHUNK_SIZE output files", {
  root <- tempfile("streaming_writer_")
  dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source_path <- file.path(root, "wide.csv")
  n <- 2001L
  data.table::fwrite(data.table::data.table(
    YEAR = rep(2020L, n),
    VALUE = seq_len(n),
    TEXT = rep(paste(rep("x", 2000L), collapse = ""), n)), source_path)
  output_dir <- file.path(root, "year=2020")

  result <- read_delimited_chunked(
    path = source_path, chunk_size = 1500L, year_dir = output_dir,
    all_cols = c("VALUE", "TEXT"),
    col_classes = list(VALUE = "numeric", TEXT = "character"),
    year_val = "2020", partition_keys = "YEAR", partition_values = "2020",
    reader = "csv", MaxChunkMemoryMB = 1L, DelimitedWriteMode = "streaming")

  files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
  rows <- vapply(files, function(file) nrow(arrow::read_parquet(file)), integer(1))
  values <- data.table::rbindlist(lapply(files, function(file) {
    data.table::as.data.table(arrow::read_parquet(file))
  }))$VALUE
  expect_identical(unname(rows), c(1500L, 501L))
  expect_identical(values, as.numeric(seq_len(n)))
  expect_equal(result$n_rows, n)
})

test_that("streaming writer keeps source-defined partitions separate", {
  root <- tempfile("streaming_partitions_")
  dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source_path <- file.path(root, "multi_year.csv")
  data.table::fwrite(data.table::data.table(YEAR = c(rep(2020L, 7L), rep(2021L, 6L)),
                                            VALUE = seq_len(13L)), source_path)
  table_dir <- file.path(root, "table")

  result <- read_delimited_chunked(
    path = source_path, chunk_size = 5L, year_dir = file.path(table_dir, "year=2020"),
    table_dir = table_dir, all_cols = "VALUE", col_classes = list(VALUE = "numeric"),
    year_val = "2020", partition_keys = "YEAR", partition_values = "2020",
    route_source_partitions = TRUE, reader = "csv", DelimitedWriteMode = "streaming")

  rows_2020 <- sum(vapply(list.files(file.path(table_dir, "year=2020"), pattern = "\\.parquet$",
                                      full.names = TRUE), function(file) nrow(arrow::read_parquet(file)), integer(1)))
  rows_2021 <- sum(vapply(list.files(file.path(table_dir, "year=2021"), pattern = "\\.parquet$",
                                      full.names = TRUE), function(file) nrow(arrow::read_parquet(file)), integer(1)))
  expect_identical(rows_2020, 7L)
  expect_identical(rows_2021, 6L)
  expect_equal(result$n_rows, 13)
})

test_that("streaming writer restarts an interrupted output piece without duplicate rows", {
  root <- tempfile("streaming_restart_")
  dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source_path <- file.path(root, "source.csv")
  output_dir <- file.path(root, "final", "year=2020")
  data.table::fwrite(data.table::data.table(VALUE = seq_len(10L)), source_path)

  original_open <- repoquet:::.open_parquet_stream_writer
  original_options <- options(repoquet.io_retry_attempts = 2L,
                              repoquet.io_retry_pause_seconds = 0)
  on.exit(options(original_options), add = TRUE)
  write_calls <- 0L
  local_mocked_bindings(
    .open_parquet_stream_writer = function(...) {
      state <- original_open(...)
      writer <- state$writer
      proxy <- new.env(parent = emptyenv())
      proxy$WriteTable <- function(...) {
        write_calls <<- write_calls + 1L
        if (write_calls == 1L) stop("IOError: simulated temporary network write failure")
        writer$WriteTable(...)
      }
      proxy$Close <- function(...) writer$Close(...)
      state$writer <- proxy
      state
    },
    .package = "repoquet")

  result <- read_delimited_chunked(
    path = source_path, chunk_size = 10L, year_dir = output_dir,
    all_cols = "VALUE", col_classes = list(VALUE = "numeric"),
    year_val = "2020", partition_keys = "YEAR", partition_values = "2020",
    reader = "csv", DelimitedWriteMode = "streaming",
    StreamingWriteRecovery = "restart_piece")

  output_files <- list.files(output_dir, pattern = "\\.parquet$", full.names = TRUE)
  expect_length(output_files, 1L)
  expect_identical(arrow::read_parquet(output_files)$VALUE, as.numeric(seq_len(10L)))
  expect_equal(result$n_rows, 10L)
  expect_equal(write_calls, 2L)
})

test_that("streaming writer restarts a routed source after an interrupted partition write", {
  root <- tempfile("streaming_routed_restart_")
  dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source_path <- file.path(root, "multi_year.csv")
  table_dir <- file.path(root, "final")
  data.table::fwrite(data.table::data.table(
    YEAR = c(rep(2020L, 5L), rep(2021L, 5L)), VALUE = seq_len(10L)), source_path)

  original_open <- repoquet:::.open_parquet_stream_writer
  original_options <- options(repoquet.io_retry_attempts = 2L,
                              repoquet.io_retry_pause_seconds = 0)
  on.exit(options(original_options), add = TRUE)
  write_calls <- 0L
  local_mocked_bindings(
    .open_parquet_stream_writer = function(...) {
      state <- original_open(...)
      writer <- state$writer
      proxy <- new.env(parent = emptyenv())
      proxy$WriteTable <- function(...) {
        write_calls <<- write_calls + 1L
        if (write_calls == 2L) stop("IOError: simulated temporary network write failure")
        writer$WriteTable(...)
      }
      proxy$Close <- function(...) writer$Close(...)
      state$writer <- proxy
      state
    },
    .package = "repoquet")

  result <- read_delimited_chunked(
    path = source_path, chunk_size = 5L, year_dir = file.path(table_dir, "year=2020"),
    table_dir = table_dir, all_cols = "VALUE", col_classes = list(VALUE = "numeric"),
    year_val = "2020", partition_keys = "YEAR", partition_values = "2020",
    route_source_partitions = TRUE, reader = "csv", DelimitedWriteMode = "streaming",
    StreamingWriteRecovery = "restart_piece")

  output_files <- list.files(table_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
  rows <- data.table::rbindlist(lapply(output_files, function(file) {
    data.table::as.data.table(arrow::read_parquet(file))
  }))
  expect_equal(result$n_rows, 10L)
  expect_identical(sort(rows$VALUE), as.numeric(seq_len(10L)))
  expect_equal(nrow(rows), 10L)
  expect_equal(write_calls, 4L)
})
