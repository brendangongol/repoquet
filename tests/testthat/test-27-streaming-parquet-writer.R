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
