test_that("post-write verification reads Parquet metadata and rejects missing output", {
  fx <- new_repo_fixture()
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  dir <- file.path(fx$pq, "REG_Core", "year=2020")
  dir.create(dir, recursive = TRUE)
  path <- file.path(dir, "source_00001.parquet")
  arrow::write_parquet(arrow::Table$create(data.frame(ID = 1:3)), path)
  update_parquet_manifest(
    ManifestPath = fx$mf, Database = "REG", TableName = "Core", DuckDBTable = "REG_Core",
    Year = 2020, SourcePath = "source.csv", ParquetPath = path, NRows = 3,
    PartitionKey = "YEAR", PartitionValue = "2020", Status = "written"
  )

  verified <- .verify_source_parquet_output(
    ManifestPath = fx$mf, Database = "REG", TableName = "Core", SourcePath = "source.csv",
    expected_rows = 3, expected_directories = dir
  )
  expect_identical(verified$n_rows, 3)
  rm(verified)
  gc()
  expect_identical(unlink(path), 0L)
  expect_error(
    .verify_source_parquet_output(
      ManifestPath = fx$mf, Database = "REG", TableName = "Core", SourcePath = "source.csv",
      expected_rows = 3, expected_directories = dir
    ),
    "expected Parquet file is missing"
  )
})

test_that("the loader does not checkpoint a source whose output vanishes after write", {
  fx <- new_repo_fixture()
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  data.table::fwrite(data.table::data.table(ID = 1:3, VALUE = 4:6), file.path(fx$src, "vanishing.csv"))
  MDT <- data.frame(Database = "REG", MDBDir = "REG", Path = "vanishing.csv",
                    TableName = "Core", FileType = "csv", PartitionKey = "year",
                    PartitionValue = "2020", stringsAsFactors = FALSE)
  env <- environment(generic_db_loader)
  original <- get("write_year_parquet", envir = env)
  assign("write_year_parquet", function(...) {
    path <- original(...)
    gc()
    expect_identical(unlink(path), 0L)
    invisible(path)
  }, envir = env)
  on.exit(assign("write_year_parquet", original, envir = env), add = TRUE)

  result <- run_loader(fx, MDT, "REG", PartitionBy = "NRows")
  expect_length(result$checkpoint, 0L)
  expect_true(any(grepl("[SOURCE START]", result$output, fixed = TRUE)))
  expect_true(any(grepl("[SOURCE READ]", result$output, fixed = TRUE)))
  expect_true(any(grepl("Post-write output verification failed", result$output)))
  expect_length(list.files(fx$pq, pattern = "\\.parquet$", recursive = TRUE), 0L)
  expect_identical(nrow(read_parquet_manifest(fx$mf)), 0L)
})

test_that("FAIL-mode CSV fallback stays in an isolated worker and retains SAV chunk controls", {
  fx <- new_repo_fixture()
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  data.table::fwrite(data.table::data.table(ID = 1:50, VALUE = sprintf("value_%d", 1:50)),
                     file.path(fx$src, "stream.csv"))
  MDT <- data.frame(Database = "REG", MDBDir = "REG", Path = "stream.csv",
                    TableName = "Stream", FileType = "csv", PartitionKey = "year",
                    PartitionValue = "2020",
                    ReaderOptions = jsonlite::toJSON(list(
                      MalformedRowPolicy = "append_previous",
                      ContinuationColumn = "VALUE"), auto_unbox = TRUE),
                    stringsAsFactors = FALSE)
  result <- read_fn(
    path = "stream.csv", year_dir = file.path(fx$pq, "REG_Stream", "year=2020"),
    table_dir = file.path(fx$pq, "REG_Stream"), all_cols = c("ID", "VALUE"),
    col_classes = c(ID = "integer", VALUE = "character"), year_val = 2020,
    MDTSelect = MDT, MasterDBPath = fx$root, reader = "csv", PartitionBy = "FAIL",
    SAV_ROW_THRESHOLD = 10L, RAMThreshold = 1, SAV_CHUNK_SIZE = 1000000L,
    FailProbeMode = "subprocess",
    FailProbeSourcePath = file.path(repoquet_root, "R", "repoquet.R"),
    chunk_size_decrement = 250000L, min_chunk_size = 250000L,
    ManifestPath = fx$mf, Database = "REG", TableName = "Stream",
    DuckDBTable = "REG_Stream", SourcePath = "stream.csv"
  )
  expect_true(isTRUE(result$written))
  expect_equal(result$n_rows, 50)
  expect_length(list.files(fx$pq, pattern = "\\.parquet$", recursive = TRUE), 1L)
})

test_that("source reset removes only the requested source and partition reset delegates safely", {
  fx <- new_repo_fixture()
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  MDT <- data.frame(
    Database = c("REG", "REG"), MDBDir = c("REG", "REG"),
    Path = c("source_2019.csv", "source_2020.csv"), TableName = c("Core", "Core"),
    FileType = c("csv", "csv"), PartitionKey = c("year", "year"),
    PartitionValue = c("2019", "2020"), stringsAsFactors = FALSE
  )
  paths <- vapply(seq_len(nrow(MDT)), function(i) {
    dir <- file.path(fx$pq, "REG_Core", paste0("year=", MDT$PartitionValue[i]))
    dir.create(dir, recursive = TRUE)
    path <- file.path(dir, paste0("source_", MDT$PartitionValue[i], ".parquet"))
    arrow::write_parquet(arrow::Table$create(data.frame(ID = i)), path)
    update_parquet_manifest(
      ManifestPath = fx$mf, Database = "REG", TableName = "Core", DuckDBTable = "REG_Core",
      Year = as.integer(MDT$PartitionValue[i]), SourcePath = MDT$Path[i], ParquetPath = path,
      NRows = 1, PartitionKey = "YEAR", PartitionValue = MDT$PartitionValue[i], Status = "written"
    )
    path
  }, character(1))
  save_checkpoint(repository_checkpoint_key(MDT), fx$cp)

  result <- reset_partition_for_reload(
    MDT = MDT, Database = "REG", TableName = "Core", PartitionKey = "year", PartitionValue = "2020",
    ParquetBasePath = fx$pq, CheckpointPath = fx$cp, ManifestPath = fx$mf, DryRun = FALSE
  )
  expect_identical(result$n_sources, 1L)
  expect_true(file.exists(paths[1]))
  expect_false(file.exists(paths[2]))
  expect_identical(load_checkpoint(fx$cp), repository_checkpoint_key(MDT[1, , drop = FALSE]))
  manifest <- read_parquet_manifest(fx$mf)
  expect_identical(as.character(manifest$SourcePath), "source_2019.csv")
})
