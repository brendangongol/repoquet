test_that("the reader registry knows its built-ins and rejects unknown types", {
  expect_true(all(c("csv", "tsv", "txt", "gz", "sav", "dta", "sas7bdat", "xpt", "parquet", "rds")
                  %in% supported_file_types()))
  expect_error(get_file_reader("xlsx"), "No file reader registered")
  b <- data.frame(Database = "R1", MDBDir = "R1", TableName = "T", Path = "a.xlsx",
                  FileType = "xlsx", PartitionKey = "year", PartitionValue = "2019")
  out <- utils::capture.output(iss <- ValidateMDTPreflight(b, strict = FALSE))
  expect_true("bad_filetype" %in% iss$Check)
})

test_that("Stata .dta sources load end-to-end with labels harvested", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  haven::write_dta(data.frame(
    AGE  = haven::labelled(c(30, 40), label = "Age at admission"),
    GRP  = haven::labelled(c(1, 2), labels = c(Treatment = 1, Control = 2), label = "Study arm")),
    file.path(fx$src, "study_2020.dta"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "study_2020.dta",
                  FileType = "dta", PartitionKey = "year", PartitionValue = "2020")
  out <- utils::capture.output(BuildRepositoryCatalog(M, DBLoad = "REG", MasterDBPath = fx$root,
      n_workers = 1, SchemaRegistryPath = fx$reg, TableSchemaPath = fx$ts))
  lab <- load_label_catalog(fx$ts)
  expect_identical(lab[lab$Column == "GRP", ]$VariableLabel, "Study arm")
  expect_match(lab[lab$Column == "GRP", ]$ValueLabels, "1 = Treatment; 2 = Control")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  p <- arrow::read_parquet(list.files(file.path(fx$pq, "REG_T", "year=2020"), full.names = TRUE)[1])
  expect_identical(nrow(p), 2L)
  expect_false(inherits(p$GRP, "haven_labelled"))   # label classes stripped at write
})

test_that("SAS transport (.xpt) sources load end-to-end", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  haven::write_xpt(data.frame(AGE = c(1, 2), CODE = c("A", "B")), file.path(fx$src, "sas_2019.xpt"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "sas_2019.xpt",
                  FileType = "xpt", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  expect_true(dir.exists(file.path(fx$pq, "REG_T", "year=2019")))
})

test_that("tsv and gzipped delimited sources load through the shared reader", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("A", "B")),
                     file.path(fx$src, "t_2019.tsv"), sep = "\t")
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "t_2019.tsv",
                  FileType = "tsv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  skip_if_not_installed("R.utils")   # fread needs R.utils for gz
  data.table::fwrite(data.table::data.table(AGE = c(3, 4), CODE = c("C", "D")),
                     file.path(fx$src, "g_2020.gz"), compress = "gzip")
  M2 <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "g_2020.gz",
                   FileType = "gz", PartitionKey = "year", PartitionValue = "2020")
  r2 <- run_loader(fx, M2, "REG", completed = load_checkpoint(fx$cp))
  expect_length(r2$checkpoint, 2L)
})

test_that("parquet sources can be re-partitioned through the pipeline", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  arrow::write_parquet(data.frame(AGE = c(1, 2), CODE = c("A", "B")), file.path(fx$src, "p_2019.parquet"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "p_2019.parquet",
                  FileType = "parquet", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  p <- arrow::read_parquet(list.files(file.path(fx$pq, "REG_T", "year=2019"), full.names = TRUE)[1])
  expect_identical(nrow(p), 2L)
})

test_that("user-registered custom readers plug into the whole pipeline", {
  fx <- new_repo_fixture()
  registry <- get(".reader_registry", envir = environment(get_file_reader))
  on.exit({ unlink(fx$root, recursive = TRUE); rm("demo2col", envir = registry) })
  saveRDS(data.frame(AGE = c(5, 6), CODE = c("X", "Y")), file.path(fx$src, "c_2019.demo2col"))
  register_file_reader("demo2col",
    read_full   = function(p) data.table::as.data.table(readRDS(p)),
    read_header = function(p) names(readRDS(p)),
    read_sample = function(p) utils::head(readRDS(p), 100L),
    count_rows  = function(p) nrow(readRDS(p)))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "c_2019.demo2col",
                  FileType = "demo2col", PartitionKey = "year", PartitionValue = "2019")
  out <- utils::capture.output(iss <- ValidateMDTPreflight(M, strict = FALSE, MasterDBPath = fx$root))
  expect_false(any(iss$Severity == "error"))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
})

test_that("large delimited files stream in chunks with full bookkeeping", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = 1:25, CODE = sprintf("C%02d", 1:25)),
                     file.path(fx$src, "big_2019.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "big_2019.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")   # SAV_ROW_THRESHOLD=10, SAV_CHUNK_SIZE=10 via fixture defaults
  expect_length(r$checkpoint, 1L)
  ydir <- file.path(fx$pq, "REG_T", "year=2019")
  chunks <- list.files(ydir, pattern = "_0000[0-9]\\.parquet$")
  expect_length(chunks, 3L)   # 10 + 10 + 5
  tot <- sum(sapply(list.files(ydir, full.names = TRUE), function(f) nrow(arrow::read_parquet(f))))
  expect_identical(tot, 25L)
  mf <- data.table::fread(fx$mf)
  expect_identical(nrow(mf[mf$Status == "written" & grepl("^chunk_", mf$Notes), ]), 3L)
  expect_identical(unique(as.character(mf[mf$Status == "written", ]$PartitionValue)), "2019")
  # physical types identical across chunks
  sch <- function(f) {
    s <- arrow::read_parquet(f, as_data_frame = FALSE)$schema
    stats::setNames(sapply(s$fields, function(x) x$type$ToString()), sapply(s$fields, function(x) x$name))
  }
  schemas <- lapply(list.files(ydir, full.names = TRUE), sch)
  expect_true(all(vapply(schemas[-1], identical, logical(1), schemas[[1]])))
  # resume: nothing re-read
  r2 <- run_loader(fx, M, "REG", completed = load_checkpoint(fx$cp))
  expect_true(any(grepl("Skipping 1 already-completed", r2$output)))
})

test_that("chunk composition cannot change values: all-empty chunks match direct reads", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  #### Rows 11-20 (exactly the second chunk at chunk_size = 10) have an     ####
  #### entirely empty NOTE column. Without pinned per-chunk types, fread    ####
  #### infers that chunk's NOTE as logical (empty -> NA) while mixed chunks ####
  #### yield "" -- the stored value would depend on chunk boundaries.       ####
  q <- data.table::data.table(ID = sprintf("Q%02d", 1:25),
                              NOTE = c(rep("text", 10), rep(NA_character_, 10), rep("tail", 5)),
                              VAL = 1:25)
  data.table::fwrite(q, file.path(fx$src, "empties_2019.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "E", Path = "empties_2019.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  expect_false(any(grepl("not found", r$output)))   # positional colClasses matched cleanly
  ydir <- file.path(fx$pq, "REG_E", "year=2019")
  got <- data.table::rbindlist(lapply(list.files(ydir, full.names = TRUE),
                                      function(f) data.table::as.data.table(arrow::read_parquet(f))))
  data.table::setorder(got, ID)
  direct <- data.table::fread(file.path(fx$src, "empties_2019.csv"),
                              na.strings = c("NA", "NULL"), encoding = "UTF-8")
  expect_identical(got$NOTE, direct$NOTE)   # chunked == direct, including the empty block
  expect_identical(unique(got$NOTE[11:20]), unique(direct$NOTE[11:20]))
})

test_that("chunked delimited loads validate partition columns and clean up on failure", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(YEAR = rep(2018L, 25), AGE = 1:25),
                     file.path(fx$src, "bad_2019.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "bad_2019.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 0L)
  expect_true(any(grepl("disagree with the workbook partition value", r$output)))
  ydir <- file.path(fx$pq, "REG_T", "year=2019")
  n_left <- if (dir.exists(ydir)) length(list.files(ydir, pattern = "parquet$")) else 0L
  expect_identical(n_left, 0L)   # partial chunk outputs removed
})
