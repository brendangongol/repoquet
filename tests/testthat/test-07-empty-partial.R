test_that("verified-empty SAV completes via the chunked path with a manifest record", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  esav <- file.path(fx$src, "empty.sav")
  haven::write_sav(data.frame(AGE = numeric(0), SEX = numeric(0)), esav)
  ydir <- file.path(fx$pq, "D_T", "year=2020"); dir.create(ydir, recursive = TRUE)
  out <- utils::capture.output(
    r <- safe_read_sav_chunked(path = esav, chunk_size = 10L, year_dir = ydir,
                               out_path = file.path(ydir, "x.parquet"),
                               all_cols = c("AGE", "SEX"),
                               col_classes = list(AGE = "numeric", SEX = "numeric"),
                               year_val = 2020, ManifestPath = fx$mf,
                               Database = "D", TableName = "T", DuckDBTable = "D_T",
                               SourcePath = "empty.sav"))
  expect_true(isTRUE(r$written))
  expect_identical(r$n_rows, 0L)
  mf <- data.table::fread(fx$mf)
  expect_true(any(mf$Status == "empty" & mf$Notes == "verified_empty_source"))
})

test_that("verified-empty CSV checkpoints in the full loader and is not retried", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "full.csv"))
  writeLines("AGE,CODE", file.path(fx$src, "empty.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T",
                  Path = c("full.csv", "empty.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2020", "2021"))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 2L)
  expect_true(any(grepl("verified empty", r$output)))
  mf <- data.table::fread(fx$mf)
  expect_true(any(mf$SourcePath == "empty.csv" & mf$Status == "empty"))
  r2 <- run_loader(fx, M, "REG", completed = load_checkpoint(fx$cp))
  expect_true(any(grepl("Skipping 2 already-completed", r2$output)))
})

test_that("unverifiable zero-row reads keep failing (missing / wrong-format files)", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = 1), file.path(fx$src, "real.csv"))
  expect_false(verify_source_empty(file.path(fx$src, "missing.csv"), "csv"))
  expect_false(verify_source_empty(file.path(fx$src, "real.csv"), "sav"))
})

test_that("truncation is fatal by default and AcceptPartial checkpoints the readable rows", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  bsav <- file.path(fx$src, "trunc.sav")
  haven::write_sav(data.frame(AGE = rep(1:100, 50), SEX = rep(1:2, 2500)), bsav)  # 5000 rows
  #### Simulate an unreadable tail from row 3000 by stubbing the readers.    ####
  real_read_sav <- haven::read_sav
  fake_read_sav <- function(file, encoding = NULL, user_na = FALSE, col_select = NULL,
                            skip = 0L, n_max = Inf, .name_repair = "unique") {
    if (skip >= 3000) stop("simulated truncation: unreadable past row 3000")
    if (!is.null(col_select)) return(real_read_sav(file, col_select = 1L, skip = skip, n_max = n_max))
    real_read_sav(file, skip = skip, n_max = n_max)
  }
  utils::assignInNamespace("read_sav", fake_read_sav, ns = "haven")
  real_read_spss <- if (requireNamespace("foreign", quietly = TRUE)) foreign::read.spss else NULL
  if (!is.null(real_read_spss)) {
    utils::assignInNamespace("read.spss", function(...) stop("simulated foreign failure"), ns = "foreign")
  }
  on.exit({
    utils::assignInNamespace("read_sav", real_read_sav, ns = "haven")
    if (!is.null(real_read_spss)) utils::assignInNamespace("read.spss", real_read_spss, ns = "foreign")
  }, add = TRUE)

  ydir <- file.path(fx$pq, "D_T", "year=2020"); dir.create(ydir, recursive = TRUE)
  args <- list(path = bsav, chunk_size = 1000L, year_dir = ydir,
               out_path = file.path(ydir, "x.parquet"), all_cols = c("AGE", "SEX"),
               col_classes = list(AGE = "numeric", SEX = "numeric"), year_val = 2020,
               min_chunk_size = 500L, ManifestPath = fx$mf,
               Database = "D", TableName = "T", DuckDBTable = "D_T", SourcePath = "trunc.sav")
  out <- utils::capture.output(err <- tryCatch(do.call(safe_read_sav_chunked, args), error = function(e) e))
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "row_count_mismatch")
  expect_length(list.files(ydir, pattern = "\\.parquet$"), 0L)   # partial outputs cleaned

  out <- utils::capture.output(r <- do.call(safe_read_sav_chunked, c(args, list(accept_partial = TRUE))))
  expect_true(isTRUE(r$written))
  expect_identical(r$n_rows, 3000L)
  mf <- data.table::fread(fx$mf)
  expect_true(any(mf$Status == "partial_accepted" &
                    grepl("declared_ncases=5000 rows_written=3000", mf$Notes)))
  expect_length(list.files(ydir, pattern = "\\.parquet$"), 3L)   # chunks retained
})
