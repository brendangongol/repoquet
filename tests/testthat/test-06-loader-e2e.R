test_that("year- and site-partitioned tables load, resume, and reconcile", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "r_2020.csv"))
  data.table::fwrite(data.table::data.table(AGE = c(3, 4), CODE = c("Z3", "Z4")), file.path(fx$src, "r_2021.csv"))
  data.table::fwrite(data.table::data.table(AGE = c(30, 40), CODE = c("X1", "X2")), file.path(fx$src, "r_mgh.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = c("Yr", "Yr", "Ev"),
                  Path = c("r_2020.csv", "r_2021.csv", "r_mgh.csv"), FileType = "csv",
                  PartitionKey = c("year", "year", "SITE"), PartitionValue = c("2020", "2021", "MGH"))
  r1 <- run_loader(fx, M, "REG")
  expect_length(r1$checkpoint, 3L)
  expect_true(dir.exists(file.path(fx$pq, "REG_Yr", "year=2020")))
  expect_true(dir.exists(file.path(fx$pq, "REG_Ev", "site=MGH")))
  # partition columns are never written into the files
  p <- arrow::read_parquet(list.files(file.path(fx$pq, "REG_Ev", "site=MGH"), full.names = TRUE)[1])
  expect_false("SITE" %in% names(p))
  # resume skips everything
  r2 <- run_loader(fx, M, "REG", completed = load_checkpoint(fx$cp))
  expect_true(any(grepl("Skipping 3 already-completed", r2$output)))
  # manifest Year: from the spec for year rows, NA for site rows
  mf <- data.table::fread(fx$mf)
  expect_setequal(mf[mf$TableName == "Yr" & mf$Status == "written", ]$Year, c(2020L, 2021L))
  expect_true(all(is.na(mf[mf$TableName == "Ev", ]$Year)))
})

test_that("a direct-read source spanning multiple years is routed to its own Hive partitions", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  # header carries YEAR itself, so this is eligible for source-defined partition
  # routing even though PartitionBy="FAIL" would otherwise try a direct read first
  data.table::fwrite(data.table::data.table(
    YEAR = c(2020L, 2021L, 2022L, 2023L), AGE = c(10L, 20L, 30L, 40L)),
    file.path(fx$src, "multi_year.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "MultiYr",
                  Path = "multi_year.csv", FileType = "csv",
                  PartitionKey = "YEAR", PartitionValue = "2020")
  r <- run_loader(fx, M, "REG", PartitionBy = "FAIL")
  expect_true(any(grepl("PARTITION ROUTE", r$output)))
  for (yr in 2020:2023) {
    expect_true(dir.exists(file.path(fx$pq, "REG_MultiYr", sprintf("year=%d", yr))))
  }
  rows_per_year <- sapply(2020:2023, function(yr) {
    f <- list.files(file.path(fx$pq, "REG_MultiYr", sprintf("year=%d", yr)), full.names = TRUE)
    nrow(arrow::read_parquet(f[1]))
  })
  expect_identical(sum(rows_per_year), 4L)
})

test_that("physical parquet schemas are identical across years despite type drift", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  # KEY_X numeric one year / character the next; AGE int vs decimal; DIED string vs int
  data.table::fwrite(data.table::data.table(KEY_X = c(101, 102), AGE = c(30L, 40L), DIED = c("0", "1")),
                     file.path(fx$src, "core_2019.csv"))
  data.table::fwrite(data.table::data.table(KEY_X = c("103", "104"), AGE = c(50.5, 60.2), DIED = c(0L, 1L)),
                     file.path(fx$src, "core_2020.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Core",
                  Path = c("core_2019.csv", "core_2020.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2019", "2020"))
  run_loader(fx, M, "REG")
  sch <- function(f) {
    s <- arrow::read_parquet(f, as_data_frame = FALSE)$schema
    stats::setNames(sapply(s$fields, function(x) x$type$ToString()),
                    sapply(s$fields, function(x) x$name))
  }
  s19 <- sch(list.files(file.path(fx$pq, "REG_Core", "year=2019"), full.names = TRUE)[1])
  s20 <- sch(list.files(file.path(fx$pq, "REG_Core", "year=2020"), full.names = TRUE)[1])
  expect_identical(s19[sort(names(s19))], s20[sort(names(s20))])
  expect_identical(unname(s19[["KEY_X"]]), "int32")    # data-derived in generic profile
  expect_identical(unname(s19[["AGE"]]), "double")     # promoted
  expect_identical(unname(s19[["DIED"]]), "int32")     # consistently integer-like
})

test_that("stale chunks from a previous run are removed before rewriting", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  sav <- file.path(fx$root, "big.sav")
  haven::write_sav(data.frame(AGE = 1:25, SEX = rep(1:5, 5)), sav)
  ydir <- file.path(fx$root, "REG_T", "year=2020"); dir.create(ydir, recursive = TRUE)
  stem <- parquet_chunk_stem(sav, partition_dir = ydir, MaxFileStemTruncate = FALSE)
  stale <- file.path(ydir, sprintf("%s_%05d.parquet", stem, 9L))
  arrow::write_parquet(data.frame(AGE = 99), stale)
  out <- utils::capture.output(
    r <- safe_read_sav_chunked(path = sav, chunk_size = 10L, year_dir = ydir,
                               out_path = file.path(ydir, "x.parquet"),
                               all_cols = c("AGE", "SEX"),
                               col_classes = list(AGE = "numeric", SEX = "numeric"),
                               year_val = 2020, ManifestPath = fx$mf,
                               Database = "REG", TableName = "T", DuckDBTable = "REG_T",
                               SourcePath = "big.sav"))
  expect_false(file.exists(stale))
  expect_true(isTRUE(r$written))
  tot <- sum(sapply(list.files(ydir, full.names = TRUE), function(f) nrow(arrow::read_parquet(f))))
  expect_identical(tot, 25L)
})

test_that("a missing source file degrades to a per-file failure, not a database failure", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "good1.csv"))
  data.table::fwrite(data.table::data.table(AGE = c(3, 4), CODE = c("Z3", "Z4")), file.path(fx$src, "good2.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T",
                  Path = c("good1.csv", "good2.csv", "missing.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2019", "2020", "2021"))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 2L)
  expect_true(any(grepl("missing on disk", r$output)))
  r2 <- run_loader(fx, M, "REG", completed = load_checkpoint(fx$cp))
  expect_true(any(grepl("Skipping 2 already-completed", r2$output)))
})

test_that("database-level failures make the orchestrator fail after logging", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = 1), file.path(fx$src, "a.csv"))
  data.table::fwrite(data.table::data.table(AGE = 2), file.path(fx$src, "b.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T",
                  Path = c("a.csv", "b.csv"), FileType = c("csv", "sav"),
                  PartitionKey = "year", PartitionValue = c("2019", "2020"))
  expect_error(run_loader(fx, M, "REG"), "failed for 1 database")
})
