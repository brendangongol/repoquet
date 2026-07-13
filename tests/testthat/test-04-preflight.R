base_mdt <- function(...) {
  data.frame(Database = "R1", MDBDir = "R1", TableName = "Ev",
             Path = c("a.csv", "b.csv"), FileType = "csv",
             PartitionKey = "year", PartitionValue = c("2019", "2020"), ...)
}

quiet_preflight <- function(...) {
  out <- utils::capture.output(iss <- ValidateMDTPreflight(...))
  iss
}

test_that("a clean MDT passes with zero errors", {
  iss <- quiet_preflight(base_mdt(), strict = FALSE, ParquetBasePath = "X:/pq")
  expect_false(any(iss$Severity == "error"))
})

test_that("mixed partition key sets within a table error", {
  b <- base_mdt(); b$PartitionKey <- c("SITE", NA); b$PartitionValue <- c("M", NA); b$Year <- 2020
  iss <- quiet_preflight(b, strict = FALSE)
  expect_true("mixed_partition_keys" %in% iss$Check)
})

test_that("partition value sanitization collisions error", {
  b <- base_mdt(); b$PartitionValue <- c("S A", "S*A"); b$PartitionKey <- "SITE"
  iss <- quiet_preflight(b, strict = FALSE)
  expect_true("partition_value_collision" %in% iss$Check)
})

test_that("case-only partition directory collisions error on case-insensitive filesystems", {
  b <- base_mdt(); b$PartitionValue <- c("MGH", "mgh"); b$PartitionKey <- "SITE"
  iss <- quiet_preflight(b, strict = FALSE)
  expect_true("partition_value_collision" %in% iss$Check)
})

test_that("case-only repository table names are rejected", {
  b <- base_mdt()
  b$Database <- c("R1", "r1")
  iss <- quiet_preflight(b, strict = FALSE)
  expect_true("table_name_case_collision" %in% iss$Check)
})

test_that("YEAR values must be whole numbers (as.integer truncation gap stays closed)", {
  b <- base_mdt(); b$PartitionValue <- c("2019", "2019.5")
  iss <- quiet_preflight(b, strict = FALSE)
  expect_true("bad_year" %in% iss$Check)
})

test_that("output filename collisions are predicted with the writers' own stem logic", {
  b <- base_mdt(); b$Path <- c("data-2019.csv", "data_2019.csv"); b$PartitionValue <- "2019"
  iss <- quiet_preflight(b, strict = FALSE, ParquetBasePath = "X:/pq", MaxFileStemTruncate = TRUE)
  expect_true("output_filename_collision" %in% iss$Check)
  s <- base_mdt(); s$FileType <- "sav"; s$Path <- c("core.part1.sav", "core_part1.sav"); s$PartitionValue <- "2019"
  iss2 <- quiet_preflight(s, strict = FALSE, ParquetBasePath = "X:/pq", MaxFileStemTruncate = TRUE)
  expect_true("chunk_filename_collision" %in% iss2$Check)
})

test_that("AcceptPartial column is validated and surfaced", {
  b <- base_mdt(); b$AcceptPartial <- c("maybe", NA)
  iss <- quiet_preflight(b, strict = FALSE)
  expect_true("bad_accept_partial" %in% iss$Check)
  b$AcceptPartial <- c(TRUE, NA)
  iss2 <- quiet_preflight(b, strict = FALSE)
  expect_true("accept_partial_rows" %in% iss2$Check)
  expect_identical(iss2[iss2$Check == "accept_partial_rows", ]$Severity, "warning")
})

test_that("missing source files are caught when MasterDBPath is given", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = 1), file.path(fx$src, "a.csv"))
  b <- base_mdt(); b$MDBDir <- "REG"
  iss <- quiet_preflight(b, strict = FALSE, MasterDBPath = fx$root)
  expect_true("missing_source_files" %in% iss$Check)   # b.csv absent
  iss2 <- quiet_preflight(b, strict = FALSE)            # no MasterDBPath -> skipped
  expect_false("missing_source_files" %in% iss2$Check)
})

test_that("duplicate identities are detected via the checkpoint key", {
  b <- base_mdt(); b$Path <- "same.csv"; b$PartitionValue <- "2019"
  iss <- quiet_preflight(b, strict = FALSE)
  expect_true("duplicate_repository_key" %in% iss$Check)
})
