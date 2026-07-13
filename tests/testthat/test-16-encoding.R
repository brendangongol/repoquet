write_encoded_delimited_fixture <- function(path, lines, encoding) {
  encoded <- iconv(lines, from = "UTF-8", to = encoding, sub = NA_character_, toRaw = TRUE)
  if (any(vapply(encoded, is.null, logical(1)))) stop("Fixture encoding failed.")
  con <- file(path, open = "wb")
  on.exit(close(con), add = TRUE)
  for (line in encoded) {
    writeBin(line, con)
    writeBin(as.raw(0x0A), con)
  }
  invisible(path)
}

test_that("automatic detection converts Windows-1252 CSV values to UTF-8 without touching the source", {
  path <- tempfile(fileext = ".csv")
  on.exit(unlink(path), add = TRUE)
  write_encoded_delimited_fixture(
    path,
    c("ID,TEXT", "1,\"hello, smart “quotes”\"", "2,café", "3,naïve"),
    "windows-1252"
  )
  before_raw <- readBin(path, what = "raw", n = file.info(path)$size)
  before_hash <- digest::digest(file = path, algo = "sha256")
  before_mtime <- file.info(path)$mtime

  rd <- get_file_reader("csv")
  header <- call_reader(rd, "read_header", path, reader_options = list())
  sample <- call_reader(rd, "read_sample", path, reader_options = list())
  full <- call_reader(rd, "read_full", path, reader_options = list())
  info <- attr(full, "repoquet_encoding_info")
  chunk <- call_reader(rd, "read_chunk", path, reader_options = list(),
                       offset = 0L, n_max = 3L, header = header)

  expect_identical(header, c("ID", "TEXT"))
  expect_identical(info$EncodingUsed, "windows-1252")
  expect_identical(info$DetectionMethod, "icu")
  expect_identical(full$TEXT, c("hello, smart “quotes”", "café", "naïve"))
  expect_identical(sample$TEXT, full$TEXT)
  expect_identical(chunk$TEXT, full$TEXT)
  expect_true(all(validUTF8(full$TEXT)))
  expect_true(all(validUTF8(sample$TEXT)))
  expect_true(all(validUTF8(chunk$TEXT)))

  expect_identical(readBin(path, what = "raw", n = file.info(path)$size), before_raw)
  expect_identical(digest::digest(file = path, algo = "sha256"), before_hash)
  expect_identical(file.info(path)$mtime, before_mtime)
})

test_that("an incorrect declared encoding fails strictly without byte substitution", {
  path <- tempfile(fileext = ".csv")
  on.exit(unlink(path), add = TRUE)
  write_encoded_delimited_fixture(path, c("ID,TEXT", "1,“quoted”"), "windows-1252")
  before <- readBin(path, what = "raw", n = file.info(path)$size)

  expect_error(
    read_delimited_full(path, reader_options = list(Encoding = "UTF-8")),
    "UTF-8 conversion failed"
  )
  expect_identical(readBin(path, what = "raw", n = file.info(path)$size), before)
})

test_that("later legacy bytes trigger an automatic content-level encoding retry", {
  path <- tempfile(fileext = ".csv")
  on.exit(unlink(path), add = TRUE)
  write_encoded_delimited_fixture(path, c("ID,TEXT", "1,ASCII", "2,“late byte”"),
                                  "windows-1252")
  initial_utf8 <- list(
    DeclaredEncoding = "auto", DetectedEncoding = "UTF-8",
    EncodingConfidence = 1, EncodingUsed = "UTF-8",
    DetectionMethod = "strict_utf8"
  )
  result <- read_delimited_full(
    path,
    reader_options = list(Encoding = "UTF-8", .EncodingInfo = initial_utf8)
  )
  info <- attr(result, "repoquet_encoding_info")
  expect_identical(result$TEXT, c("ASCII", "\u201clate byte\u201d"))
  expect_identical(info$EncodingUsed, "windows-1252")
  expect_identical(info$DetectionMethod, "content_retry")
  expect_true(all(validUTF8(result$TEXT)))
})

test_that("schema observations record the encoding used and contain UTF-8-valid evidence", {
  skip_if_not_installed("arrow")
  root <- tempfile("encoding_survey_")
  dir.create(file.path(root, "D"), recursive = TRUE)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source <- file.path(root, "D", "legacy.csv")
  write_encoded_delimited_fixture(source, c("ID,TEXT", "1,café “quoted”"), "windows-1252")
  before <- digest::digest(file = source, algo = "sha256")
  MDT <- data.frame(Database = "D", MDBDir = "D", Path = "legacy.csv",
                    TableName = "T", FileType = "csv",
                    PartitionKey = "year", PartitionValue = "2024")
  observations <- file.path(root, "SchemaObservations.parquet")

  survey <- SurveyRepositorySchema(MDT, root, observations, n_workers = 1,
                                   SourceFingerprintMode = "none", StrictReaders = TRUE)
  rows <- survey$observations[SurveyStatus == "ok"]
  expect_true(nrow(rows) > 0L)
  expect_true(all(rows$EncodingUsed == "windows-1252"))
  expect_true(all(rows$EncodingValidationStatus == "sample_valid_utf8"))
  expect_identical(digest::digest(file = source, algo = "sha256"), before)
})

test_that("chunked repository loading writes only UTF-8 text and leaves legacy sources unchanged", {
  fx <- new_repo_fixture()
  on.exit(unlink(fx$root, recursive = TRUE), add = TRUE)
  source <- file.path(fx$src, "legacy_2024.csv")
  values <- sprintf("row %02d “naïve café”", 1:25)
  write_encoded_delimited_fixture(
    source,
    c("ID,TEXT", sprintf("%d,\"%s\"", 1:25, values)),
    "windows-1252"
  )
  before_raw <- readBin(source, what = "raw", n = file.info(source)$size)
  before_hash <- digest::digest(file = source, algo = "sha256")
  MDT <- data.frame(Database = "REG", MDBDir = "REG", Path = "legacy_2024.csv",
                    TableName = "Legacy", FileType = "csv",
                    PartitionKey = "year", PartitionValue = "2024")

  result <- run_loader(fx, MDT, "REG")
  expect_length(result$checkpoint, 1L)
  files <- list.files(file.path(fx$pq, "REG_Legacy", "year=2024"),
                      pattern = "parquet$", full.names = TRUE)
  expect_length(files, 3L)
  stored <- data.table::rbindlist(lapply(files, function(path) {
    data.table::as.data.table(arrow::read_parquet(path))
  }))
  data.table::setorder(stored, ID)
  expect_identical(stored$TEXT, values)
  expect_true(all(validUTF8(stored$TEXT)))
  expect_identical(readBin(source, what = "raw", n = file.info(source)$size), before_raw)
  expect_identical(digest::digest(file = source, algo = "sha256"), before_hash)
})
