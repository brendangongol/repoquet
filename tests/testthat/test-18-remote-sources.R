remote_mdt <- function(path = "data.csv", uri = "https://example.test/data.csv",
                       policy = "if_missing", expected = NA_character_) {
  data.frame(
    Database = "WEB", MDBDir = "unused", Path = path,
    TableName = "Events", FileType = "csv",
    PartitionKey = "year", PartitionValue = "2024",
    SourceURI = uri, DownloadPolicy = policy, ExpectedSHA256 = expected,
    stringsAsFactors = FALSE)
}

copy_downloader <- function(source) {
  force(source)
  function(uri, destination) file.copy(source, destination, overwrite = TRUE)
}

test_that("remote sources materialize without modifying the source", {
  root <- tempfile("remote_source_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source <- file.path(root, "vendor.csv")
  writeLines(c("ID,VALUE", "1,alpha"), source, useBytes = TRUE)
  before <- readBin(source, "raw", n = file.info(source)$size)

  resolved <- MaterializeRemoteSources(
    remote_mdt(), file.path(root, "cache"),
    DownloadFunction = copy_downloader(source))

  expect_true(file.exists(resolved$ResolvedSourcePath))
  expect_identical(resolved$RemoteCacheStatus, "downloaded")
  expect_match(resolved$RemoteSourceSHA256, "^[0-9a-f]{64}$")
  expect_identical(readBin(source, "raw", n = file.info(source)$size), before)
  expect_identical(source_path_for_row(resolved, file.path(root, "missing")),
                   resolved$ResolvedSourcePath)
})

test_that("if_missing and offline modes reuse a valid cache", {
  root <- tempfile("remote_cache_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source <- file.path(root, "vendor.csv")
  writeLines(c("ID", "1"), source)
  first <- MaterializeRemoteSources(
    remote_mdt(), file.path(root, "cache"),
    DownloadFunction = copy_downloader(source))

  fail_if_called <- function(uri, destination) stop("network should not be used")
  second <- MaterializeRemoteSources(
    remote_mdt(), file.path(root, "cache"),
    DownloadFunction = fail_if_called)
  offline <- MaterializeRemoteSources(
    remote_mdt(), file.path(root, "cache"), Offline = TRUE,
    DownloadFunction = fail_if_called)

  expect_identical(second$RemoteCacheStatus, "cached")
  expect_identical(offline$RemoteCacheStatus, "offline_cached")
  expect_identical(second$RemoteSourceSHA256, first$RemoteSourceSHA256)
})

test_that("if_changed refreshes only when downloaded content changes", {
  root <- tempfile("remote_changed_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source <- file.path(root, "vendor.csv")
  writeLines(c("ID", "1"), source)
  first <- MaterializeRemoteSources(
    remote_mdt(policy = "if_changed"), file.path(root, "cache"),
    DownloadFunction = copy_downloader(source))
  unchanged <- MaterializeRemoteSources(
    remote_mdt(policy = "if_changed"), file.path(root, "cache"),
    DownloadFunction = copy_downloader(source))
  writeLines(c("ID", "2"), source)
  changed <- MaterializeRemoteSources(
    remote_mdt(policy = "if_changed"), file.path(root, "cache"),
    DownloadFunction = copy_downloader(source))

  expect_identical(unchanged$RemoteCacheStatus, "unchanged")
  expect_identical(changed$RemoteCacheStatus, "updated")
  expect_false(identical(first$RemoteSourceSHA256, changed$RemoteSourceSHA256))
  expect_identical(readLines(changed$ResolvedSourcePath), c("ID", "2"))
})

test_that("hash mismatch preserves an existing cache", {
  root <- tempfile("remote_hash_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source <- file.path(root, "vendor.csv")
  writeLines(c("ID", "original"), source)
  first <- MaterializeRemoteSources(
    remote_mdt(), file.path(root, "cache"),
    DownloadFunction = copy_downloader(source))
  cached_before <- readBin(first$ResolvedSourcePath, "raw",
                           n = file.info(first$ResolvedSourcePath)$size)
  writeLines(c("ID", "replacement"), source)

  expect_error(
    MaterializeRemoteSources(
      remote_mdt(policy = "always", expected = paste(rep("0", 64), collapse = "")),
      file.path(root, "cache"), DownloadFunction = copy_downloader(source)),
    "SHA-256 mismatch")
  expect_identical(
    readBin(first$ResolvedSourcePath, "raw", n = file.info(first$ResolvedSourcePath)$size),
    cached_before)
})

test_that("preflight validates remote declarations", {
  bad_uri <- remote_mdt(uri = "ftp://example.test/data.csv")
  bad_policy <- remote_mdt(policy = "sometimes")
  bad_hash <- remote_mdt(expected = "abc")
  embedded <- remote_mdt(uri = "https://user:secret@example.test/data.csv")

  capture.output(uri_issues <- ValidateMDTPreflight(bad_uri, strict = FALSE))
  capture.output(policy_issues <- ValidateMDTPreflight(bad_policy, strict = FALSE))
  capture.output(hash_issues <- ValidateMDTPreflight(bad_hash, strict = FALSE))
  capture.output(credential_issues <- ValidateMDTPreflight(embedded, strict = FALSE))

  expect_true("bad_source_uri" %in% uri_issues$Check)
  expect_true("bad_download_policy" %in% policy_issues$Check)
  expect_true("bad_expected_sha256" %in% hash_issues$Check)
  expect_true("embedded_source_credentials" %in% credential_issues$Check)
})

test_that("schema survey reads a materialized cache through the common resolver", {
  root <- tempfile("remote_survey_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  source <- file.path(root, "vendor.csv")
  writeLines(c("ID,VALUE", "1,alpha", "2,beta"), source)
  resolved <- MaterializeRemoteSources(
    remote_mdt(), file.path(root, "cache"),
    DownloadFunction = copy_downloader(source))
  observation_path <- file.path(root, "SchemaObservations.parquet")

  result <- SurveyRepositorySchema(
    resolved, MasterDBPath = file.path(root, "does_not_exist"),
    ObservationPath = observation_path, n_workers = 1L,
    SourceFingerprintMode = "metadata", StrictReaders = TRUE)

  expect_true(file.exists(observation_path))
  expect_equal(result$summary$Sources, 1L)
  expect_equal(result$summary$FailedSources, 0L)
})

test_that("RepositoryInitialize exposes and creates the source cache", {
  root <- tempfile("remote_paths_")
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  paths <- RepositoryInitialize(root, create = TRUE)
  expect_true(dir.exists(paths$DownloadCachePath))
  expect_identical(paths$DownloadCachePath, file.path(root, "SourceCache"))
})

test_that("remote acquisition settings can be overridden through config loading", {
  root <- tempfile("remote_config_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  config <- file.path(root, "repository_config.R")
  #### tempfile() on Windows returns backslash paths; writing those raw into ####
  #### generated R source breaks the parser (e.g. "\U" is read as a Unicode ####
  #### escape). Normalize to forward slashes, matching                       ####
  #### create_repository_project()'s norm() helper.                          ####
  norm <- function(p) gsub("\\\\", "/", p)
  writeLines(c(
    "repository_config <- list(",
    sprintf("MasterDBPath='%s',", norm(root)),
    sprintf("FormattedDBPath='%s',", norm(file.path(root, "formatted"))),
    sprintf("MDTPath='%s')", norm(file.path(root, "DBSetup.xlsx")))), config)

  cfg <- load_repository_config(
    config, RemoteOffline = TRUE, DownloadPolicy = "if_changed",
    DownloadTimeout = 120, SchemaSurveyMode = "full",
    SchemaWorkers = 3L, SchemaReuseCache = FALSE,
    SchemaFastReadMaxBytes = 1024, SchemaChunkSize = 2000L,
    SchemaAdaptiveSampleRows = 500L, SchemaFutureGlobalsMaxSizeMB = 900)
  expect_true(cfg$RemoteOffline)
  expect_identical(cfg$DownloadPolicy, "if_changed")
  expect_identical(cfg$DownloadTimeout, 120)
  expect_identical(cfg$SchemaSurveyMode, "full")
  expect_identical(cfg$SchemaWorkers, 3L)
  expect_false(cfg$SchemaReuseCache)
  expect_identical(cfg$SchemaFastReadMaxBytes, 1024)
  expect_identical(cfg$SchemaChunkSize, 2000)
  expect_identical(cfg$SchemaAdaptiveSampleRows, 500)
  expect_identical(cfg$SchemaFutureGlobalsMaxSizeMB, 900)
})

test_that("remote provenance is retained in the repository manifest", {
  root <- tempfile("remote_manifest_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  manifest <- file.path(root, "manifest.csv")
  update_parquet_manifest(
    manifest, Database = "WEB", TableName = "Events", DuckDBTable = "WEB_Events",
    Year = 2024, SourcePath = "data.csv", ParquetPath = "year=2024/data.parquet",
    PartitionKey = "YEAR", PartitionValue = "2024")
  row <- remote_mdt()
  row$ResolvedSourcePath <- file.path(root, "cache", "data.csv")
  row$RemoteSourceSHA256 <- paste(rep("a", 64), collapse = "")
  row$RemoteDownloadedAt <- "2026-07-15T00:00:00Z"
  row$RemoteCacheStatus <- "downloaded"
  update_manifest_source_provenance(manifest, "WEB", "Events", "data.csv", row)

  recorded <- read_parquet_manifest(manifest)
  expect_identical(recorded$ManifestSchemaVersion, 3L)
  expect_identical(recorded$SourceURI, row$SourceURI)
  expect_identical(recorded$DownloadPolicy, row$DownloadPolicy)
  expect_identical(recorded$DownloadStatus, "downloaded")
  expect_identical(recorded$DownloadSHA256, row$RemoteSourceSHA256)
})

test_that("ZIP members materialize atomically and reject traversal paths", {
  root <- tempfile("remote_zip_"); dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)
  zip_raw <- jsonlite::base64_dec(paste0(
    "UEsDBBQAAAAIAPBy71zqThLKDgAAAAwAAAAPAAAAbmVzdGVkL2RhdGEuY3N2c9Rx",
    "4jLUqeAy0qnkAgBQSwMEFAAAAAgA8HLvXDchzLEGAAAABAAAAAkAAABvdGhlci5j",
    "c3Zz5jLmAgBQSwECFAAUAAAACADwcu9c6k4Syg4AAAAMAAAADwAAAAAAAAAAAAAA",
    "gAEAAAAAbmVzdGVkL2RhdGEuY3N2UEsBAhQAFAAAAAgA8HLvXDchzLEGAAAABAAA",
    "AAkAAAAAAAAAAAAAAIABOwAAAG90aGVyLmNzdlBLBQYAAAAAAgACAHQAAABoAAAA",
    "AAA="))
  calls <- 0L
  downloader <- function(uri, destination) {
    calls <<- calls + 1L
    writeBin(zip_raw, destination)
    0L
  }
  rows <- data.table::rbindlist(list(
    remote_mdt(path = "data.csv", uri = "https://example.test/bundle.zip"),
    remote_mdt(path = "other.csv", uri = "https://example.test/bundle.zip")))
  rows[, `:=`(ArchiveType = "zip",
              ArchiveMember = c("nested/data.csv", "other.csv"))]

  resolved <- MaterializeRemoteSources(rows, file.path(root, "cache"),
                                       DownloadFunction = downloader)
  expect_identical(calls, 1L)
  expect_true(all(file.exists(resolved$ResolvedSourcePath)))
  expect_identical(readLines(resolved$ResolvedSourcePath[1]), c("A,B", "1,x", "2,y"))
  expect_true(all(grepl("extracted$", resolved$RemoteCacheStatus)))

  unsafe <- rows[1]
  unsafe$ArchiveMember <- "../escape.csv"
  capture.output(issues <- ValidateMDTPreflight(unsafe, strict = FALSE))
  expect_true("unsafe_archive_member" %in% issues$Check)
  expect_error(MaterializeRemoteSources(
    unsafe, file.path(root, "unsafe"), DownloadFunction = downloader),
    "safe relative ArchiveMember")
})
