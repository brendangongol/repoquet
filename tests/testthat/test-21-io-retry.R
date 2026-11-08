#### Retrying transient I/O failures (a network drive that drops out) ####
#### Synthetic files and injected failures only; every pause is 0 seconds. ####

no_pause <- function() {
  old <- options(repoquet.io_retry_attempts = 3L, repoquet.io_retry_pause_seconds = 0)
  old
}

mock_manifest <- function(name, make) mock_repoquet(name, make, via = "update_parquet_manifest")

#### Replaces an internal repoquet function, in the environment of the function under test (via),
#### for the life of one test, and returns the restorer.
mock_repoquet <- function(name, make, via = "save_checkpoint") {
  env <- environment(get(via, mode = "function"))
  original <- get(name, envir = env)
  if (isNamespace(env)) {
    utils::assignInNamespace(name, make(original), ns = getNamespaceName(env))
    function() utils::assignInNamespace(name, original, ns = getNamespaceName(env))
  } else {
    assign(name, make(original), envir = env)
    function() assign(name, original, envir = env)
  }
}

#### A reader whose whole-file read is scripted: behavior(call_number, df) returns the rows or stops.
register_scripted_reader <- function(type, behavior) {
  state <- new.env()
  state$full_calls <- 0L
  register_file_reader(type,
    read_full   = function(p, ...) {
      state$full_calls <- state$full_calls + 1L
      behavior(state$full_calls, data.table::fread(p))
    },
    read_header = function(p, ...) names(data.table::fread(p, nrows = 0L)),
    read_sample = function(p, ...) data.table::fread(p, nrows = 100L),
    count_rows  = function(p, ...) nrow(data.table::fread(p, select = 1L)),
    read_chunk  = function(p, offset, n_max, header, col_classes = NULL, ...) {
      d <- data.table::fread(p)
      if (offset >= nrow(d)) return(d[0])   # past the end: an empty table, as a real reader returns
      d[seq.int(offset + 1L, min(nrow(d), offset + n_max))]
    },
    chunkable = TRUE)
  state
}

scripted_fixture <- function(n = 8L) {
  fx <- new_repo_fixture()
  data.table::fwrite(data.table::data.table(ID = seq_len(n), V = seq_len(n) * 2L), file.path(fx$src, "x.dat"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Scripted", Path = "x.dat",
                  FileType = "scripted", PartitionKey = "year", PartitionValue = "2019")
  list(fx = fx, M = M)
}

stored_ids <- function(fx) {
  pieces <- list.files(file.path(fx$pq, "REG_Scripted", "year=2019"), pattern = "parquet$", full.names = TRUE)
  sort(unlist(lapply(pieces, function(p) arrow::read_parquet(p)$ID)))
}

test_that("the classifier retries I/O failures and never retries deterministic failures", {
  transient <- c("IOError: Error writing bytes to file. Detail: [errno 22] Invalid argument",
                 "cannot open the connection",
                 "IO Error: Could not set lock on file",
                 "Checkpoint verification failed after replacement.",
                 "NIS_2019.ASC line 3,700,003 has 888 character(s), but layout x.SPS defines 1026-character records.",
                 "Hardened FAIL worker exited with status -1073741818 without a result",
                 "row count mismatch: read 5 row(s) from a.csv but the independent count is 8.",
                 "premature end of file: a.csv ended after 10 of 99 bytes (a dropped connection or a short read).",
                 #### The fragment of a record a cut-off read leaves at the end of a batch.
                 "Delimited structure error in a.csv: Discarded single-line footer: <<31108,3201805453832,0,0>>. The source was not modified.")
  deterministic <- c("cannot allocate vector of size 152.2 Mb", "result would exceed 2^31-1 bytes",
                     "Layout file not found: x.SPS", "Hardened FAIL worker did not complete: timed out",
                     "std::bad_alloc", "Delimited structure error in a.csv: Stopped early on line 4",
                     "Delimited structure error in a.csv: Stopped early on line 4. First discarded non-empty line: <<15,t15>>.",
                     "Can't assign 544 names to a 248-column data.table", NA_character_, "")
  for (m in transient) expect_true(.is_transient_io_error(m), info = m)
  for (m in deterministic) expect_false(.is_transient_io_error(m), info = as.character(m))
})

test_that("byte-positioned delimited chunks preserve newlines inside quoted fields", {
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(csv), add = TRUE)
  writeLines(c("ID,DESC,CODE", "1,\"first line", "second line\",A",
               "2,plain,B", "3,\"quoted, comma\",C"), csv, useBytes = TRUE)
  opts <- .resolve_delimited_reader_options(csv, list())
  header <- .read_delimited_header(csv, opts)

  first <- .read_delimited_chunk(csv, offset = 0L, n_max = 2L, header = header,
                                 reader_options = opts, exact_rows = TRUE)
  second <- .read_delimited_chunk(csv, offset = 2L, n_max = 2L, header = header,
                                  reader_options = opts, exact_rows = FALSE)

  expect_identical(first$ID, 1:2)
  expect_identical(gsub("\r\n", "\n", first$DESC), c("first line\nsecond line", "plain"))
  expect_identical(second$ID, 3L)
  expect_identical(second$DESC, "quoted, comma")
})

test_that(".retry_io retries a transient failure once, and stops at the first deterministic one", {
  old <- no_pause(); on.exit(options(old), add = TRUE)
  calls <- 0L
  out <- utils::capture.output(value <- .retry_io(function() {
    calls <<- calls + 1L
    if (calls == 1L) stop("IOError: Error reading bytes. Detail: [errno 22] Invalid argument")
    "ok"
  }, label = "demo"))
  expect_identical(value, "ok")
  expect_identical(calls, 2L)

  calls <- 0L
  expect_error(.retry_io(function() { calls <<- calls + 1L; stop("cannot allocate vector of size 1 Gb") },
                         label = "demo"), "cannot allocate")
  expect_identical(calls, 1L)

  calls <- 0L
  out <- utils::capture.output(expect_error(.retry_io(function() {
    calls <<- calls + 1L
    stop("IOError: the drive is gone")
  }, label = "demo"), "IOError"))
  expect_identical(calls, 3L)

  options(repoquet.io_retry_attempts = 5L)
  calls <- 0L
  out <- utils::capture.output(expect_error(.retry_io(function() {
    calls <<- calls + 1L
    stop("row count mismatch: read 1 row(s) from a.csv but the independent count is 2.")
  }, label = "demo"), "row count mismatch"))
  expect_identical(calls, 2L)   # a mismatch repeats, and each try is a full read
})

test_that("the retry settings come from options with safe fallbacks, and the waits triple", {
  old <- options(repoquet.io_retry_attempts = 4L, repoquet.io_retry_pause_seconds = 2); on.exit(options(old), add = TRUE)
  expect_identical(.io_retry_waits(), c(0, 2, 6, 18))
  options(repoquet.io_retry_attempts = "x", repoquet.io_retry_pause_seconds = -1)
  expect_identical(.io_retry_waits(), c(0, 10, 30))
  expect_identical(.check_read_row_count(5, NA, "a.csv"), invisible(TRUE))
  expect_error(.check_read_row_count(5, 8, "a.csv"), "row count mismatch: read 5 row\\(s\\) from a.csv")
})

test_that("a checkpoint save that fails once is retried and ends verified; a persistent failure keeps the old one", {
  old <- no_pause(); on.exit(options(old), add = TRUE)
  path <- tempfile(fileext = ".rds"); on.exit(unlink(paste0(path, c("", ".pending", ".previous"))), add = TRUE)
  save_checkpoint(c("a", "b"), path)

  failures <- 0L
  restore <- mock_repoquet("replace_file_safely", function(original) function(tmp, path) {
    if (failures < 1L) { failures <<- failures + 1L; stop("Could not replace file: simulated drop") }
    original(tmp, path)
  })
  out <- utils::capture.output(save_checkpoint(c("a", "b", "c"), path))
  restore()
  expect_identical(failures, 1L)
  expect_identical(load_checkpoint(path), c("a", "b", "c"))
  expect_false(file.exists(paste0(path, ".pending")))
  expect_true(any(grepl("\\[IO RETRY\\]", out)))

  restore <- mock_repoquet("replace_file_safely", function(original) function(tmp, path) {
    stop("Could not replace file: the drive is gone")
  })
  out <- utils::capture.output(expect_error(save_checkpoint(c("x"), path), "Could not replace file"))
  restore()
  expect_identical(load_checkpoint(path), c("a", "b", "c"))   # never replaced by a failed save
})

test_that("manifest writes that fail once are retried without duplicating or losing rows", {
  old <- no_pause(); on.exit(options(old), add = TRUE)
  root <- tempfile("mf_"); dir.create(root); on.exit(unlink(root, recursive = TRUE), add = TRUE)

  #### DuckDB manifest: the first transaction fails midway, rolls back, and the retry commits once.
  mf <- file.path(root, "manifest.duckdb")
  failures <- 0L
  restore <- mock_manifest("manifest_add_missing_columns", function(original) function(con, row) {
    if (failures < 1L) { failures <<- failures + 1L; stop("IO Error: Could not set lock on file (simulated)") }
    original(con, row)
  })
  out <- utils::capture.output(update_parquet_manifest(mf, "D", "T", "D_T", 2024, "a.csv", "p1.parquet", 5,
                                                        Notes = "chunk_00001"))
  restore()
  expect_identical(failures, 1L)
  expect_true(any(grepl("\\[IO RETRY\\]", out)))
  m <- read_parquet_manifest(mf)
  expect_identical(nrow(m), 1L)
  expect_identical(m$NRows, 5)

  #### CSV manifest: the replacement fails once and the file is rewritten whole.
  mf_csv <- file.path(root, "manifest.csv")
  update_parquet_manifest(mf_csv, "D", "T", "D_T", 2024, "a.csv", "p1.parquet", 5, Notes = "chunk_00001")
  failures <- 0L
  restore <- mock_manifest("replace_file_safely", function(original) function(tmp, path) {
    if (failures < 1L) { failures <<- failures + 1L; stop("Could not replace file: simulated drop") }
    original(tmp, path)
  })
  out <- utils::capture.output(update_parquet_manifest(mf_csv, "D", "T", "D_T", 2024, "a.csv", "p2.parquet", 7,
                                                        Notes = "chunk_00002"))
  restore()
  expect_identical(failures, 1L)
  m <- read_parquet_manifest(mf_csv)
  expect_identical(sort(m$ParquetPath), c("p1.parquet", "p2.parquet"))
  expect_equal(sum(m$NRows), 12)
  expect_length(list.files(root, pattern = "\\.tmp_|\\.bak_"), 0L)
})

test_that("a direct read that fails once with an I/O error is retried, with no fallback to chunking", {
  s <- scripted_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  state <- register_scripted_reader("scripted", function(k, df) {
    if (k == 1L) stop("IOError: Error reading bytes from file. Detail: [errno 22] Invalid argument")
    df
  })
  r <- run_loader(s$fx, s$M, "REG", IORetryPauseSeconds = 0)
  expect_length(r$checkpoint, 1L)
  expect_identical(state$full_calls, 2L)
  expect_true(any(grepl("\\[IO RETRY\\] Attempt 2/3", r$output)))
  expect_false(any(grepl("chunked reader", r$output)))
  expect_identical(stored_ids(s$fx), 1:8)
})

test_that("a memory failure is not retried and falls back to the chunked reader as before", {
  s <- scripted_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  state <- register_scripted_reader("scripted", function(k, df) stop("cannot allocate vector of size 152.2 Mb"))
  r <- run_loader(s$fx, s$M, "REG", PartitionBy = "FAIL", FailProbeMode = "in_process", IORetryPauseSeconds = 0)
  expect_length(r$checkpoint, 1L)
  expect_identical(state$full_calls, 1L)
  expect_false(any(grepl("\\[IO RETRY\\]", r$output)))
  expect_true(any(grepl("using adaptive chunked reader", r$output)))
  expect_identical(stored_ids(s$fx), 1:8)
})

test_that("a direct read shorter than the independent count is detected, retried, and never loads short", {
  #### Short once: the retry returns every row.
  s <- scripted_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  state <- register_scripted_reader("scripted", function(k, df) if (k == 1L) df[seq_len(5L)] else df)
  r <- run_loader(s$fx, s$M, "REG", IORetryPauseSeconds = 0)
  expect_length(r$checkpoint, 1L)
  expect_identical(state$full_calls, 2L)
  expect_true(any(grepl("row count mismatch", r$output)))
  expect_identical(stored_ids(s$fx), 1:8)

  #### Always short, FAIL mode: one retry, then the chunked reader loads the whole file.
  s2 <- scripted_fixture(); on.exit(unlink(s2$fx$root, recursive = TRUE), add = TRUE)
  state2 <- register_scripted_reader("scripted", function(k, df) df[seq_len(5L)])
  r2 <- run_loader(s2$fx, s2$M, "REG", PartitionBy = "FAIL", FailProbeMode = "in_process", IORetryPauseSeconds = 0)
  expect_length(r2$checkpoint, 1L)
  expect_identical(state2$full_calls, 2L)
  expect_true(any(grepl("using adaptive chunked reader", r2$output)))
  expect_identical(stored_ids(s2$fx), 1:8)

  #### Always short, direct (NRows) mode: the file fails clearly and nothing is recorded.
  s3 <- scripted_fixture(); on.exit(unlink(s3$fx$root, recursive = TRUE), add = TRUE)
  register_scripted_reader("scripted", function(k, df) df[seq_len(5L)])
  r3 <- run_loader(s3$fx, s3$M, "REG", IORetryPauseSeconds = 0)
  expect_length(r3$checkpoint, 0L)
  expect_true(any(grepl("row count mismatch", r3$output)))
  expect_length(stored_ids(s3$fx), 0L)
})

test_that("the isolated worker is relaunched after a transient failure, not after a deterministic one", {
  old <- no_pause(); on.exit(options(old), add = TRUE)
  calls <- 0L
  out <- utils::capture.output(probe <- .retry_fail_probe(function() {
    calls <<- calls + 1L
    if (calls == 1L) list(ok = FALSE, error = "x.ASC line 3 has 888 character(s), but layout y.SPS defines 1026-character records.")
    else list(ok = TRUE, result = "loaded")
  }, "x.ASC"))
  expect_true(probe$ok)
  expect_identical(calls, 2L)
  expect_true(any(grepl("relaunching the isolated direct worker", out)))

  calls <- 0L
  probe <- .retry_fail_probe(function() { calls <<- calls + 1L; list(ok = FALSE, error = "cannot allocate vector of size 49.4 Mb") }, "x")
  expect_false(probe$ok)
  expect_identical(calls, 1L)

  calls <- 0L
  out <- utils::capture.output(probe <- .retry_fail_probe(function() {
    calls <<- calls + 1L
    list(ok = FALSE, error = "row count mismatch: read 1 row(s) from a.csv but the independent count is 2.")
  }, "a.csv"))
  expect_false(probe$ok)
  expect_identical(calls, 2L)
})

test_that("the worker compares its whole-file read with the independent row count before writing", {
  root <- tempfile("worker_"); dir.create(root); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  csv <- file.path(root, "a.csv")
  data.table::fwrite(data.table::data.table(ID = 1:6, V = 6:1), csv)
  payload <- list(path = csv, reader = "csv", reader_options = list(), col_classes = NULL,
                  expected_rows = 9, io_retry = list(attempts = 2L, pause = 0),
                  LogPath = file.path(root, "log.txt"), RunId = "t")
  rds <- file.path(root, "payload.rds"); saveRDS(payload, rds)
  response <- .hardened_fail_probe_worker(rds)
  expect_false(response$ok)
  expect_match(response$error, "row count mismatch: read 6 row\\(s\\) from a.csv but the independent count is 9")
})

test_that("the retry settings are visible configuration keys with validation", {
  cfg_path <- tempfile(fileext = ".R"); on.exit(unlink(cfg_path), add = TRUE)
  writeLines(c("repository_config <- list(MasterDBPath = 'a', FormattedDBPath = 'b', MDTPath = 'c')"), cfg_path)
  cfg <- load_repository_config(cfg_path)
  expect_identical(cfg$IORetryAttempts, 3L)
  expect_identical(cfg$IORetryPauseSeconds, 10)
  cfg2 <- load_repository_config(cfg_path, IORetryAttempts = 5L, IORetryPauseSeconds = 2)
  expect_identical(cfg2$IORetryAttempts, 5L)
  expect_identical(cfg2$IORetryPauseSeconds, 2)
  expect_error(load_repository_config(cfg_path, IORetryAttempts = 0L), "IORetryAttempts must be a positive integer")
  expect_error(load_repository_config(cfg_path, IORetryPauseSeconds = -1), "IORetryPauseSeconds must be a non-negative")
})

test_that("a source on a dropped drive is waited for, and a file missing under a visible root is not", {
  old <- no_pause(); on.exit(options(old), add = TRUE)

  #### The drive is back after the first pause.
  checks <- 0L
  out <- utils::capture.output(found <- .wait_for_source(
    "X:/data/a.csv", "X:/data",
    file_exists = function(p) { checks <<- checks + 1L; checks >= 2L },
    dir_exists = function(p) FALSE))
  expect_true(found)
  expect_identical(checks, 2L)
  expect_true(any(grepl("[IO RETRY] Attempt 2/3 after 0s: source root X:/data is not visible", out, fixed = TRUE)))

  #### The drive never returns: every attempt is used, then the file counts as missing.
  checks <- 0L
  out <- utils::capture.output(found <- .wait_for_source(
    "X:/data/a.csv", "X:/data",
    file_exists = function(p) { checks <<- checks + 1L; FALSE }, dir_exists = function(p) FALSE))
  expect_false(found)
  expect_identical(checks, 3L)

  #### The root is visible: the file is genuinely missing, so there is no pause and no retry.
  checks <- 0L
  out <- utils::capture.output(found <- .wait_for_source(
    "X:/data/a.csv", "X:/data",
    file_exists = function(p) { checks <<- checks + 1L; FALSE }, dir_exists = function(p) TRUE))
  expect_false(found)
  expect_identical(checks, 1L)
  expect_length(out, 0L)

  #### A file already present returns at once.
  expect_true(.wait_for_source("a.csv", "x", file_exists = function(p) TRUE, dir_exists = function(p) stop("unused")))

  #### End to end: a listed source that is not on disk fails as before, without any retry.
  s <- scripted_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  register_scripted_reader("scripted", function(k, df) df)
  s$M$Path <- "absent.dat"
  r <- run_loader(s$fx, s$M, "REG", IORetryPauseSeconds = 0)
  expect_length(r$checkpoint, 0L)
  expect_true(any(grepl("File not found", r$output)))
  expect_false(any(grepl("source root", r$output)))
})

#### Short reads in the chunked CSV reader and the fixed-width stream ####

chunked_csv_fixture <- function(n = 30L, short_line = NA_integer_) {
  fx <- new_repo_fixture()
  lines <- sprintf("%d,t%d,%.1f", seq_len(n), seq_len(n), seq_len(n) / 2)
  if (!is.na(short_line)) lines[short_line] <- sprintf("%d,t%d", short_line, short_line)   # one field missing
  writeLines(c("ID,TAG,VALUE", lines), file.path(fx$src, "c.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Chunked", Path = "c.csv", FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2019")
  list(fx = fx, M = M)
}

chunked_ids <- function(fx, table = "REG_Chunked") {
  pieces <- list.files(file.path(fx$pq, table, "year=2019"), pattern = "parquet$", full.names = TRUE)
  sort(unlist(lapply(pieces, function(p) arrow::read_parquet(p)$ID)))
}

test_that("a chunked CSV batch cut off mid-record is re-read from its start offset, and the file loads complete", {
  s <- chunked_csv_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  calls <- 0L
  restore <- mock_repoquet(".cursor_take", function(original) function(cursor, ...) {
    out <- original(cursor, ...)
    batch_text <- if (!is.null(out$Raw)) rawToChar(out$Raw) else out$Text
    if (nzchar(batch_text)) {
      calls <<- calls + 1L
      if (calls == 2L) {   # a record cut in two lines
        cut <- sub(",", "\n", batch_text, fixed = TRUE)
        if (!is.null(out$Raw)) out$Raw <- charToRaw(cut) else out$Text <- cut
      }
    }
    out
  }, via = ".read_delimited_chunk")
  r <- run_loader(s$fx, s$M, "REG", IORetryPauseSeconds = 0)
  restore()
  expect_length(r$checkpoint, 1L)
  expect_true(any(grepl("[IO RETRY] Attempt 2/", r$output, fixed = TRUE)))
  expect_true(any(grepl("chunk read c.csv from record 10", r$output, fixed = TRUE)))
  expect_identical(chunked_ids(s$fx), 1:30)   # no duplicated and no missing rows
})

test_that("an early end of file in a chunk read is detected against the expected rows and retried", {
  s <- chunked_csv_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  calls <- 0L
  restore <- mock_repoquet(".cursor_take", function(original) function(cursor, ...) {
    calls <<- calls + 1L
    if (calls == 3L) return(list(Text = "", Rows = 0L))   # the drive reports an end of file too soon
    original(cursor, ...)
  }, via = ".read_delimited_chunk")
  r <- run_loader(s$fx, s$M, "REG", IORetryPauseSeconds = 0)
  restore()
  expect_length(r$checkpoint, 1L)
  expect_true(any(grepl("row count mismatch: c.csv ended at record", r$output)))
  expect_identical(chunked_ids(s$fx), 1:30)
})

test_that("a CSV that is genuinely malformed at a fixed line fails clearly once the retries are used up", {
  old <- no_pause(); on.exit(options(old), add = TRUE)
  s <- chunked_csv_fixture(short_line = 15L); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  csv <- file.path(s$fx$src, "c.csv")
  header <- c("ID", "TAG", "VALUE")
  opts <- list(Delimiter = ",")
  first <- .read_delimited_chunk(csv, 0L, 10L, header, reader_options = opts, exact_rows = TRUE)
  expect_identical(first$ID, 1:10)
  out <- utils::capture.output(err <- tryCatch(
    .read_delimited_chunk(csv, 10L, 10L, header, reader_options = opts, exact_rows = TRUE),
    error = function(e) conditionMessage(e)))
  expect_match(err, "Delimited structure error|row count mismatch")
  #### A malformed record inside a batch is a structural error: it fails at once, with no pause.
  expect_identical(sum(grepl("[IO RETRY] Attempt ", out, fixed = TRUE)), 0L)
  expect_length(ls(.delimited_cursors), 0L)   # the failed read leaves no open cursor
})

fixed_sps_lines <- c("DATA LIST FILE = 'F.ASC' FIXED /", "    ID    1-  4", "    TAG   5-  8 (A)", "    VAL   9- 12.")
fixed_fixture <- function(n = 25L, short_line = NA_integer_) {
  fx <- new_repo_fixture()
  sps <- file.path(fx$root, "f.SPS")
  writeLines(fixed_sps_lines, sps)
  lines <- sprintf("%4d%-4s%4d", seq_len(n), sprintf("t%d", seq_len(n) %% 10L), seq_len(n) * 2L)
  if (!is.na(short_line)) lines[short_line] <- substr(lines[short_line], 1L, 9L)
  con <- file(file.path(fx$src, "F.ASC"), open = "wb")
  writeBin(charToRaw(paste0(paste(lines, collapse = "\r\n"), "\r\n")), con)
  close(con)
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Chunked", Path = "F.ASC", FileType = "asc",
                  PartitionKey = "year", PartitionValue = "2019",
                  ReaderOptions = jsonlite::toJSON(list(LayoutFile = sps), auto_unbox = TRUE))
  list(fx = fx, M = M)
}

test_that("a fixed-width batch with a cut-off record is re-read from its byte offset and loads complete", {
  s <- fixed_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  calls <- 0L
  restore <- mock_repoquet(".cursor_take", function(original) function(cursor, ...) {
    out <- original(cursor, ...)
    if (nzchar(out$Text)) {
      calls <<- calls + 1L
      if (calls == 2L) out$Text <- sub("\n", "", substr(out$Text, 1L, nchar(out$Text) - 1L), fixed = TRUE)
    }
    out
  }, via = ".stream_asc_records")
  r <- run_loader(s$fx, s$M, "REG", IORetryPauseSeconds = 0)
  restore()
  expect_length(r$checkpoint, 1L)
  expect_true(any(grepl("[IO RETRY] Attempt 2/3 after 0s: fixed-width read F.ASC from line", r$output, fixed = TRUE)))
  expect_equal(chunked_ids(s$fx), 1:25)
})

test_that("an early end of the fixed-width stream is detected against the expected rows and retried", {
  s <- fixed_fixture(); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  calls <- 0L
  restore <- mock_repoquet(".cursor_take", function(original) function(cursor, ...) {
    calls <<- calls + 1L
    if (calls == 3L) { cursor$eof <- TRUE; cursor$buf <- raw(0); return(list(Text = "", Rows = 0L)) }
    original(cursor, ...)
  }, via = ".stream_asc_records")
  r <- run_loader(s$fx, s$M, "REG", IORetryPauseSeconds = 0)
  restore()
  expect_length(r$checkpoint, 1L)
  expect_true(any(grepl("row count mismatch: F.ASC ended at record", r$output)))
  expect_equal(chunked_ids(s$fx), 1:25)
})

test_that("a fixed-width file with a record of the wrong length fails clearly after the retries", {
  old <- no_pause(); on.exit(options(old), add = TRUE)
  s <- fixed_fixture(short_line = 13L); on.exit(unlink(s$fx$root, recursive = TRUE), add = TRUE)
  asc <- file.path(s$fx$src, "F.ASC")
  opts <- list(LayoutFile = file.path(s$fx$root, "f.SPS"))
  delivered <- 0L
  out <- utils::capture.output(err <- tryCatch(
    .stream_asc_records(asc, opts, chunk_size = 10L, callback = function(df) delivered <<- delivered + nrow(df),
                        expected_rows = 25),
    error = function(e) conditionMessage(e)))
  expect_match(err, "F.ASC line 13 has 9 character\\(s\\), but layout f.SPS defines 12-character records")
  expect_identical(sum(grepl("[IO RETRY] Attempt ", out, fixed = TRUE)), 2L)   # attempts 2 and 3
  expect_identical(delivered, 10L)   # the chunk before the bad record was delivered once, never twice
})

test_that("a retry reopens at the recorded batch offset instead of reading again from the start", {
  root <- tempfile("seek_"); dir.create(root); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  csv <- file.path(root, "a.csv")
  n <- 200L
  body <- paste0("ID,V\n", paste(sprintf("%d,%d", seq_len(n), seq_len(n)), collapse = "\n"), "\n")
  con <- file(csv, open = "wb"); writeBin(charToRaw(body), con); close(con)
  opts <- .resolve_delimited_reader_options(csv, list(Delimiter = ","))
  first <- .delimited_chunk_batch(csv, 0L, 50L, opts, batch_rows = 50L)
  second <- .delimited_chunk_batch(csv, 50L, 50L, opts, batch_rows = 50L)
  cursor <- get(normalizePath(csv, winslash = "/"), envir = .delimited_cursors)
  expect_true(all(c(0, 50, 100) %in% cursor$po))
  expect_identical(cursor$pp[match(50, cursor$po)],
                   nchar(paste0("ID,V\n", paste(sprintf("%d,%d", 1:50, 1:50), collapse = "\n"), "\n")) + 0)
  #### A forced reopen at record 50 lands on record 51 without re-reading the first 50.
  again <- .delimited_chunk_batch(csv, 50L, 10L, opts, batch_rows = 10L, reopen = TRUE)
  again_text <- if (!is.null(again$Raw)) rawToChar(again$Raw) else again$Text
  expect_identical(strsplit(again_text, "\n", fixed = TRUE)[[1]][1], "51,51")
  .close_chunk_cursors(csv)
})

#### An early end of file, detected against the known file size ####

test_that("an end of file before the file's known size is a premature end, not the end of the data", {
  root <- tempfile("premature_"); dir.create(root); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  csv <- file.path(root, "a.csv")
  body <- paste0("ID,V\n", paste(sprintf("%d,%d", 1:100, 1:100), collapse = "\n"), "\n")
  con <- file(csv, open = "wb"); writeBin(charToRaw(body), con); close(con)
  #### A cursor that has read nothing yet (no header to skip, no BOM check), whose file is reported larger.
  cursor <- .cursor_new(csv, normalizePath(csv, winslash = "/"), "ISO-8859-1", ",", 0L, register = FALSE)
  on.exit(try(close(cursor$con), silent = TRUE), add = TRUE)
  cursor$size <- cursor$size + 40      # the share reported more bytes than it delivered
  expect_error(.cursor_take(cursor, 1000, 1e6, as_raw = TRUE), "premature end of file: a.csv ended after")
  #### The same read against the true size is a normal end of file.
  cursor2 <- .cursor_new(csv, normalizePath(csv, winslash = "/"), "UTF-8", ",", 1L, register = FALSE)
  on.exit(try(close(cursor2$con), silent = TRUE), add = TRUE)
  taken <- .cursor_take(cursor2, 1000, 1e6, as_raw = TRUE)
  expect_identical(taken$Rows, 100L)
  expect_true(cursor2$eof)
})

test_that("a connection that ends early is retried from the batch offset even when no row count is known", {
  old <- no_pause(); on.exit(options(old), add = TRUE)
  root <- tempfile("shortconn_"); dir.create(root); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  csv <- file.path(root, "a.csv")
  n <- 60L
  body <- paste0("ID,TAG\n", paste(sprintf("%d,t%d", seq_len(n), seq_len(n)), collapse = "\n"), "\n")
  con <- file(csv, open = "wb"); writeBin(charToRaw(body), con); close(con)
  cut <- file.path(root, "cut.csv")
  con <- file(cut, open = "wb"); writeBin(charToRaw(substr(body, 1L, floor(nchar(body) * 0.7))), con); close(con)
  opened <- 0L
  restore <- mock_repoquet(".binary_open", function(original) function(path) {
    opened <<- opened + 1L
    if (opened == 1L) original(cut) else original(path)   # only the first connection is cut short
  }, via = ".read_delimited_chunk")
  out <- utils::capture.output(
    got <- .read_delimited_chunk(csv, 0L, n, c("ID", "TAG"), reader_options = list(Delimiter = ","),
                                 exact_rows = FALSE))
  restore()
  expect_equal(got$ID, seq_len(n))   # complete, with no rows silently missing
  expect_true(any(grepl("premature end of file", out, fixed = TRUE)))
  expect_true(any(grepl("[IO RETRY] Attempt 2/", out, fixed = TRUE)))
  expect_length(ls(.delimited_cursors), 0L)
})

#### The end of a file is remembered, so a read at the end does not rescan the source ####

test_that("a read at the end of a file is answered from the remembered end, not by rescanning", {
  root <- tempfile("eofmark_"); dir.create(root); on.exit(unlink(root, recursive = TRUE), add = TRUE)
  csv <- file.path(root, "a.csv")
  write_body <- function(n) {
    body <- paste0("ID,V\n", paste(sprintf("%d,%d", seq_len(n), seq_len(n)), collapse = "\n"), "\n")
    con <- file(csv, open = "wb"); writeBin(charToRaw(body), con); close(con)
  }
  write_body(100L)
  key <- normalizePath(csv, winslash = "/")
  opened <- 0L
  restore <- mock_repoquet(".cursor_new", function(original) function(...) {
    opened <<- opened + 1L
    original(...)
  }, via = ".delimited_chunk_batch")
  on.exit(restore(), add = TRUE)
  read <- function(offset, n) .read_delimited_chunk(csv, offset, n, c("ID", "V"),
                                                    reader_options = list(Delimiter = ","), exact_rows = FALSE)
  #### A partial last batch asks for more records after the end: one cursor, not a second rescan.
  last <- read(90L, 50L)
  expect_equal(last$ID, 91:100)
  expect_identical(opened, 1L)
  expect_length(ls(.delimited_cursors), 0L)
  expect_identical(.delimited_eof_marks[[key]]$records, 100)
  #### The probe past the end (what the streaming writer sends) opens nothing.
  probe <- read(100L, 50L)
  expect_identical(nrow(probe), 0L)
  expect_identical(opened, 1L)
  #### A read before the end still works from a new cursor.
  expect_equal(read(10L, 5L)$ID, 11:15)
  expect_identical(opened, 2L)
  #### A file that grew is not taken for the old end.
  write_body(120L)
  grown <- read(100L, 50L)
  expect_equal(grown$ID, 101:120)
  #### An exact read still reports a short file.
  expect_error(.read_delimited_chunk(csv, 120L, 5L, c("ID", "V"), reader_options = list(Delimiter = ","),
                                     exact_rows = TRUE), "row count mismatch")
  .close_chunk_cursors(csv)
  expect_null(.delimited_eof_marks[[key]])
})
