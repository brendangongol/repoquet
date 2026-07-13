test_that("RunPreflight = FALSE skips the loader's internal validation pass", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "a.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG", RunPreflight = FALSE)
  expect_true(any(grepl("Skipped inside loader", r$output)))
  expect_length(r$checkpoint, 1L)
})

test_that("state snapshots are taken at loader start and retention prunes old ones", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "a.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  run_loader(fx, M, "REG")                       # first run: nothing to snapshot yet
  r2 <- run_loader(fx, M, "REG", completed = load_checkpoint(fx$cp))
  snaps <- list.dirs(file.path(fx$root, "StateBackups"), recursive = FALSE)
  expect_gte(length(snaps), 1L)
  expect_true(any(file.exists(file.path(snaps[length(snaps)], basename(fx$cp)))))
  expect_true(any(grepl("SNAPSHOT", r2$output)))
  # retention: plant fake old snapshots, next snapshot call prunes to keep_last
  bdir <- file.path(fx$root, "StateBackups")
  for (i in 1:25) dir.create(file.path(bdir, sprintf("20200101_%06d", i)), showWarnings = FALSE)
  out <- utils::capture.output(
    snapshot_repository_state(CheckpointPath = fx$cp, BackupDir = bdir, keep_last = 3L))
  remaining <- list.dirs(bdir, recursive = FALSE)
  expect_lte(length(remaining), 3L)
})

test_that("BuildRepositoryCatalog refuses to run while the writer lock is held", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2)), file.path(fx$src, "a_2019.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "a_2019.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  lockpath <- file.path(fx$root, ".repository.lock")
  out <- utils::capture.output(held <- acquire_repository_lock(lockpath, owner_note = "held"))
  err <- tryCatch(BuildRepositoryCatalog(M, DBLoad = "REG", MasterDBPath = fx$root, n_workers = 1,
                                         SchemaRegistryPath = fx$reg, TableSchemaPath = fx$ts,
                                         LockPath = lockpath),
                  error = function(e) e)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "locked by another")
  out <- utils::capture.output(release_repository_lock(held))
})

dictionary_fixture <- function() {
  fx <- new_repo_fixture()
  dir.create(file.path(fx$root, "D"))
  #### Note: SPSS has no string NULL -- a character NA round-trips as "" and ####
  #### correctly counts as out-of-domain, so NULL handling is tested via the ####
  #### numeric DIED column instead.                                          ####
  haven::write_sav(data.frame(
    DIED = haven::labelled(c(0, 1, 1, NA), labels = c("Did not die" = 0, "Died" = 1),
                           label = "Died during hospitalization"),
    SEX  = haven::labelled(c("M", "F", "X", "M"), labels = c(Male = "M", Female = "F"),
                           label = "Patient sex"),
    LOS  = haven::labelled(c(3, 5, 7, 9), label = "Length of stay")),
    file.path(fx$root, "D", "d_2020.sav"))
  fx$mdt <- data.frame(Database = "D", MDBDir = "D", TableName = "T", Path = "d_2020.sav",
                       FileType = "sav", PartitionKey = "year", PartitionValue = "2020")
  out <- utils::capture.output(BuildRepositoryCatalog(fx$mdt, DBLoad = "D", MasterDBPath = fx$root,
      n_workers = 1, SchemaRegistryPath = fx$reg, TableSchemaPath = fx$ts))
  fx
}

test_that("search_labels finds variables by label, column, and value text", {
  fx <- dictionary_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  hits <- search_labels("hospitalization", TableSchemaPath = fx$ts)
  expect_identical(hits$Column, "DIED")
  expect_identical(hits$CanonicalType, "integer")     # registry-resolved type merged in
  hits2 <- search_labels("^LOS$", TableSchemaPath = fx$ts, search_in = "column")
  expect_identical(hits2$VariableLabel, "Length of stay")
  hits3 <- search_labels("Female", TableSchemaPath = fx$ts, search_in = "values")
  expect_identical(hits3$Column, "SEX")
  expect_identical(nrow(search_labels("no_such_thing_anywhere", TableSchemaPath = fx$ts)), 0L)
})

test_that("parse_value_label_codes handles domains, embedded separators, and truncation", {
  expect_identical(parse_value_label_codes("0 = Did not die; 1 = Died"), c("0", "1"))
  # a label containing "; " contributes no phantom codes
  expect_identical(parse_value_label_codes("1 = Yes; complicated; 2 = No"), c("1", "2"))
  expect_null(parse_value_label_codes("0 = A; 1 = B ..."))   # truncated -> domain unknowable
  expect_identical(parse_value_label_codes(NA), character(0))
})

test_that("validate_against_dictionary reports out-of-domain values per column", {
  skip_if_not_installed("duckdb")
  fx <- dictionary_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  run_loader(fx, fx$mdt, "D")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  sr <- load_schema_registry(fx$reg, create_if_missing = FALSE)
  out <- utils::capture.output(register_parquet_view(con, fx$pq, "D_T", schema_registry = sr))
  out <- utils::capture.output(chk <- validate_against_dictionary(con, TableSchemaPath = fx$ts))
  sex <- chk[chk$Column == "SEX", ]
  expect_equal(sex$OutOfDomain, 1)              # the "X"
  died <- chk[chk$Column == "DIED", ]
  expect_equal(died$OutOfDomain, 0)             # 0/1 all in domain (numeric compare)
  expect_equal(died$NNull, 1)                   # the numeric NA
  expect_false("LOS" %in% chk$Column)           # unlabeled -> skipped
})

test_that("scan_for_new_source_files proposes reviewable MDT rows", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = 1), file.path(fx$src, "REG_2019_Core.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Core",
                  Path = "REG_2019_Core.csv", FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2019")
  # a guessable new release, an unguessable stray, and a non-loader file
  # (.txt is now a supported delimited FileType, so use .log for "ignored")
  data.table::fwrite(data.table::data.table(AGE = 2), file.path(fx$src, "REG_2021_Core.csv"))
  data.table::fwrite(data.table::data.table(AGE = 3), file.path(fx$src, "strange_file.csv"))
  writeLines("x", file.path(fx$src, "notes.log"))
  out_path <- file.path(fx$root, "NewFiles.csv")
  outp <- utils::capture.output(cand <- scan_for_new_source_files(fx$root, M, OutputPath = out_path))
  expect_identical(nrow(cand), 2L)                          # .log ignored, known file ignored
  good <- cand[cand$Path == "REG_2021_Core.csv", ]
  expect_identical(good$TableName, "Core")
  expect_identical(good$PartitionValue, "2021")
  expect_false(good$NeedsReview)
  stray <- cand[cand$Path == "strange_file.csv", ]
  expect_true(stray$NeedsReview)
  expect_true(is.na(stray$TableName))
  expect_true(file.exists(out_path))
})

test_that("weighted helpers compute correct point estimates with missing data", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  df <- data.frame(YEAR = c(2019, 2019, 2019, 2020, 2020),
                   DISCWT = c(2, 3, NA, 4, 5),
                   LOS = c(10, 20, 99, NA, 8))
  duckdb::dbWriteTable(con, "T", df)
  cnt <- hcup_weighted_count(con, "T", by = "YEAR")
  expect_identical(cnt$n_weighted, c(5, 9))                    # NA weight excluded
  expect_identical(cnt$n_missing_weight, c(1, 0))
  m <- hcup_weighted_mean(con, "T", value_col = "LOS", by = "YEAR")
  expect_equal(m$mean_weighted[1], (2 * 10 + 3 * 20) / (2 + 3))  # NA-weight row's LOS ignored
  expect_equal(m$mean_weighted[2], (5 * 8) / 5)                  # NA LOS excluded from denominator
  expect_identical(m$n_value_missing, c(0, 1))
  filt <- hcup_weighted_count(con, "T", where = "LOS >= 10")
  expect_equal(as.numeric(filt$n_unweighted), 3)
  expect_error(hcup_weighted_mean(con, "T", value_col = "NOPE"), "not found")
})
