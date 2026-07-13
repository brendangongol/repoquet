test_that("physical partition columns must agree with the workbook value", {
  # unit level: canonicalization tolerates numeric years, NAs, and sanitized strings
  ok_num <- data.table::data.table(YEAR = c(2019, 2019, NA))
  expect_true(validate_partition_column_values(ok_num, "YEAR", "2019", "f"))
  ok_site <- data.table::data.table(SITE = c("MGH General", NA))
  expect_true(validate_partition_column_values(ok_site, "SITE", "MGH_General", "f"))
  bad <- data.table::data.table(YEAR = c(2019, 2018))
  expect_error(validate_partition_column_values(bad, "YEAR", "2019", "f"),
               "disagree with the workbook partition value")
  # absent column or no expected values: nothing to check
  expect_true(validate_partition_column_values(data.table::data.table(AGE = 1), "YEAR", "2019", "f"))
  expect_true(validate_partition_column_values(bad, "YEAR", NULL, "f"))
})

test_that("a mislabeled workbook year fails the file instead of relabeling rows", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(YEAR = c(2018, 2018), AGE = c(1, 2)),
                     file.path(fx$src, "mislabeled.csv"))
  data.table::fwrite(data.table::data.table(YEAR = c(2020, 2020), AGE = c(3, 4)),
                     file.path(fx$src, "correct.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T",
                  Path = c("mislabeled.csv", "correct.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2019", "2020"))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)   # only the correct file completes
  expect_true(any(grepl("disagree with the workbook partition value", r$output)))
  expect_false(dir.exists(file.path(fx$pq, "REG_T", "year=2019")) &&
                 length(list.files(file.path(fx$pq, "REG_T", "year=2019"))) > 0)
  expect_true(dir.exists(file.path(fx$pq, "REG_T", "year=2020")))
})

test_that("an invalid PartitionBy string errors immediately", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = 1), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "a.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  #### match.arg fires as the loader's first statement -- before the lock,   ####
  #### snapshots, preflight, or any file I/O.                                ####
  expect_error(ParquetBackEndCreate(MDT = M, DBLoad = "REG", MasterDBPath = fx$root,
                                    completed_checkpoint = character(0),
                                    CheckpointPath = fx$cp, ParquetBasePath = fx$pq,
                                    PartitionBy = "banana", RAMThreshold = 30,
                                    LogPath = fx$log),
               "should be one of")
  expect_error(read_fn(path = "a.csv", MDTSelect = M, MasterDBPath = fx$root, reader = "csv",
                       PartitionBy = "banana", SAV_ROW_THRESHOLD = 1L, RAMThreshold = 1,
                       SAV_CHUNK_SIZE = 1L),
               "should be one of")
})

test_that("blank values in required MDT columns are caught by the preflight", {
  b <- data.frame(Database = "R1", MDBDir = c("R1", "  "), TableName = "T",
                  Path = c("a.csv", "b.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2019", "2020"))
  out <- utils::capture.output(iss <- ValidateMDTPreflight(b, strict = FALSE))
  expect_true("blank_required_values" %in% iss$Check)
  expect_identical(iss[iss$Check == "blank_required_values", ]$Severity, "error")
})

test_that("the manifest records general partition provenance", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2)), file.path(fx$src, "r_2020.csv"))
  data.table::fwrite(data.table::data.table(AGE = c(3, 4)), file.path(fx$src, "r_mgh.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = c("Yr", "Ev"),
                  Path = c("r_2020.csv", "r_mgh.csv"), FileType = "csv",
                  PartitionKey = c("year", "SITE"), PartitionValue = c("2020", "MGH"))
  run_loader(fx, M, "REG")
  mf <- data.table::fread(fx$mf)
  expect_true(all(c("PartitionKey", "PartitionValue") %in% names(mf)))
  yr <- mf[mf$TableName == "Yr" & mf$Status == "written", ]
  expect_identical(unique(yr$PartitionKey), "YEAR")
  expect_identical(unique(yr$PartitionValue), "2020")
  ev <- mf[mf$TableName == "Ev" & mf$Status == "written", ]
  expect_identical(unique(ev$PartitionKey), "SITE")
  expect_identical(unique(ev$PartitionValue), "MGH")
  expect_true(all(is.na(ev$Year)))   # Year stays a derived convenience
})

test_that("legacy manifests without PartitionValue still dedupe by Year", {
  tmp <- tempfile(); dir.create(tmp); on.exit(unlink(tmp, recursive = TRUE))
  mfp <- file.path(tmp, "mf.csv")
  #### Simulate a pre-generalization manifest row (no PartitionKey/Value). ####
  old_row <- data.table::data.table(run_time = "2026-01-01 00:00:00", Database = "D", TableName = "T",
                                    DuckDBTable = "D_T", Year = 2019L, SourcePath = "a.sav",
                                    ParquetPath = "p/a.parquet", NRows = 5L, SchemaHash = "x",
                                    Status = "written", Notes = "single_file")
  data.table::fwrite(old_row, mfp)
  update_parquet_manifest(mfp, Database = "D", TableName = "T", DuckDBTable = "D_T", Year = 2019,
                          SourcePath = "a.sav", ParquetPath = "p/a.parquet", NRows = 7L,
                          Status = "written", Notes = "rewritten")
  mf <- data.table::fread(mfp)
  expect_identical(nrow(mf), 1L)          # replaced, not duplicated
  expect_identical(mf$NRows, 7L)
})

test_that("coercion damage beyond the threshold fails the file; report is written", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  #### DIED is registry-forced to integer; half its values are text.        ####
  data.table::fwrite(data.table::data.table(DIED = c("0", "1", "UNKNOWN", "MISSING"), AGE = c(1, 2, 3, 4)),
                     file.path(fx$src, "dmg_2019.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "dmg_2019.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  # base R's "NAs introduced by coercion" warning is the behavior under test
  r_strict <- suppressWarnings(run_loader(fx, M, "REG", MaxCoerceNAPct = 25))
  expect_length(r_strict$checkpoint, 0L)
  expect_true(any(grepl("exceeds the coercion NA threshold", r_strict$output)))
  #### Without a threshold the file loads (warn-only), and the run report   ####
  #### aggregates the damage next to the manifest.                          ####
  r_lax <- suppressWarnings(run_loader(fx, M, "REG"))
  expect_length(r_lax$checkpoint, 1L)
  rep_path <- file.path(dirname(fx$mf), "CoercionReport.csv")
  expect_true(file.exists(rep_path))
  rep <- data.table::fread(rep_path)
  expect_true("DIED" %in% rep$Column)
  expect_equal(rep[rep$Column == "DIED", ]$NDestroyed, 2)
})

test_that("rename_checkpoint_table migrates checkpoint and manifest identities", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2)), file.path(fx$src, "a_2019.csv"))
  M_old <- data.frame(Database = "NRD", MDBDir = "REG", TableName = "CORE", Path = "a_2019.csv",
                      FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M_old, "NRD")
  expect_length(r$checkpoint, 1L)
  #### The workbook is then normalized: CORE -> Core. Without migration the ####
  #### loaded file would look pending again.                                ####
  M_new <- M_old; M_new$TableName <- "Core"
  expect_false(any(checkpoint_completed_mask(M_new, load_checkpoint(fx$cp))))
  out <- utils::capture.output(
    dry <- rename_checkpoint_table(fx$cp, M_new, "NRD", "CORE", "Core", fx$mf, DryRun = TRUE))
  expect_gte(dry$n_checkpoint_migrated, 1L)
  expect_false(any(checkpoint_completed_mask(M_new, load_checkpoint(fx$cp))))  # dry run changed nothing
  out <- utils::capture.output(
    rename_checkpoint_table(fx$cp, M_new, "NRD", "CORE", "Core", fx$mf, DryRun = FALSE))
  expect_true(all(checkpoint_completed_mask(M_new, load_checkpoint(fx$cp))))
  mf <- data.table::fread(fx$mf)
  expect_false(any(mf$TableName == "CORE"))
  expect_true(all(mf$DuckDBTable[mf$TableName == "Core"] == "NRD_Core"))
})

test_that("standalone no-catalog runs validate cross-database keys before writing", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  dir.create(file.path(fx$root, "A")); dir.create(file.path(fx$root, "B"))
  #### PATIENT_ID is an unregistered identifier: character in A, integer in ####
  #### B. Combined validation must stop the run before any Parquet exists.  ####
  data.table::fwrite(data.table::data.table(PATIENT_ID = c("A1", "A2"), AGE = c(1, 2)),
                     file.path(fx$root, "A", "a_2019.csv"))
  data.table::fwrite(data.table::data.table(PATIENT_ID = c(7L, 9L), AGE = c(3, 4)),
                     file.path(fx$root, "B", "b_2019.csv"))
  M <- data.frame(Database = c("A", "B"), MDBDir = c("A", "B"), TableName = "Core",
                  Path = c("a_2019.csv", "b_2019.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2019")
  err <- tryCatch(run_loader(fx, M, c("A", "B")), error = function(e) e)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "merge-key validation failed")
  n_parquet <- if (dir.exists(fx$pq)) length(list.files(fx$pq, pattern = "\\.parquet$", recursive = TRUE)) else 0L
  expect_identical(n_parquet, 0L)
})
