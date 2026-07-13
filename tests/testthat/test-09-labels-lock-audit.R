test_that("SPSS variable and value labels are harvested into the catalog", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  dir.create(file.path(fx$root, "D"))
  haven::write_sav(data.frame(
    AGE  = haven::labelled(c(30, 40), label = "Age in years at admission"),
    DIED = haven::labelled(c(0, 1), labels = c("Did not die" = 0, "Died" = 1),
                           label = "Died during hospitalization")),
    file.path(fx$root, "D", "d_2020.sav"))
  lab <- harvest_sav_labels(file.path(fx$root, "D", "d_2020.sav"), n_workers = 1)
  expect_identical(lab[lab$Column == "AGE", ]$VariableLabel, "Age in years at admission")
  expect_match(lab[lab$Column == "DIED", ]$ValueLabels, "0 = Did not die; 1 = Died")
  M <- data.frame(Database = "D", MDBDir = "D", TableName = "T", Path = "d_2020.sav",
                  FileType = "sav", PartitionKey = "year", PartitionValue = "2020")
  ts_x <- file.path(fx$root, "TS.xlsx")
  out <- utils::capture.output(BuildRepositoryCatalog(M, DBLoad = "D", MasterDBPath = fx$root,
      n_workers = 1, SchemaRegistryPath = fx$reg, TableSchemaPath = ts_x))
  expect_true("Labels" %in% openxlsx::getSheetNames(ts_x))
  lab2 <- load_label_catalog(ts_x)
  expect_identical(lab2[lab2$Column == "DIED", ]$VariableLabel, "Died during hospitalization")
  # a catalog rewrite that does not harvest must preserve the dictionary
  sch <- load_table_schema_catalog(ts_x)
  write_table_schema_catalog(sch$table_schema, ts_x)
  expect_false(is.null(load_label_catalog(ts_x)))
})

test_that("the repository lock is exclusive, heartbeatable, and stale-recoverable", {
  tmp <- tempfile(); dir.create(tmp); on.exit(unlink(tmp, recursive = TRUE))
  lp <- file.path(tmp, ".repository.lock")
  out <- utils::capture.output({
    l1 <- acquire_repository_lock(lp, owner_note = "run1")
    err <- tryCatch(acquire_repository_lock(lp, owner_note = "run2"), error = function(e) e)
  })
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "locked by another")
  out <- utils::capture.output(ok <- release_repository_lock(l1))
  expect_true(ok)
  expect_false(dir.exists(lp))
  # stale takeover
  out <- utils::capture.output(l3 <- acquire_repository_lock(lp, stale_minutes = 720))
  Sys.setFileTime(file.path(lp, "owner.txt"), Sys.time() - 800 * 60)
  out <- utils::capture.output(l4 <- acquire_repository_lock(lp, stale_minutes = 720, owner_note = "takeover"))
  expect_s3_class(l4, "repository_lock")
  out <- utils::capture.output(ok2 <- release_repository_lock(lp, force = TRUE))
  expect_true(ok2)
})

test_that("the loader takes the lock and a concurrent run is refused", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = "a.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")
  expect_true(any(grepl("[LOCK] Acquired", r$output, fixed = TRUE)))
  expect_true(any(grepl("[LOCK] Released", r$output, fixed = TRUE)))
  expect_false(dir.exists(file.path(fx$root, ".repository.lock")))
  lockpath <- file.path(fx$root, ".repository.lock")
  out <- utils::capture.output(held <- acquire_repository_lock(lockpath, owner_note = "held"))
  err <- tryCatch(run_loader(fx, M, "REG", LockPath = lockpath), error = function(e) e)
  expect_s3_class(err, "error")
  expect_match(conditionMessage(err), "locked by another")
  out <- utils::capture.output(release_repository_lock(held))
})

test_that("audit_repository is silent on a clean repo and detects each divergence", {
  skip_if_not_installed("duckdb")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "a_2019.csv"))
  data.table::fwrite(data.table::data.table(AGE = c(3, 4), CODE = c("Z3", "Z4")), file.path(fx$src, "b_2020.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T",
                  Path = c("a_2019.csv", "b_2020.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2019", "2020"))
  run_loader(fx, M, "REG")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  a0 <- audit_repository(M, fx$pq, fx$cp, fx$mf, con = con, verbose = FALSE)
  expect_identical(nrow(a0$issues), 0L)
  # sabotage: lose a written file, plant an orphan, add a stale checkpoint entry
  wf <- list.files(file.path(fx$pq, "REG_T", "year=2019"), pattern = "parquet$", full.names = TRUE)[1]
  file.remove(wf)
  arrow::write_parquet(data.frame(x = 1), file.path(fx$pq, "REG_T", "year=2020", "orphan_file.parquet"))
  saveRDS(c(load_checkpoint(fx$cp), "REG||Gone||1999||REG||gone.csv"), fx$cp)
  a1 <- audit_repository(M, fx$pq, fx$cp, fx$mf, con = con, verbose = FALSE)
  expect_true(all(c("manifest_missing_file", "orphan_parquet", "stale_checkpoint",
                    "checkpointed_no_output", "duckdb_count_mismatch") %in% a1$issues$Check))
  expect_match(a1$orphan_parquet$ParquetPath[1], "orphan_file")
})
