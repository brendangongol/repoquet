test_that("checkpoint keys are bit-identical between legacy Year and PartitionValue workbooks", {
  old_mdt <- data.frame(Database = "NIS", TableName = "Core", Year = c(2019, 2020),
                        MDBDir = "NIS", Path = c("NIS_2019_Core.sav", "NIS_2020_Core.sav"))
  new_mdt <- data.frame(Database = "NIS", TableName = "Core", MDBDir = "NIS",
                        Path = c("NIS_2019_Core.sav", "NIS_2020_Core.sav"),
                        PartitionKey = "year", PartitionValue = c("2019", "2020"))
  expect_identical(repository_checkpoint_key(old_mdt), repository_checkpoint_key(new_mdt))
  # historical string format itself, not just internal consistency
  expect_identical(repository_checkpoint_key(new_mdt)[1],
                   "NIS||Core||2019||NIS||NIS_2019_Core.sav")
})

test_that("old checkpoint entries mark new-format rows complete (no re-ingest)", {
  old_mdt <- data.frame(Database = "NIS", TableName = "Core", Year = 2019,
                        MDBDir = "NIS", Path = "NIS_2019_Core.sav")
  new_mdt <- data.frame(Database = "NIS", TableName = "Core", MDBDir = "NIS",
                        Path = "NIS_2019_Core.sav",
                        PartitionKey = "year", PartitionValue = "2019")
  expect_true(all(checkpoint_completed_mask(new_mdt, repository_checkpoint_key(old_mdt))))
})

test_that("legacy bare-path checkpoint entries still complete unique paths", {
  mdt <- data.frame(Database = "D", TableName = "T", MDBDir = "D", Path = "only.sav",
                    PartitionKey = "year", PartitionValue = "2019")
  expect_true(checkpoint_completed_mask(mdt, "only.sav"))
})

test_that("generalized checkpoint identities include partition key names", {
  site <- data.frame(Database = "D", TableName = "T", MDBDir = "D", Path = "same.csv",
                     PartitionKey = "SITE", PartitionValue = "MGH")
  facility <- site; facility$PartitionKey <- "FACILITY"
  expect_false(identical(repository_checkpoint_key(site), repository_checkpoint_key(facility)))
  # Checkpoints written before key names were added remain valid during migration.
  expect_true(checkpoint_completed_mask(site, repository_checkpoint_legacy_key(site)))
})
