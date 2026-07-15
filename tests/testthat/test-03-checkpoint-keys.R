test_that("checkpoint keys encode explicit partition keys and values", {
  mdt <- data.frame(Database = "NIS", TableName = "Core", MDBDir = "NIS",
                    Path = c("NIS_2019_Core.sav", "NIS_2020_Core.sav"),
                    PartitionKey = "year", PartitionValue = c("2019", "2020"))
  expect_identical(repository_checkpoint_key(mdt)[1],
                   "NIS||Core||YEAR=2019||NIS||NIS_2019_Core.sav")
})

test_that("value-only checkpoint entries remain readable during migration", {
  mdt <- data.frame(Database = "NIS", TableName = "Core", MDBDir = "NIS",
                    Path = "NIS_2019_Core.sav",
                    PartitionKey = "year", PartitionValue = "2019")
  old_key <- "NIS||Core||2019||NIS||NIS_2019_Core.sav"
  expect_true(all(checkpoint_completed_mask(mdt, old_key)))
  expect_false(all(checkpoint_completed_mask(mdt, old_key, accept_legacy = FALSE)))
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
