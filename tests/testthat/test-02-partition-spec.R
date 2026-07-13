test_that("partition specs resolve defaults, custom keys, and nesting", {
  expect_identical(partition_spec_for_row(data.frame(Path = "f", Year = 2019))$dir, "year=2019")
  expect_identical(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "year",
                                                     PartitionValue = "2019"))$dir, "year=2019")
  s <- partition_spec_for_row(data.frame(Path = "f", Year = 1,
                                         PartitionKey = "SITE", PartitionValue = "MGH General"))
  expect_identical(s$keys, "SITE")
  expect_identical(s$dir, "site=MGH_General")   # sanitized space, lowercase dir key
  expect_identical(partition_spec_for_row(data.frame(Path = "f", Year = 1,
                                                     PartitionKey = "SITE;YEAR",
                                                     PartitionValue = "MGH;2019"))$dir,
                   "site=MGH/year=2019")
})

test_that("invalid partition specs error clearly", {
  expect_error(partition_spec_for_row(data.frame(Path = "f", Year = 1,
                                                 PartitionKey = "SITE", PartitionValue = NA)),
               "only the default YEAR partition")
  expect_error(partition_spec_for_row(data.frame(Path = "f", Year = 1,
                                                 PartitionKey = "SITE;YEAR", PartitionValue = "MGH")),
               "disagree on the number of levels")
  expect_error(partition_spec_for_row(data.frame(Path = "f")),
               "neither a PartitionValue nor a Year")
  expect_error(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "../SITE",
                                                 PartitionValue = "MGH")),
               "Invalid PartitionKey")
  expect_error(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "SITE;site",
                                                 PartitionValue = "A;B")),
               "duplicate level")
})

test_that("all rows of a table must share one partition key set", {
  ok <- data.frame(Database = "D", TableName = "T", Path = c("a", "b"), Year = 1:2,
                   PartitionKey = "SITE", PartitionValue = c("A", "B"))
  expect_identical(table_partition_keys(ok), "SITE")
  mixed <- data.frame(Database = "D", TableName = "T", Path = c("a", "b"), Year = 1:2,
                      PartitionKey = c("SITE", NA), PartitionValue = c("A", NA))
  expect_error(table_partition_keys(mixed), "mixes partition key sets")
})
