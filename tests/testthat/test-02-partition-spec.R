test_that("partition specs resolve explicit keys and nesting", {
  expect_identical(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "year",
                                                     PartitionValue = "2019"))$dir, "year=2019")
  s <- partition_spec_for_row(data.frame(Path = "f", PartitionKey = "SITE",
                                         PartitionValue = "MGH General"))
  expect_identical(s$keys, "SITE")
  expect_identical(s$dir, "site=MGH_General")   # sanitized space, lowercase dir key
  expect_identical(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "SITE;YEAR",
                                                     PartitionValue = "MGH;2019"))$dir,
                   "site=MGH/year=2019")
})

test_that("invalid partition specs error clearly", {
  expect_error(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "SITE",
                                                 PartitionValue = NA)),
               "PartitionValue is required")
  expect_error(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "SITE;YEAR",
                                                 PartitionValue = "MGH")),
               "disagree on the number of levels")
  expect_error(partition_spec_for_row(data.frame(Path = "f")),
               "PartitionKey is required")
  expect_error(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "../SITE",
                                                 PartitionValue = "MGH")),
               "Invalid PartitionKey")
  expect_error(partition_spec_for_row(data.frame(Path = "f", PartitionKey = "SITE;site",
                                                 PartitionValue = "A;B")),
               "duplicate level")
})

test_that("all rows of a table must share one partition key set", {
  ok <- data.frame(Database = "D", TableName = "T", Path = c("a", "b"),
                   PartitionKey = "SITE", PartitionValue = c("A", "B"))
  expect_identical(table_partition_keys(ok), "SITE")
  mixed <- data.frame(Database = "D", TableName = "T", Path = c("a", "b"),
                      PartitionKey = c("SITE", "YEAR"), PartitionValue = c("A", "2020"))
  expect_error(table_partition_keys(mixed), "mixes partition key sets")
})

test_that("the writer removes only explicitly configured physical partition columns", {
  root <- tempfile("partition_writer_")
  dir.create(root)
  on.exit(unlink(root, recursive = TRUE), add = TRUE)

  year_path <- write_year_parquet(
    data.frame(YEAR = c(2020L, 2020L), VALUE = 1:2), root, "D_Year", "2020",
    "year.csv", partition_keys = "YEAR", partition_values = "2020")
  expect_false("YEAR" %in% names(arrow::read_parquet(year_path)))

  site_path <- write_year_parquet(
    data.frame(YEAR = c(2020L, 2021L), VALUE = 1:2), root, "D_Site", NA,
    "site.csv", partition_keys = "SITE", partition_values = "MGH")
  expect_true("YEAR" %in% names(arrow::read_parquet(site_path)))

  expect_error(
    write_year_parquet(
      data.frame(YEAR = 2021L, VALUE = 1L), root, "D_BadYear", "2020",
      "bad.csv", partition_keys = "YEAR", partition_values = "2020"),
    "disagree with the workbook partition value")
})
