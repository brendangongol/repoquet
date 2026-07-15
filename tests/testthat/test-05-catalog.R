catalog_fixture <- function() {
  fx <- new_repo_fixture()
  dir.create(file.path(fx$root, "DEMO"))
  data.table::fwrite(data.table::data.table(KEY_D = c(1, 2), AGE = c(30, 40), DX1 = c("A10", "B20")),
                     file.path(fx$root, "DEMO", "d_2019.csv"))
  data.table::fwrite(data.table::data.table(KEY_D = c(3, 4), AGE = c(50, 60), DX1 = c("C30", "D40")),
                     file.path(fx$root, "DEMO", "d_2020.csv"))
  fx$mdt <- data.frame(Database = "DEMO", MDBDir = "DEMO", TableName = "Core",
                       Path = c("d_2019.csv", "d_2020.csv"), FileType = "csv",
                       PartitionKey = "year", PartitionValue = c("2019", "2020"))
  fx
}

build_cat <- function(fx, ...) {
  out <- utils::capture.output(res <- BuildRepositoryCatalog(fx$mdt, DBLoad = "DEMO",
      MasterDBPath = fx$root, n_workers = 1, SchemaRegistryPath = fx$reg, TableSchemaPath = fx$ts, ...))
  res
}

test_that("generic catalog derives identifier types from the data", {
  fx <- catalog_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  cat1 <- build_cat(fx)
  expect_identical(cat1$col_classes$DEMO$Core$KEY_D, "integer")
})

test_that("manual CanonicalType overrides survive a preflight re-run", {
  fx <- catalog_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  build_cat(fx)
  ts <- data.table::fread(fx$ts)
  ts[Column == "AGE", `:=`(CanonicalType = "integer", Source = "manual")]
  data.table::fwrite(ts, fx$ts)
  cat2 <- build_cat(fx)
  row <- cat2$table_schema[cat2$table_schema$Column == "AGE", ]
  expect_identical(row$CanonicalType, "integer")
  expect_identical(row$Source, "manual")
})

test_that("invalid CanonicalType strings warn and fall back to character", {
  fx <- catalog_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  build_cat(fx)
  ts <- data.table::fread(fx$ts)
  ts[Column == "DX1", CanonicalType := "strng"]
  data.table::fwrite(ts, fx$ts)
  out <- utils::capture.output(back <- load_table_schema_catalog(fx$ts))
  expect_true(any(grepl("CATALOG WARNING", out)))
  expect_identical(back$col_classes$DEMO$Core$DX1, "character")
  expect_error(load_table_schema_catalog(fx$ts, strict = TRUE), "Invalid CanonicalType")
})

test_that("invalid registry CanonicalType values fail before loading", {
  fx <- catalog_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  reg <- build_default_schema_registry("hcup")
  reg$CanonicalType[1] <- "strng"
  data.table::fwrite(reg, fx$reg)
  expect_error(load_schema_registry(fx$reg), "invalid CanonicalType")
})

test_that("unregistered identifier-like merge keys are still validated", {
  ts <- data.table::data.table(
    Database = c("A", "B"), TableName = c("Core", "Core"),
    DuckDBTable = c("A_Core", "B_Core"), Column = "PATIENT_ID",
    CanonicalType = c("character", "integer"), Role = "inferred")
  expect_error(ValidateSchemaMergeKeys(ts, strict = TRUE), "merge-key validation failed")
})

test_that("merge carries forward databases the fresh pass did not touch", {
  fx <- catalog_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  cat1 <- build_cat(fx)
  other <- data.table::data.table(Database = "NIS", TableName = "Core", DuckDBTable = "NIS_Core",
                                  Column = "KEY_NIS", CanonicalType = "character", Source = "resolved")
  merged <- merge_table_schema_catalog(cat1$table_schema, other)
  expect_identical(nrow(merged[merged$Database == "NIS", ]), 1L)
})

test_that("catalog-mode schema build honors manual types and flags new columns", {
  fx <- catalog_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  build_cat(fx)
  ts <- data.table::fread(fx$ts)
  ts[Column == "AGE", `:=`(CanonicalType = "integer", Source = "manual")]
  data.table::fwrite(ts, fx$ts)
  cat2 <- build_cat(fx)
  # new column appears in a new year's file
  data.table::fwrite(data.table::data.table(KEY_D = 5, AGE = 70, DX1 = "E50", NEWCOL = 9.5),
                     file.path(fx$root, "DEMO", "d_2021.csv"))
  mdt2 <- rbind(fx$mdt, data.frame(Database = "DEMO", MDBDir = "DEMO", TableName = "Core",
                                   Path = "d_2021.csv", FileType = "csv",
                                   PartitionKey = "year", PartitionValue = "2021"))
  out <- utils::capture.output(so <- BuildRepositorySchema(MDTSelect = mdt2, MasterDBPath = fx$root,
      Database = "DEMO", n_workers = 1, SchemaRegistryPath = fx$reg,
      write_catalog = FALSE, known_col_classes = cat2$col_classes$DEMO))
  expect_identical(so$col_classes$Core$AGE, "integer")
  expect_identical(so$col_classes$Core$NEWCOL, "numeric")
  expect_true(any(grepl("absent from the schema catalog", out)))
  expect_identical(so$table_schema[so$table_schema$Column == "NEWCOL", ]$Source, "inferred_at_load")
})
