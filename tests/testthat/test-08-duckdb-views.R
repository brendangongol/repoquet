test_that("DuckDB views expose normalized types and join without casts", {
  skip_if_not_installed("duckdb")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  dir.create(file.path(fx$root, "A")); dir.create(file.path(fx$root, "B"))
  data.table::fwrite(data.table::data.table(KEY_X = c(101, 102), AGE = c(30L, 40L), DX1 = c("A10", "B20")),
                     file.path(fx$root, "A", "core_2019.csv"))
  data.table::fwrite(data.table::data.table(KEY_X = c("103", "104"), AGE = c(50.5, 60.2), DX1 = c("C30", "D40")),
                     file.path(fx$root, "A", "core_2020.csv"))
  data.table::fwrite(data.table::data.table(KEY_X = c("101", "104"), GRP = c("g1", "g2")),
                     file.path(fx$root, "A", "link_2019.csv"))
  data.table::fwrite(data.table::data.table(KEY_X = c(101, 104), SITECODE = c(7, 9)),
                     file.path(fx$root, "B", "ev_2019.csv"))
  M <- data.frame(Database = c("A", "A", "A", "B"), MDBDir = c("A", "A", "A", "B"),
                  TableName = c("Core", "Core", "Link", "Ev"),
                  Path = c("core_2019.csv", "core_2020.csv", "link_2019.csv", "ev_2019.csv"),
                  FileType = "csv", PartitionKey = "year",
                  PartitionValue = c("2019", "2020", "2019", "2019"))
  run_loader(fx, M, c("A", "B"))
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  sr <- load_schema_registry(fx$reg, create_if_missing = FALSE)
  table_schema <- load_table_schema_catalog(fx$ts, strict = TRUE)$table_schema
  out <- utils::capture.output({
    for (tb in c("A_Core", "A_Link", "B_Ev")) {
      register_parquet_view(con, fx$pq, tb, schema_registry = sr,
                            validate = TRUE, strict_validation = TRUE,
                            table_schema = table_schema)
    }
  })
  d <- DBI::dbGetQuery(con, "DESCRIBE A_Core")
  expect_identical(d$column_type[d$column_name == "KEY_X"], "VARCHAR")
  expect_identical(d$column_type[d$column_name == "AGE"], "DOUBLE")
  q1 <- DBI::dbGetQuery(con, "SELECT c.KEY_X FROM A_Core c INNER JOIN A_Link l ON c.KEY_X = l.KEY_X")
  expect_identical(sort(q1$KEY_X), c("101", "104"))
  q2 <- DBI::dbGetQuery(con, "SELECT c.KEY_X FROM A_Core c INNER JOIN B_Ev b ON c.KEY_X = b.KEY_X")
  expect_identical(sort(q2$KEY_X), c("101", "104"))
  q3 <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM A_Core WHERE YEAR >= 2020")
  expect_identical(q3$n[1], 2)

  bad_schema <- data.table::copy(table_schema)
  bad_schema[DuckDBTable == "A_Core" & Column == "AGE", CanonicalType := "integer"]
  expect_error(validate_duckdb_table(con, "A_Core", schema_registry = sr,
                                     table_schema = bad_schema, strict = TRUE),
               "catalog expected exact resolved types")
})

test_that("view registration skips (not errors) a table with zero parquet files", {
  skip_if_not_installed("duckdb")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(AGE = c(1, 2), CODE = c("Z1", "Z2")), file.path(fx$src, "full.csv"))
  writeLines("AGE,CODE", file.path(fx$src, "allempty.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = c("T", "Empt"),
                  Path = c("full.csv", "allempty.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2020", "2019"))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 2L)
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  sr <- load_schema_registry(fx$reg, create_if_missing = FALSE)
  out <- utils::capture.output({
    r1 <- register_parquet_view(con, fx$pq, "REG_T", schema_registry = sr)
    r2 <- register_parquet_view(con, fx$pq, "REG_Empt", schema_registry = sr)
  })
  expect_true(isTRUE(r1))
  expect_false(isTRUE(r2))
  expect_true("REG_T" %in% DBI::dbListTables(con))
  expect_false("REG_Empt" %in% DBI::dbListTables(con))
})
