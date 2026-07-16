test_that("promote_types resolves drift conservatively", {
  expect_identical(promote_types(c("integer", "numeric")), "numeric")
  expect_identical(promote_types(c("character", "integer")), "character")
  expect_identical(promote_types(c("logical", "integer")), "integer")
  # a pure-logical vote means every sampled row was NA (fread defaults all-NA
  # columns to logical) -- trusting it corrupts real values encountered later
  # in a full-file read, so fall back to the safe universal type instead
  expect_identical(promote_types("logical"), "character")
  expect_identical(promote_types(c("logical", "logical")), "character")
  expect_identical(promote_types(character(0)), "character")
})

test_that("enforce_col_classes coerces and warns when values are destroyed", {
  df <- data.table::data.table(DIED = c("0", "1", "UNKNOWN"))
  # the base-R "NAs introduced by coercion" warning is the tested behavior itself
  out <- suppressWarnings(utils::capture.output(res <- enforce_col_classes(df, list(DIED = "integer"))))
  expect_true(any(grepl("COERCE WARNING", out)))
  expect_true(is.integer(res$DIED))
  expect_identical(sum(is.na(res$DIED)), 1L)
})

test_that("align_columns fills missing columns with typed NAs of the agreed class", {
  df <- data.table::data.table(AGE = c(1, 2))
  res <- align_columns(df, c("AGE", "DX1"), list(AGE = "numeric", DX1 = "character"))
  expect_true(is.character(res$DX1))
  expect_true(all(is.na(res$DX1)))
})

test_that("case-colliding source columns merge without deleting the retained column", {
  df <- data.table::data.table(year = c(2020L, NA_integer_), YEAR = c(NA_integer_, 2021L))
  res <- canonicalize_dataframe_names(df)
  expect_identical(names(res), "YEAR")
  expect_identical(res$YEAR, c(2020L, 2021L))
})

test_that("type normalization is case-insensitive and rejects unsupported targets", {
  expect_identical(normalize_type_name(" Numeric "), "numeric")
  expect_identical(normalize_type_name("posixCT"), "POSIXct")
  expect_error(coerce_to_class(1:2, "not_a_type"), "Unsupported canonical type")
})
