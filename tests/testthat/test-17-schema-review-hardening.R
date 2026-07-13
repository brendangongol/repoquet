test_that("parallel schema surveys initialize delimited readers in sourced workers", {
  skip_if_not_installed("future")
  skip_if_not_installed("future.apply")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(ID = 1:2, TEXT = c("a", "b")),
                     file.path(fx$src, "parallel.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Core",
                  Path = "parallel.csv", FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2024")
  observations <- file.path(fx$root, "parallel-observations.parquet")

  output <- utils::capture.output(result <- suppressWarnings(SurveyRepositorySchema(
    M, fx$root, observations, n_workers = 2,
    SourceFingerprintMode = "none", StrictReaders = TRUE)))

  expect_identical(result$summary$FailedSources, 0L)
  expect_false(any(grepl("PARALLEL FALLBACK", output, fixed = TRUE)))
})

test_that("an unconfigured short record remains a structural warning", {
  internal <- function(name) get(name, envir = environment(PrepareSchemaRegistry))
  warning <- paste0("Stopped early on line 4. Expected 4 fields but found 1. ",
                    "Consider fill=TRUE. First discarded non-empty line: <<footer>>")
  classified <- internal(".classify_schema_reader_warnings")(warning, rows_sampled = 2L)
  expect_identical(classified$Class, "structural_mismatch")
  expect_identical(classified$Severity, "warning")

  rows <- data.table::data.table(
    ObservedType = "integer", ReaderWarning = warning,
    ReaderWarningSeverity = "warning", PrecisionRisk = FALSE,
    FractionalCount = 0)
  recommendation <- internal(".schema_recommendation_for_group")(rows)
  expect_identical(recommendation$RecommendedType, "integer")
  expect_true(recommendation$RequiresReview)
})

test_that("policy matches are visible and unsafe policies require explicit override", {
  internal <- function(name) get(name, envir = environment(PrepareSchemaRegistry))
  rows <- data.table::data.table(
    ObservedType = "character", ReaderWarning = NA_character_,
    ReaderWarningSeverity = NA_character_, PrecisionRisk = FALSE,
    FractionalCount = NA_real_)
  recommendation <- internal(".schema_recommendation_for_group")(rows)
  policy <- data.table::data.table(ColumnPattern = "^AGE$", CanonicalType = "numeric",
                                   Role = "analytic", AppliesTo = "all")
  resolved <- internal(".schema_policy_resolution")(rows, recommendation, policy)

  expect_identical(resolved$DataRecommendedType, "character")
  expect_identical(resolved$RecommendedType, "character")
  expect_identical(resolved$PolicyType, "numeric")
  expect_identical(resolved$PolicyStatus, "explicit_override_required")
  expect_true(resolved$PolicyConflict)
  expect_true(resolved$RequiresReview)
})

test_that("approved compatibility groups unify types and persist merge intent", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("openxlsx")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(KEY = 1:2), file.path(fx$src, "a.csv"))
  data.table::fwrite(data.table::data.table(KEY = c("A", "B")), file.path(fx$src, "b.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = c("A", "B"),
                  Path = c("a.csv", "b.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2024")
  observations <- file.path(fx$root, "observations.parquet")
  review_path <- file.path(fx$root, "review.xlsx")
  final_path <- file.path(fx$root, "tables.xlsx")
  output <- utils::capture.output(PrepareSchemaRegistry(
    M, fx$root, observations, review_path, n_workers = 1,
    SourceFingerprintMode = "none"))

  sheets <- openxlsx::getSheetNames(review_path)
  workbook <- stats::setNames(lapply(sheets, function(sheet) {
    openxlsx::read.xlsx(review_path, sheet = sheet)
  }), sheets)
  expect_equal(nrow(workbook$CompatibilityReview), 1L)
  workbook$CompatibilityReview$Decision <- "Accept"
  if (nrow(workbook$Review) > 0L) workbook$Review$Decision <- "Accept"
  openxlsx::write.xlsx(workbook, review_path, overwrite = TRUE)

  output <- utils::capture.output(final <- FinalizeSchemaRegistry(
    review_path, final_path, strict = TRUE))
  key_rows <- final$table_schema[Column == "KEY"]
  expect_true(all(key_rows$CanonicalType == "character"))
  expect_true(all(key_rows$MergeReviewed))
  expect_true(all(nzchar(key_rows$MergeGroup)))
  expect_true(all(key_rows$Role == "join_key"))
  expect_equal(nrow(ValidateSchemaMergeKeys(final$table_schema, strict = FALSE)), 0L)
  relationships <- discover_schema_relationships(final$table_schema)
  key_relationships <- relationships[Column == "KEY"]
  expect_equal(nrow(key_relationships), 1L)
  expect_identical(key_relationships$Detection, "approved_group")
})

test_that("ignored compatibility conflicts remain separate without later strict failure", {
  internal <- function(name) get(name, envir = environment(PrepareSchemaRegistry))
  registry <- data.table::data.table(
    Database = "D", TableName = c("A", "B"), DuckDBTable = c("D_A", "D_B"),
    Column = "KEY", ApprovedType = c("integer", "character"),
    RecommendedType = c("integer", "character"), Role = "data", MergeGroup = "")
  compatibility <- data.table::data.table(
    Scope = "within_database", Database = "D", Column = "KEY",
    MergeGroup = "D::KEY", RecommendedCommonType = "character",
    ApprovedCommonType = "character", SuggestedRole = "join_key",
    Decision = "Ignore")
  reviewed <- internal(".apply_compatibility_review")(registry, compatibility, strict = TRUE)
  schema <- reviewed[, .(Database, TableName, DuckDBTable, Column,
                         CanonicalType = ApprovedType, Role, MergeGroup, MergeReviewed)]

  expect_true(all(schema$MergeReviewed))
  expect_identical(schema$CanonicalType, c("integer", "character"))
  expect_equal(nrow(ValidateSchemaMergeKeys(schema, strict = FALSE)), 0L)
  expect_equal(nrow(discover_schema_relationships(schema)), 0L)
})

test_that("clean healthcare schema workflow remains executable", {
  skip_if(is.na(repoquet_root), "source-package reference file is unavailable")
  path <- file.path(repoquet_root, "inst", "examples", "healthcare",
                    "schema_workflow_test.R")
  expect_silent(parse(path))
  text <- paste(readLines(path, warn = FALSE), collapse = "\n")
  expect_match(text, "library(repoquet)", fixed = TRUE)
  expect_false(grepl("^>", text))
})
