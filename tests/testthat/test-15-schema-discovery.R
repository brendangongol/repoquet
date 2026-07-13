test_that("schema discovery produces observations, review, and finalized catalog", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("openxlsx")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(
    ID = c("001", "002"), VALUE = c(1L, 2L), CODE = c("1", "2")),
    file.path(fx$src, "a_2023.csv"), quote = TRUE)
  data.table::fwrite(data.table::data.table(
    ID = c("003", "004"), VALUE = c(1.5, 2.5), CODE = c("A", "B")),
    file.path(fx$src, "a_2024.csv"), quote = TRUE)
  M <- data.frame(
    Database = "REG", MDBDir = "REG", TableName = "Core",
    Path = c("a_2023.csv", "a_2024.csv"), FileType = "csv",
    PartitionKey = "year", PartitionValue = c("2023", "2024"))
  observations <- file.path(fx$root, "SchemaObservations.parquet")
  review_path <- file.path(fx$root, "SchemaReview.xlsx")
  final_path <- file.path(fx$root, "TableSchemas.xlsx")

  output <- utils::capture.output(prepared <- PrepareSchemaRegistry(
    M, MasterDBPath = fx$root, ObservationPath = observations,
    SchemaReviewPath = review_path, n_workers = 1,
    SourceFingerprintMode = "none"))
  expect_true(file.exists(observations))
  expect_true(file.exists(review_path))

  id_observations <- GetSchemaObservations(observations, Database = "REG",
                                           TableName = "Core", Column = "ID")
  expect_true(all(id_observations$ObservedType == "character"))
  expect_true(all(id_observations$LeadingZeroCount > 0))
  registry <- prepared$proposal$registry
  expect_identical(registry[Column == "VALUE"]$RecommendedType, "numeric")
  expect_identical(registry[Column == "CODE"]$RecommendedType, "character")
  expect_true(registry[Column == "CODE"]$RequiresReview)
  expect_match(registry[Column == "VALUE"]$TypeHistory, "2023")
  expect_match(registry[Column == "VALUE"]$TypeHistory, "2024")

  sheet_names <- openxlsx::getSheetNames(review_path)
  workbook <- stats::setNames(lapply(sheet_names, function(sheet) {
    openxlsx::read.xlsx(review_path, sheet = sheet)
  }), sheet_names)
  workbook$Review$Decision <- "Accept"
  openxlsx::write.xlsx(workbook, review_path, overwrite = TRUE)

  output <- utils::capture.output(finalized <- FinalizeSchemaRegistry(
    review_path, final_path, strict = TRUE))
  expect_true(file.exists(final_path))
  final_schema <- finalized$table_schema
  expect_identical(final_schema[Column == "CODE"]$CanonicalType, "character")
  expect_identical(final_schema[Column == "VALUE"]$CanonicalType, "numeric")
  expect_identical(final_schema[Column == "YEAR"]$Role, "partition")
})

test_that("schema finalization refuses unresolved review decisions", {
  skip_if_not_installed("openxlsx")
  path <- tempfile(fileext = ".xlsx"); on.exit(unlink(path))
  out <- tempfile(fileext = ".xlsx"); on.exit(unlink(out), add = TRUE)
  registry <- data.frame(
    Database = "D", TableName = "T", DuckDBTable = "D_T", Column = "X",
    ObservedTypes = "character,integer", TypeHistory = "year=2023: integer; year=2024: character",
    RecommendedType = "character", ApprovedType = "character", Risk = "Review",
    RecommendationReason = "Mixed types.", Confidence = "sampled", Role = "data",
    MergeGroup = "", RequiresReview = TRUE, Decision = "", UserNotes = "",
    ObservationSignature = "abc")
  openxlsx::write.xlsx(list(Review = registry, Registry = registry,
                            History = data.frame(), Settings = data.frame()), path)
  expect_error(FinalizeRepositorySchema(path, out, strict = TRUE), "remain unresolved")
})

test_that("schema proposal writes cleanly when no columns need review", {
  skip_if_not_installed("openxlsx")
  path <- tempfile(fileext = ".xlsx"); on.exit(unlink(path))
  registry <- data.table::data.table(
    Database = "D", TableName = "T", DuckDBTable = "D_T", Column = "X",
    ObservedTypes = "integer", TypeHistory = "year=2024: integer",
    RecommendedType = "integer", ApprovedType = "integer", Risk = "Lossless",
    RecommendationReason = "Observed consistently as integer.", Confidence = "sampled",
    Role = "data", MergeGroup = "", RequiresReview = FALSE,
    Decision = "Auto-approved", UserNotes = "", ObservationSignature = "abc")
  proposal <- structure(list(
    registry = registry, history = registry[0, ], source_issues = registry[0, ],
    summary = data.table::data.table(Columns = 1L, AutoApproved = 1L,
                                     NeedsReview = 0L, SourceIssues = 0L),
    ObservationPath = "observations.parquet"),
    class = "RepositorySchemaProposal")
  output <- utils::capture.output(WriteSchemaProposal(proposal, path, PreserveDecisions = FALSE))
  expect_true(all(c("Review", "Registry", "History", "Settings") %in%
                    openxlsx::getSheetNames(path)))
})

test_that("schema finalization requires explicit, valid overrides", {
  skip_if_not_installed("openxlsx")
  path <- tempfile(fileext = ".xlsx"); on.exit(unlink(path))
  out <- tempfile(fileext = ".xlsx"); on.exit(unlink(out), add = TRUE)
  registry <- data.frame(
    Database = "D", TableName = "T", DuckDBTable = "D_T", Column = "X",
    ObservedTypes = "integer", RecommendedType = "integer",
    ApprovedType = "character", RequiresReview = FALSE,
    Decision = "Auto-approved", Role = "data", MergeGroup = "")
  openxlsx::write.xlsx(list(Registry = registry), path)
  expect_error(FinalizeRepositorySchema(path, out), "Decision='Override'")
  registry$Decision <- "Override"
  registry$ApprovedType <- ""
  openxlsx::write.xlsx(list(Registry = registry), path, overwrite = TRUE)
  expect_error(FinalizeRepositorySchema(path, out), "ApprovedType contains blank")
})

test_that("the healthcare reference stages the reviewed schema workflow", {
  skip_if(is.na(repoquet_root), "source-package reference file is unavailable")
  path <- file.path(repoquet_root, "inst", "examples", "healthcare",
                    "CECORC_loader_reference.R")
  expect_silent(parse(path))
  text <- paste(readLines(path, warn = FALSE), collapse = "\n")
  expect_match(text, "PrepareSchemaRegistry\\(", fixed = FALSE)
  expect_match(text, "GetSchemaObservations\\(", fixed = FALSE)
  expect_match(text, "FinalizeSchemaRegistry\\(", fixed = FALSE)
  expect_false(grepl("repository_catalog <- BuildRepositoryCatalog", text, fixed = TRUE))
})
