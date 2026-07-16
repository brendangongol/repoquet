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
  progress_output <- paste(output, collapse = "\n")
  expect_match(progress_output, "[SCHEMA PREPARE] Stage 1/3", fixed = TRUE)
  expect_match(progress_output, "[SCAN PROGRESS] repository schema survey", fixed = TRUE)
  expect_match(progress_output, "2/2 complete (100.0%)", fixed = TRUE)
  expect_match(progress_output, "[SCHEMA PREPARE] Stage 3/3", fixed = TRUE)
  expect_true(file.exists(observations))
  expect_true(file.exists(review_path))

  id_observations <- GetSchemaObservations(observations, Database = "REG",
                                           TableName = "Core", Column = "ID")
  expect_true(all(id_observations$ObservedType == "character"))
  expect_true(all(id_observations$LeadingZeroCount > 0))
  registry <- prepared$proposal$registry
  expect_identical(registry[Column == "VALUE"]$RecommendedType, "numeric")
  expect_identical(registry[Column == "CODE"]$RecommendedType, "character")
  expect_false(registry[Column == "CODE"]$RequiresReview)
  expect_identical(registry[Column == "CODE"]$DistinctValueCount, 4)
  expect_match(registry[Column == "CODE"]$ObservedValues, "A")
  expect_identical(registry[Column == "CODE"]$ValueProfileStatus, "complete")
  expect_true(registry[Column == "CODE"]$ValuesChangeAcrossPartitions)
  expect_identical(registry[Column == "ID"]$ValueProfileStatus,
                   "suppressed_identifier")
  expect_match(registry[Column == "VALUE"]$TypeHistory, "2023")
  expect_match(registry[Column == "VALUE"]$TypeHistory, "2024")

  sheet_names <- openxlsx::getSheetNames(review_path)
  expect_true("ValuePreview" %in% sheet_names)
  value_preview <- openxlsx::read.xlsx(review_path, sheet = "ValuePreview")
  expect_setequal(unique(value_preview$Value[value_preview$Column == "CODE"]),
                  c("1", "2", "A", "B"))
  workbook <- stats::setNames(lapply(sheet_names, function(sheet) {
    openxlsx::read.xlsx(review_path, sheet = sheet)
  }), sheet_names)
  if ("Decision" %in% names(workbook$ColumnDecisions)) workbook$ColumnDecisions$Decision <- "Accept"
  openxlsx::write.xlsx(workbook, review_path, overwrite = TRUE)

  output <- utils::capture.output(finalized <- FinalizeSchemaRegistry(
    review_path, final_path, strict = TRUE))
  expect_true(file.exists(final_path))
  final_schema <- finalized$table_schema
  expect_identical(final_schema[Column == "CODE"]$CanonicalType, "character")
  expect_identical(final_schema[Column == "VALUE"]$CanonicalType, "numeric")
  expect_identical(final_schema[Column == "YEAR"]$Role, "partition")
  expect_true(all(c("DistinctValueCount", "ObservedValues", "ValueProfileStatus",
                    "ValuesChangeAcrossPartitions") %in% names(final_schema)))
  expect_true("ValueDictionary" %in% openxlsx::getSheetNames(final_path))
  final_values <- load_value_dictionary(final_path)
  expect_setequal(unique(final_values$Value[final_values$Column == "CODE"]),
                  c("1", "2", "A", "B"))
})

test_that("semantic dictionaries preserve source labels and partition drift", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("duckdb")
  skip_if_not_installed("haven")
  skip_if_not_installed("openxlsx")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  haven::write_sav(data.frame(
    DIED = haven::labelled(c(0, 1), labels = c(No = 0, Yes = 1),
                           label = "Outcome")), file.path(fx$src, "d_2023.sav"))
  haven::write_sav(data.frame(
    DIED = haven::labelled(c(0, 1), labels = c(Alive = 0, Dead = 1),
                           label = "Outcome")), file.path(fx$src, "d_2024.sav"))
  M <- data.frame(
    Database = "REG", MDBDir = "REG", TableName = "Core",
    Path = c("d_2023.sav", "d_2024.sav"), FileType = "sav",
    PartitionKey = "year", PartitionValue = c("2023", "2024"))
  observations <- file.path(fx$root, "dictionary_observations.parquet")
  review_path <- file.path(fx$root, "dictionary_review.xlsx")
  final_path <- file.path(fx$root, "dictionary_catalog.xlsx")
  prepared <- suppressMessages(PrepareSchemaRegistry(
    M, MasterDBPath = fx$root, ObservationPath = observations,
    SchemaReviewPath = review_path, SourceFingerprintMode = "none"))

  label_evidence <- GetSchemaObservations(
    observations, Database = "REG", TableName = "Core", Column = "DIED")
  expect_true(all(c("variable_label", "value_label") %in%
                    label_evidence$ObservationKind))
  dictionary <- prepared$proposal$dictionary_review
  expect_true(all(dictionary$RequiresReview))
  expect_setequal(unique(dictionary$PartitionValue), c("2023", "2024"))
  expect_true(all(dictionary$Status == "label_changed_across_partitions"))
  expect_error(FinalizeSchemaRegistry(review_path, final_path, strict = TRUE),
               "semantic dictionary conflict")

  sheets <- openxlsx::getSheetNames(review_path)
  workbook <- stats::setNames(lapply(sheets, function(sheet) {
    openxlsx::read.xlsx(review_path, sheet = sheet)
  }), sheets)
  workbook$DictionaryReview$Decision <- "Accept"
  openxlsx::write.xlsx(workbook, review_path, overwrite = TRUE)
  finalized <- suppressMessages(FinalizeSchemaRegistry(
    review_path, final_path, strict = TRUE))
  expect_true("ColumnDictionary" %in% openxlsx::getSheetNames(final_path))
  approved <- load_column_dictionary(final_path)
  expect_identical(nrow(approved), 4L)
  expect_setequal(approved$Label, c("No", "Yes", "Alive", "Dead"))

  description <- describe_column(final_path, "REG", "Core", "DIED")
  expect_identical(nrow(description$schema), 1L)
  expect_identical(description$schema$VariableLabel, "Outcome")
  expect_identical(nrow(description$dictionary), 4L)
  expect_identical(search_labels("Alive", final_path, search_in = "values")$Column,
                   "DIED")
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(con, "REG_Core", data.frame(
    DIED = c(0, 1, 0, 1), YEAR = c(2023L, 2023L, 2024L, 2024L)))
  decoded <- decode_column(con, "REG_Core", "DIED", final_path, limit = 10L)
  expect_identical(decoded$DIED_LABEL, c("No", "Yes", "Alive", "Dead"))
})

test_that("missing semantic labels are optional but user additions are finalized", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("openxlsx")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(FLAG = c(1L, 2L)),
                     file.path(fx$src, "flags.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Core",
                  Path = "flags.csv", FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2024")
  review_path <- file.path(fx$root, "optional_dictionary.xlsx")
  final_path <- file.path(fx$root, "optional_catalog.xlsx")
  suppressMessages(PrepareSchemaRegistry(
    M, MasterDBPath = fx$root,
    ObservationPath = file.path(fx$root, "optional_observations.parquet"),
    SchemaReviewPath = review_path, SourceFingerprintMode = "none"))
  sheets <- openxlsx::getSheetNames(review_path)
  workbook <- stats::setNames(lapply(sheets, function(sheet) {
    openxlsx::read.xlsx(review_path, sheet = sheet)
  }), sheets)
  expect_true(all(workbook$DictionaryReview$Status == "optional_label_missing"))
  add_row <- workbook$DictionaryReview$Column == "FLAG" &
    as.character(workbook$DictionaryReview$Value) == "1"
  workbook$DictionaryReview$ApprovedLabel[add_row] <- "Yes"
  workbook$DictionaryReview$Decision[add_row] <- "Add"
  openxlsx::write.xlsx(workbook, review_path, overwrite = TRUE)
  suppressMessages(FinalizeSchemaRegistry(review_path, final_path, strict = TRUE))
  approved <- load_column_dictionary(final_path)
  expect_identical(nrow(approved), 1L)
  expect_identical(approved$Value, "1")
  expect_identical(approved$Label, "Yes")
  expect_identical(approved$Source, "user_approved")
})

test_that("delimited schema profiling detects rare types after the former sample boundary", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  path <- file.path(fx$src, "late_text.csv")
  data.table::fwrite(data.table::data.table(
    ID = seq_len(100005L),
    SPARSE_VALUE = c(rep("1", 100004L), "UNPLANNED")), path, quote = TRUE)

  profile_fun <- if (exists(".profile_delimited_schema", mode = "function")) {
    get(".profile_delimited_schema", mode = "function")
  } else {
    getFromNamespace(".profile_delimited_schema", "repoquet")
  }
  profile <- profile_fun(
    path, get_file_reader("csv"), reader_options = list(Encoding = "auto"),
    chunk_size = 100000L)
  expect_identical(profile$Rows, 100005)
  expect_identical(profile$Stats$SPARSE_VALUE$ObservedType, "character")
  expect_identical(profile$Stats$SPARSE_VALUE$NumericParseFailureCount, 1)
})

test_that("value previews enforce the distinct limit across source partitions", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(CATEGORY = paste0("C", 1:10)),
                     file.path(fx$src, "categories_a.csv"))
  data.table::fwrite(data.table::data.table(CATEGORY = paste0("C", 8:17)),
                     file.path(fx$src, "categories_b.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Core",
                  Path = c("categories_a.csv", "categories_b.csv"), FileType = "csv",
                  PartitionKey = "year", PartitionValue = c("2023", "2024"))
  prepared <- suppressMessages(PrepareSchemaRegistry(
    M, MasterDBPath = fx$root,
    ObservationPath = file.path(fx$root, "observations.parquet"),
    SchemaReviewPath = file.path(fx$root, "review.xlsx"),
    SourceFingerprintMode = "none", ValuePreviewMaxDistinct = 15L))

  category <- prepared$proposal$registry[Column == "CATEGORY"]
  expect_identical(category$ValueProfileStatus, "exceeds_limit")
  expect_true(is.na(category$DistinctValueCount))
  expect_true(is.na(category$ObservedValues))
  expect_false(any(prepared$proposal$value_preview$Column == "CATEGORY"))
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
  expect_true(all(c("StartHere", "ColumnDecisions", "CompatibilityDecisions",
                    "SourceIssues", "ColumnOverview", "CompatibilityOverview",
                    "TypeHistory", "ValuePreview", "PolicyReport", "Registry",
                    "CompatibilityRegistry", "Settings") %in%
                    openxlsx::getSheetNames(path)))
  expect_identical(openxlsx::getSheetNames(path)[1:10],
                   c("StartHere", "ColumnDecisions", "CompatibilityDecisions",
                     "DictionaryReview", "SourceIssues", "ColumnOverview", "CompatibilityOverview",
                     "TypeHistory", "ValuePreview", "PolicyReport"))
  wb <- openxlsx::loadWorkbook(path)
  visibility <- stats::setNames(openxlsx::sheetVisibility(wb),
                                openxlsx::getSheetNames(path))
  expect_identical(unname(visibility["StartHere"]), "visible")
  expect_true(all(visibility[c("Registry", "CompatibilityRegistry",
                               "DictionaryRegistry", "Settings")] == "hidden"))
  expect_true(all(visibility[c("ColumnOverview", "CompatibilityOverview",
                               "TypeHistory", "ValuePreview", "PolicyReport")] == "visible"))
  start <- openxlsx::read.xlsx(path, sheet = "StartHere", startRow = 5)
  expect_identical(start$Status[start$Step == "Finalization"], "READY")
  guide <- openxlsx::read.xlsx(path, sheet = "StartHere", startRow = 14)
  expect_true(all(c("ColumnOverview", "ColumnDecisions", "DictionaryReview",
                    "Registry") %in%
                  guide$Worksheet))
  expect_match(guide$Contains[guide$Worksheet == "ValuePreview"],
               "Low-cardinality")
  expect_match(guide$Contains[guide$Worksheet == "ColumnOverview"],
               "observed type history")
  expect_match(guide$Contains[guide$Worksheet == "DictionaryReview"],
               "source-provided meanings")
  expect_identical(guide$UserAction[guide$Worksheet == "Registry"], "Do not edit.")
  column_status <- openxlsx::read.xlsx(path, sheet = "ColumnDecisions")
  source_status <- openxlsx::read.xlsx(path, sheet = "SourceIssues")
  expect_identical(column_status$Status, "COMPLETE")
  expect_identical(column_status$RequiredAction, "No column decisions are required.")
  expect_identical(source_status$Status, "COMPLETE")
  expect_identical(source_status$RequiredAction, "No source issues were identified.")
})

test_that("the guided workbook exposes only unresolved decisions", {
  skip_if_not_installed("openxlsx")
  path <- tempfile(fileext = ".xlsx"); on.exit(unlink(path))
  registry <- data.table::data.table(
    Database = "D", TableName = "T", DuckDBTable = "D_T", Column = "X",
    ObservedTypes = "integer,character", TypeHistory = "YEAR=2023: integer; YEAR=2024: character",
    DataRecommendedType = "character", RecommendedType = "character",
    ApprovedType = "character", Risk = "Review",
    RecommendationReason = "Reader warning requires confirmation.", Confidence = "sampled",
    PolicyPattern = NA_character_, PolicyType = NA_character_, PolicyRole = NA_character_,
    PolicyStatus = "not_configured", PolicyConflict = FALSE,
    Role = "data", MergeGroup = "", RequiresReview = TRUE, Decision = "",
    DecisionOrigin = "new", UserNotes = "", ObservationSignature = "sig")
  proposal <- structure(list(
    registry = registry,
    compatibility = data.table::data.table(),
    history = registry[0, ], source_issues = registry[0, ],
    summary = data.table::data.table(Columns = 1L, AutoApproved = 0L,
                                     NeedsReview = 1L, SourceIssues = 0L,
                                     CompatibilityConflicts = 0L),
    ObservationPath = "observations.parquet"),
    class = "RepositorySchemaProposal")
  output <- utils::capture.output(WriteSchemaProposal(proposal, path,
                                                       PreserveDecisions = FALSE))
  decisions <- openxlsx::read.xlsx(path, sheet = "ColumnDecisions")
  expect_identical(nrow(decisions), 1L)
  expect_identical(names(decisions)[1:4],
                   c("Decision", "ApprovedType", "UserNotes", "RequiredAction"))
  start <- openxlsx::read.xlsx(path, sheet = "StartHere", startRow = 5)
  expect_identical(start$Status[start$Step == "Column schema decisions"],
                   "ACTION REQUIRED")
  # The second table on StartHere places text in the same worksheet column,
  # so openxlsx reads the combined used range as character even though the
  # status-table cells are numeric in Excel.
  expect_identical(as.numeric(start$Remaining[start$Step == "Column schema decisions"]), 1)
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
