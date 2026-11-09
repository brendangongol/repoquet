#### A rule the data supports resolves a column; a rule the data contradicts is a decision; coverage is reported ####

internal <- function(name) get(name, envir = environment(PrepareSchemaRegistry))

resolve_with_rule <- function(observed, pattern, type, precision_risk = FALSE, warning = NA_character_,
                              fractional = 0, minimum = NULL, maximum = NULL) {
  rows <- data.table::data.table(
    ObservedType = observed, ReaderWarning = warning,
    ReaderWarningSeverity = if (is.na(warning)) NA_character_ else "warning",
    PrecisionRisk = precision_risk, FractionalCount = fractional)
  if (!is.null(minimum)) rows[, Minimum := minimum]
  if (!is.null(maximum)) rows[, Maximum := maximum]
  recommendation <- internal(".schema_recommendation_for_group")(rows)
  policy <- data.table::data.table(ColumnPattern = pattern, CanonicalType = type, Role = "analytic", AppliesTo = "all")
  list(data = recommendation, with_rule = internal(".schema_policy_resolution")(rows, recommendation, policy),
       without_rule = internal(".schema_policy_resolution")(rows, recommendation, NULL))
}

test_that("a rule that the data supports resolves a column the data alone would have sent to review", {
  #### No usable values: the data alone has no evidence, so it asks; a rule declares the type.
  none <- resolve_with_rule("unknown", "^PAY[0-9]$", "integer")
  expect_true(none$data$RequiresReview)
  expect_true(none$without_rule$RequiresReview)
  expect_false(none$with_rule$RequiresReview)
  expect_identical(none$with_rule$RecommendedType, "integer")
  expect_match(none$with_rule$Reason, "declares integer, so no review is needed")
  #### Integers beyond exact floating-point precision: the data alone asks; a character key rule resolves it.
  wide <- resolve_with_rule(c("integer", "numeric"), "^KEY$", "character", precision_risk = TRUE)
  expect_true(wide$without_rule$RequiresReview)
  expect_false(wide$with_rule$RequiresReview)
  expect_identical(wide$with_rule$PolicyStatus, "matched")
  #### A rule that agrees with a clean column changes nothing.
  clean <- resolve_with_rule("integer", "^LOS$", "integer")
  expect_false(clean$with_rule$RequiresReview)
  expect_false(grepl("no review is needed", clean$with_rule$Reason, fixed = TRUE))
  #### A lossless promotion is applied and resolved.
  promoted <- resolve_with_rule("integer", "^AGE$", "numeric")
  expect_identical(promoted$with_rule$RecommendedType, "numeric")
  expect_identical(promoted$with_rule$PolicyStatus, "lossless_policy_promotion")
  expect_false(promoted$with_rule$RequiresReview)
})

test_that("reader warnings and contradicted rules still need a person", {
  warning_text <- "Stopped early on line 4. Expected 4 fields but found 1. First discarded non-empty line: <<x>>"
  warned <- resolve_with_rule("integer", "^LOS$", "integer", warning = warning_text)
  expect_true(warned$with_rule$RequiresReview)
  #### The data cannot be the rule's type: the data type is kept and a decision is requested.
  contradicted <- resolve_with_rule("character", "^DIED$", "integer")
  expect_false(contradicted$without_rule$RequiresReview)
  expect_true(contradicted$with_rule$RequiresReview)
  expect_identical(contradicted$with_rule$RecommendedType, "character")
  expect_identical(contradicted$with_rule$PolicyStatus, "explicit_override_required")
  #### Numeric values prove nothing about being integer unless the survey recorded that they are whole numbers
  #### inside the integer range; with no range evidence the rule is a contradiction.
  unproven <- resolve_with_rule("numeric", "^DRG$", "integer")
  expect_true(unproven$with_rule$RequiresReview)
  expect_identical(unproven$with_rule$RecommendedType, "numeric")
  #### Without a rule nothing changed: clean evidence passes, risky evidence is reviewed.
  expect_false(resolve_with_rule("integer", "^NOMATCH$", "integer")$without_rule$RequiresReview)
  expect_true(resolve_with_rule(c("integer", "numeric"), "^NOMATCH$", "integer", precision_risk = TRUE)$without_rule$RequiresReview)
})

test_that("rule coverage counts how each rule was used and what no rule covers", {
  coverage_of <- internal(".schema_rule_coverage")
  rules <- data.table::data.table(
    ColumnPattern = c("^KEY_[A-Z]+$", "^DIED$", "^KEY_[A-Z]+$", "^NEVER$", "^SCOPED$", "^AGE$"),
    CanonicalType = c("character", "integer", "numeric", "integer", "integer", "numeric"),
    Role = c("join_key", "analytic", "join_key", "analytic", "analytic", "analytic"),
    AppliesTo = c("all", "all", "all", "all", "Epic", "all"))
  registry <- data.table::data.table(
    Database = c("NIS", "NIS", "NRD", "NIS", "NIS", "NIS", "NIS"),
    TableName = c("Core", "Core", "Core", "Core", "Core", "Core", "Core"),
    Column = c("KEY_NIS", "DIED", "DIED", "FREE", "SCOPED", "AGE", "KEY_NRD"),
    PolicyStatus = c("matched", "explicit_override_required", "matched", "not_configured",
                     "not_configured", "lossless_policy_promotion", "matched"))
  coverage <- coverage_of(registry, rules)
  expect_identical(nrow(coverage), nrow(rules) + 1L)
  expect_identical(coverage$ColumnPattern[1], "^KEY_[A-Z]+$")
  expect_identical(coverage$ColumnsMatched[1:6], c(2L, 2L, 0L, 0L, 0L, 1L))
  expect_identical(coverage$Agreed[2], 1L)
  expect_identical(coverage$Contradicted[2], 1L)
  expect_identical(coverage$LosslessPromotion[6], 1L)
  expect_match(coverage$RuleStatus[2], "contradicts this rule in 1 column")
  expect_match(coverage$RuleStatus[3], "Same pattern and scope as rule 1")
  expect_identical(coverage$RuleStatus[4], "Matched no column in this survey.")
  expect_identical(coverage$RuleStatus[5], "Matched no column in this survey.")   # an Epic-only rule does not reach NIS
  expect_identical(coverage$RuleStatus[1], "OK")
  #### Two columns match no rule, and every column is accounted for exactly once.
  expect_identical(coverage$ColumnPattern[nrow(coverage)], "(no rule matched)")
  expect_identical(coverage$ColumnsMatched[nrow(coverage)], 2L)
  expect_equal(sum(coverage$ColumnsMatched), nrow(registry))
  #### No rules, or a registry without the needed columns, gives an empty table instead of an error.
  expect_identical(nrow(coverage_of(registry, NULL)), 0L)
  expect_identical(nrow(coverage_of(registry, rules[0])), 0L)
  expect_identical(nrow(coverage_of(data.table::data.table(Other = 1), rules)), 0L)
})

test_that("the review workbook asks only about a contradicted rule and reports rule coverage", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("openxlsx")
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  data.table::fwrite(data.table::data.table(
    KEY = 1:5, DIED = c("yes", "no", "no", "yes", "no"), AGE = c(30L, 41L, 52L, 63L, 74L),
    NOTE = letters[1:5],
    #### Whole numbers written with a decimal point are read as doubles; the survey records that they are whole.
    DISPUNIFORM = c("1.0", "2.0", "3.0", "1.0", "5.0")), file.path(fx$src, "a.csv"))
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "A", Path = "a.csv", FileType = "csv",
                  PartitionKey = "year", PartitionValue = "2024")
  review_path <- file.path(fx$root, "review.xlsx")
  output <- utils::capture.output(PrepareSchemaRegistry(
    M, fx$root, file.path(fx$root, "observations.parquet"), review_path, n_workers = 1,
    SourceFingerprintMode = "none", SchemaProfile = "hcup"))
  sheets <- openxlsx::getSheetNames(review_path)
  expect_true("RuleCoverage" %in% sheets)
  expect_identical(sheets[10:11], c("PolicyReport", "RuleCoverage"))
  visibility <- stats::setNames(openxlsx::sheetVisibility(openxlsx::loadWorkbook(review_path)), sheets)
  expect_identical(unname(visibility["RuleCoverage"]), "visible")
  #### DIED is text but the rule says integer: it is the only column that needs a decision.
  decisions <- data.table::as.data.table(openxlsx::read.xlsx(review_path, sheet = "ColumnDecisions"))
  expect_identical(decisions$Column, "DIED")
  expect_match(decisions$RecommendationReason, "Select Accept to keep it")
  registry <- data.table::as.data.table(openxlsx::read.xlsx(review_path, sheet = "Registry"))
  expect_identical(sort(registry$Column[registry$RequiresReview %in% TRUE]), "DIED")
  expect_true(all(registry$Decision[registry$Column %in% c("KEY", "AGE", "NOTE", "DISPUNIFORM")] == "Auto-approved"))
  #### The double-valued column that the rule declares integer needs no decision, and is stored as integer.
  expect_identical(registry$ObservedTypes[registry$Column == "DISPUNIFORM"], "numeric")
  expect_identical(registry$RecommendedType[registry$Column == "DISPUNIFORM"], "integer")
  expect_match(registry$RecommendationReason[registry$Column == "DISPUNIFORM"], "every sampled value is a whole number")
  #### The policy report lists both rule differences and says which one needs action.
  policy <- data.table::as.data.table(openxlsx::read.xlsx(review_path, sheet = "PolicyReport"))
  expect_identical(policy$ActionRequired[policy$Column == "DIED"], "Yes - decide in ColumnDecisions")
  expect_identical(policy$ActionRequired[policy$Column == "KEY"], "No - informational only")
  #### Rule coverage records the contradiction, the promotion, and the column no rule covers.
  coverage <- data.table::as.data.table(openxlsx::read.xlsx(review_path, sheet = "RuleCoverage"))
  expect_equal(coverage$Contradicted[coverage$ColumnPattern == "^DIED$"], 1)
  expect_equal(coverage$LosslessPromotion[coverage$ColumnPattern == "^AGE$"], 1)
  expect_equal(coverage$ColumnsMatched[coverage$ColumnPattern == "(no rule matched)"], 1)   # NOTE
  expect_equal(sum(coverage$ColumnsMatched), nrow(registry))
  guide <- data.table::as.data.table(openxlsx::read.xlsx(review_path, sheet = "StartHere", startRow = 14))
  expect_true("RuleCoverage" %in% guide$Worksheet)
})

test_that("whole numbers stored as doubles satisfy an integer rule without a decision", {
  #### A 0/1 flag read as 0.0/1.0: every sampled value is whole and inside the integer range.
  flag <- resolve_with_rule("numeric", "^DIED$", "integer", minimum = "0", maximum = "1")
  expect_true(flag$without_rule$RequiresReview == FALSE)
  expect_identical(flag$with_rule$RecommendedType, "integer")
  expect_identical(flag$with_rule$PolicyStatus, "lossless_policy_promotion")
  expect_false(flag$with_rule$RequiresReview)
  expect_match(flag$with_rule$Reason, "every sampled value is a whole number, so integer was applied automatically")
  #### Mixed integer and double evidence is the same.
  mixed <- resolve_with_rule(c("integer", "numeric"), "^DRG$", "integer", minimum = c("1", "1"), maximum = c("999", "999"))
  expect_false(mixed$with_rule$RequiresReview)
  expect_identical(mixed$with_rule$RecommendedType, "integer")
  #### One fractional value, a value beyond the integer range, a precision risk, or no recorded range: still a decision.
  fractional <- resolve_with_rule("numeric", "^DIED$", "integer", fractional = 3, minimum = "0", maximum = "1")
  expect_true(fractional$with_rule$RequiresReview)
  expect_identical(fractional$with_rule$RecommendedType, "numeric")
  beyond <- resolve_with_rule("numeric", "^DIED$", "integer", minimum = "0", maximum = "3000000000")
  expect_true(beyond$with_rule$RequiresReview)
  expect_identical(beyond$with_rule$RecommendedType, "numeric")
  risky <- resolve_with_rule("numeric", "^DIED$", "integer", precision_risk = TRUE, minimum = "0", maximum = "1")
  expect_true(risky$with_rule$RequiresReview)
  norange <- resolve_with_rule("numeric", "^DIED$", "integer")
  expect_true(norange$with_rule$RequiresReview)
  #### A 64-bit rule needs no range, only whole numbers within exact precision.
  wide <- resolve_with_rule("numeric", "^ENCOUNTERKEY$", "int64")
  expect_false(wide$with_rule$RequiresReview)
  expect_identical(wide$with_rule$RecommendedType, "int64")
  #### Text never counts as whole numbers: a label where a code is expected stays a decision.
  labels <- resolve_with_rule("character", "^DIED$", "integer", minimum = "0", maximum = "1")
  expect_true(labels$with_rule$RequiresReview)
  expect_identical(labels$with_rule$RecommendedType, "character")
  #### Without any rule, nothing changed.
  expect_false(flag$without_rule$RequiresReview)
})
