#### The shipped hcup rule file, and how an existing registry gains its rules ####

hcup_type <- function(database, table, column, inferred, registry = build_default_schema_registry("hcup")) {
  classes <- stats::setNames(list(inferred), column)
  unname(unlist(apply_schema_registry(classes, schema_registry = registry,
                                      database = database, table_name = table)))
}

test_that("the shipped hcup rule file is well formed", {
  reg <- build_default_schema_registry("hcup")
  expect_identical(names(reg), c("Profile", "ColumnPattern", "CanonicalType", "Role", "AppliesTo", "Notes"))
  expect_gt(nrow(reg), 50L)
  expect_true(all(reg$Profile == "hcup"))
  expect_false(anyNA(reg$ColumnPattern))
  expect_false(anyNA(reg$AppliesTo))
  expect_false(anyNA(reg$Role))
  expect_true(all(is_allowed_canonical_type(reg$CanonicalType)))
  #### Every pattern compiles, and no pattern is declared twice for the same scope.
  compiled <- vapply(reg$ColumnPattern, function(pattern) {
    !inherits(tryCatch(regexpr(pattern, "X", perl = TRUE), error = function(e) e), "error")
  }, logical(1))
  expect_true(all(compiled))
  keys <- .schema_rule_key(reg$ColumnPattern, reg$AppliesTo)
  expect_false(anyDuplicated(keys) > 0L)
  #### The file loads through the normal registry reader unchanged.
  expect_identical(nrow(load_schema_registry(NULL, profile = "hcup")), nrow(reg))
  #### The generic profile stays an empty template with the same columns.
  generic <- build_default_schema_registry("generic")
  expect_identical(nrow(generic), 0L)
  expect_identical(names(generic), names(reg))
})

test_that("hcup rules type representative columns as intended", {
  cases <- list(
    #### identifiers and keys
    list("NIS", "Core", "KEY_NIS", "integer", "character"),
    list("NIS", "Hospital", "HOSP_NIS", "integer", "character"),
    list("NIS", "Core", "NIS_STRATUM", "integer", "character"),
    list("NRD", "Core", "NRD_VISITLINK", "integer", "character"),
    #### codes
    list("NIS", "Core", "I10_DX5", "integer", "character"),
    list("NIS", "Core", "DX1", "integer", "character"),
    list("NIS", "Core", "I10_PR3", "integer", "character"),
    list("NIS", "Core", "CPT1", "integer", "character"),
    #### analytic columns and weights
    list("NIS", "Core", "AGE", "integer", "numeric"),
    list("NIS", "Core", "DISCWT", "integer", "numeric"),
    list("NIS", "Core", "TOTCHG", "integer", "numeric"),
    #### the two over-broad built-in rules were retired: these keep their inferred type
    list("NIS", "Hospital", "HOSP_TEACH", "numeric", "numeric"),
    list("NIS", "Hospital", "HOSP_CONTROL", "integer", "integer"),
    list("NTDB", "DEMO", "ANGIOGRAPHY_CODE", "integer", "integer"),
    #### Epic: DateKey must come before Key, and Epic rules stay inside the Epic database
    list("Epic", "VarDecompAdmission", "ADMISSIONDATEKEY", "integer", "integer"),
    list("Epic", "VarDecompDx", "STARTDATEKEY", "integer", "integer"),
    list("Epic", "VarDecompAdmission", "ENCOUNTERKEY", "integer", "int64"),
    list("Epic", "VarDecompAdmission", "DEPARTMENTKEY", "integer", "int64"),
    list("NIS", "Core", "DEPARTMENTKEY", "integer", "integer"),
    list("Epic", "VarDecompAdmission", "LOCATIONEPICID", "integer", "character"),
    list("Epic", "VarDecompDx", "ICD", "character", "character"),
    list("Epic", "VarDecompDx", "PRIMARYADMITDIAGNOSIS_ICD", "integer", "character"),
    list("Epic", "VarDecompDx", "ISPRIMARY", "integer", "integer"),
    list("Epic", "VarDecompAdmission", "LENGTHOFSTAYINDAYS", "integer", "integer"),
    list("Epic", "VarDecompDRG", "DRGCODE", "integer", "character"),
    #### a timestamp rule never forces text that was not read as a timestamp
    list("Epic", "VarDecompWPHS", "RECORDEDINSTANT", "POSIXct", "POSIXct"),
    list("Epic", "VarDecompWPHS", "RECORDEDINSTANT", "character", "character"),
    #### TQP's ECODE is a code; the same name elsewhere is left alone
    list("TQP", "ECODE", "ECODE", "integer", "character"),
    list("NTDB", "ECODE", "ECODE", "integer", "integer"))
  for (case in cases) {
    expect_identical(hcup_type(case[[1]], case[[2]], case[[3]], case[[4]]), case[[5]],
                     info = paste(case[[1]], case[[2]], case[[3]], "inferred", case[[4]]))
  }
})

registry_fixture <- function(extension = "csv", rows = NULL) {
  path <- tempfile("registry_", fileext = paste0(".", extension))
  if (is.null(rows)) rows <- data.frame(
    Profile = "hcup", ColumnPattern = c("^MYCOLUMN$", "^LOS$"),
    CanonicalType = c("character", "integer"), Role = c("code", "analytic"),
    AppliesTo = "all", Notes = c("A person's own rule.", "A person's edit of a shipped rule."),
    stringsAsFactors = FALSE)
  if (extension == "xlsx") openxlsx::write.xlsx(list(SchemaRegistry = rows), path) else data.table::fwrite(rows, path)
  path
}

read_registry <- function(path) {
  if (tolower(tools::file_ext(path)) == "xlsx") data.table::as.data.table(openxlsx::read.xlsx(path, sheet = 1))
  else data.table::fread(path)
}

test_that("an existing registry gains only the shipped rules it lacks, after its own rows", {
  shipped <- build_default_schema_registry("hcup")
  for (extension in c("csv", "xlsx")) {
    path <- registry_fixture(extension); on.exit(unlink(path), add = TRUE)
    before <- read_registry(path)
    out <- utils::capture.output(added <- install_shipped_schema_rules(path, "hcup"))
    after <- read_registry(path)
    #### The person's rows come first and are unchanged, including the edited copy of a shipped rule.
    expect_identical(after$ColumnPattern[1:2], before$ColumnPattern)
    expect_identical(after$CanonicalType[1:2], c("character", "integer"))
    expect_identical(sum(after$ColumnPattern == "^LOS$"), 1L)
    #### Every other shipped rule was added once, in shipped order, after them.
    expect_identical(added, nrow(shipped) - 1L)
    expect_identical(after$ColumnPattern[-(1:2)], shipped$ColumnPattern[shipped$ColumnPattern != "^LOS$"])
    #### A backup of the original was saved once.
    backup <- paste0(tools::file_path_sans_ext(path), ".pre_hcup.", tools::file_ext(path))
    expect_true(file.exists(backup))
    expect_identical(nrow(read_registry(backup)), 2L)
    expect_true(any(grepl("Added .* shipped hcup rule", out)))
    #### A second call finds nothing to add and writes nothing.
    stamp <- file.mtime(path)
    expect_identical(install_shipped_schema_rules(path, "hcup"), 0L)
    expect_identical(file.mtime(path), stamp)
    #### The result is still a valid registry.
    expect_gt(nrow(load_schema_registry(path, create_if_missing = FALSE, profile = "hcup")), nrow(shipped) - 1L)
    unlink(backup)
  }
})

test_that("the installer leaves alone anything it should not change", {
  shipped <- build_default_schema_registry("hcup")
  #### The generic profile never adds rules.
  generic_path <- registry_fixture("csv"); on.exit(unlink(generic_path), add = TRUE)
  stamp <- file.mtime(generic_path)
  expect_identical(install_shipped_schema_rules(generic_path, "generic"), 0L)
  expect_identical(file.mtime(generic_path), stamp)
  expect_false(file.exists(paste0(tools::file_path_sans_ext(generic_path), ".pre_hcup.csv")))
  #### A registry that already has every shipped rule is untouched and not backed up.
  complete_path <- registry_fixture("csv", rows = as.data.frame(shipped)); on.exit(unlink(complete_path), add = TRUE)
  expect_identical(install_shipped_schema_rules(complete_path, "hcup"), 0L)
  expect_false(file.exists(paste0(tools::file_path_sans_ext(complete_path), ".pre_hcup.csv")))
  #### A missing file, and a file without the required columns, are left for the loader to report.
  expect_identical(install_shipped_schema_rules(file.path(tempdir(), "no_such_registry.csv"), "hcup"), 0L)
  odd_path <- registry_fixture("csv", rows = data.frame(Pattern = "^X$", Type = "integer")); on.exit(unlink(odd_path), add = TRUE)
  expect_identical(install_shipped_schema_rules(odd_path, "hcup"), 0L)
  expect_identical(names(read_registry(odd_path)), c("Pattern", "Type"))
})

test_that("RepositoryInitialize adds the shipped rules to an existing registry under hcup only", {
  dir <- tempfile("hcup_init_"); on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  shipped <- build_default_schema_registry("hcup")
  out <- utils::capture.output(paths <- RepositoryInitialize(FormattedDBPath = dir, profile = "hcup"))
  expect_identical(nrow(read_registry(paths$SchemaRegistryPath)), nrow(shipped))   # a new registry is seeded with all of them
  #### Cut the registry back to two rows, as an older repository might have, then initialize again.
  openxlsx::write.xlsx(list(SchemaRegistry = as.data.frame(shipped[1:2])), paths$SchemaRegistryPath)
  out <- utils::capture.output(RepositoryInitialize(FormattedDBPath = dir, profile = "generic"))
  expect_identical(nrow(read_registry(paths$SchemaRegistryPath)), 2L)                # generic never adds
  out <- utils::capture.output(RepositoryInitialize(FormattedDBPath = dir, profile = "hcup"))
  expect_identical(nrow(read_registry(paths$SchemaRegistryPath)), nrow(shipped))
  expect_true(file.exists(sub("\\.xlsx$", ".pre_hcup.xlsx", paths$SchemaRegistryPath)))
  #### An unreadable registry warns instead of stopping the initialization.
  writeLines("this is not a workbook", paths$SchemaRegistryPath)
  expect_warning(utils::capture.output(RepositoryInitialize(FormattedDBPath = dir, profile = "hcup")),
                 "Could not add the shipped hcup schema rules")
})
