#### Standalone test runner for the repoquet loader workflow.                  ####
#### The focused workflow suite sources the active implementation via        ####
#### tests/testthat/helper-repoquet.R so it can test development changes before ####
#### package installation. Run from the repository root:                      ####
####   Rscript tests/run_tests.R                                             ####

if (!requireNamespace("testthat", quietly = TRUE)) {
  stop("The 'testthat' package is required to run the test suite.")
}

#### Anchor to the repository root regardless of invocation directory.       ####
script_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_file <- if (length(script_arg) == 1L) sub("^--file=", "", script_arg) else "tests/run_tests.R"
if (!file.exists(script_file)) script_file <- file.path(getwd(), basename(script_file))
script_dir <- dirname(normalizePath(script_file, winslash = "/"))
repo_root <- normalizePath(file.path(script_dir, ".."), winslash = "/", mustWork = FALSE)

test_candidates <- c(file.path(repo_root, "tests", "testthat"),
                     file.path(script_dir, "testthat"))
test_dir <- test_candidates[dir.exists(test_candidates)][1]
if (is.na(test_dir)) stop("Cannot locate tests/testthat from ", script_dir)

results <- testthat::test_dir(test_dir, stop_on_failure = TRUE)
invisible(results)
