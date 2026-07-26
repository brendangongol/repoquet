#### Exercises inst/scripts/repoquet.R -- the generic command-line driver -- ####
#### end-to-end against the fully synthetic, network-free example repo.     ####

run_cli <- function(cli_args, wd = repoquet_root) {
  skip_if(is.na(repoquet_root), "source-package root is unavailable")
  script <- file.path(repoquet_root, "inst", "scripts", "repoquet.R")
  skip_if_not(file.exists(script), "CLI script is unavailable")
  rscript <- file.path(R.home("bin"), if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")
  if (!file.exists(rscript)) rscript <- Sys.which("Rscript")
  skip_if_not(nzchar(rscript) && file.exists(rscript), "Rscript is unavailable")
  withr_wd <- getwd()
  on.exit(setwd(withr_wd), add = TRUE)
  setwd(wd)
  #### system2()'s env= argument is unreliable on Windows (it can be         ####
  #### literally prepended as a stray command-line argument rather than     ####
  #### setting the child's environment). Setting it in this process instead ####
  #### works everywhere, since child processes inherit the environment.     ####
  old_source <- Sys.getenv("REPOQUET_SOURCE", unset = NA)
  Sys.setenv(REPOQUET_SOURCE = file.path(repoquet_root, "R", "repoquet.R"))
  on.exit(if (is.na(old_source)) Sys.unsetenv("REPOQUET_SOURCE") else
            Sys.setenv(REPOQUET_SOURCE = old_source), add = TRUE)
  system2(rscript, args = c("--vanilla", shQuote(script), cli_args),
          stdout = TRUE, stderr = TRUE)
}

test_that("the CLI script rejects a missing or unknown command before doing any work", {
  skip_if(is.na(repoquet_root), "source-package root is unavailable")
  out_no_args <- run_cli(character(0))
  expect_true(any(grepl("Usage: repoquet.R", out_no_args)))
  out_bad_cmd <- run_cli(c("bogus", tempdir()))
  expect_true(any(grepl("Unknown command: bogus", out_bad_cmd)))
})

test_that("the CLI init command scaffolds a new project", {
  dir <- tempfile("cli_init_"); on.exit(unlink(dir, recursive = TRUE))
  out_init <- run_cli(c("init", dir))
  expect_true(any(grepl("\\[SCAFFOLD\\]", out_init)))
  expect_true(file.exists(file.path(dir, "repository_config.R")))
  expect_true(file.exists(file.path(dir, "DBSetup.xlsx")))
  expect_true(file.exists(file.path(dir, "run_repository.R")))
})

test_that("the CLI script runs the full validate -> schema -> finalize -> load -> audit cycle", {
  skip_if(is.na(repoquet_root), "source-package root is unavailable")
  #### generate_example_repository() builds a complete, self-consistent      ####
  #### synthetic project (config + populated MDT + real source files) in    ####
  #### one call -- driving the CLI's "init" separately and then splicing in ####
  #### a different project's MDT/MasterDBPath is unnecessary and fragile.   ####
  dir <- tempfile("cli_project_"); on.exit(unlink(dir, recursive = TRUE))
  invisible(utils::capture.output(generate_example_repository(dir)))
  formatted_dir <- file.path(dir, "formatted")

  #### validate: structural check only, no network ####
  out_validate <- run_cli(c("validate", dir))
  expect_false(any(grepl("Error", out_validate, fixed = TRUE)))

  #### schema: survey sources and write the review workbook ####
  out_schema <- run_cli(c("schema", dir))
  expect_true(any(grepl("Open StartHere", out_schema, fixed = TRUE)))
  review_path <- file.path(formatted_dir, "Schema", "SchemaReview.xlsx")
  expect_true(file.exists(review_path))

  #### finalize: accept the (auto-approved, no blocking decisions) review ####
  out_finalize <- run_cli(c("finalize", dir))
  expect_false(any(grepl("Error", out_finalize, fixed = TRUE)))
  expect_true(file.exists(file.path(formatted_dir, "Schema", "TableSchemas.xlsx")))

  #### load: write Hive-partitioned Parquet from the synthetic sources ####
  out_load <- run_cli(c("load", dir))
  expect_true(any(grepl("\\$checkpoint", out_load)) || any(grepl("checkpoint", out_load, ignore.case = TRUE)))
  expect_true(dir.exists(file.path(formatted_dir, "parquet")))

  #### audit: read-only reconciliation, should run without crashing ####
  out_audit <- run_cli(c("audit", dir))
  expect_false(any(grepl("Error in", out_audit, fixed = TRUE)))
})
