#### Test harness for the repoquet loader workflow.                           ####
#### The suite sources the current development file into a                    ####
#### dedicated environment. Bump one constant when the version advances.     ####

REPOQUET_SOURCE_FILE <- "R/repoquet.R"

suppressMessages({
  library(data.table)
  library(arrow)
  library(haven)
  library(openxlsx)
  library(glue)
  library(DBI)
  library(future)
  library(future.apply)
})

#### Locate the repository root from wherever testthat is running.           ####
repoquet_root <- local({
  root <- Sys.getenv("REPOQUET_ROOT", unset = NA)
  if (!is.na(root) && nzchar(root)) return(normalizePath(root, winslash = "/"))
  d <- normalizePath(getwd(), winslash = "/")
  for (i in 1:6) {
    if (file.exists(file.path(d, REPOQUET_SOURCE_FILE))) return(d)
    parent <- dirname(d)
    if (identical(parent, d)) break
    d <- parent
  }
  NA_character_
})

#### Source the workflow once per test session into its own environment.     ####
if (is.na(repoquet_root)) {
  if (!requireNamespace("repoquet", quietly = TRUE)) {
    stop(sprintf("Cannot locate %s or an installed repoquet package (start: %s).",
                 REPOQUET_SOURCE_FILE, getwd()))
  }
  suppressPackageStartupMessages(library(repoquet))
} else if (!exists(".repoquet_env", inherits = TRUE)) {
  .repoquet_env <- new.env(parent = globalenv())
  src <- file.path(repoquet_root, REPOQUET_SOURCE_FILE)
  eval_errors <- character(0)
  for (ex in parse(src)) {
    tryCatch(eval(ex, .repoquet_env),
             error = function(e) eval_errors <<- c(eval_errors, conditionMessage(e)))
  }
  if (length(eval_errors) > 0L) {
    stop(sprintf("Sourcing %s produced %d error(s); first: %s",
                 REPOQUET_SOURCE_FILE, length(eval_errors), eval_errors[1]))
  }
  if (!"repoquet_fns" %in% search()) attach(.repoquet_env, name = "repoquet_fns", warn.conflicts = FALSE)
}

#### Shared fixture builders ################################################

#### A throwaway repository directory tree; caller cleans up via             ####
#### withr-style on.exit or unlink().                                        ####
new_repo_fixture <- function() {
  tmp <- tempfile("repoquet_test_")
  dir.create(file.path(tmp, "REG"), recursive = TRUE)
  list(root = tmp,
       src = file.path(tmp, "REG"),
       pq = file.path(tmp, "pq"),
       cp = file.path(tmp, "cp.rds"),
       reg = file.path(tmp, "reg.csv"),
       ts = file.path(tmp, "TS.csv"),
       mf = file.path(tmp, "mf.csv"),
       log = file.path(tmp, "log.txt"))
}

#### Run ParquetBackEndCreate with fixture defaults, quietly.                ####
run_loader <- function(fx, MDT, DBLoad, completed = character(0), PartitionBy = "NRows", ...) {
  out <- utils::capture.output(
    ck <- ParquetBackEndCreate(MDT = MDT, DBLoad = DBLoad, MasterDBPath = fx$root,
                               completed_checkpoint = completed,
                               CheckpointPath = fx$cp, ParquetBasePath = fx$pq,
                               PartitionBy = PartitionBy, RAMThreshold = 30,
                               SAV_ROW_THRESHOLD = 10L, SAV_CHUNK_SIZE = 10L,
                               LogPath = fx$log, n_workers = 1,
                               SchemaRegistryPath = fx$reg, TableSchemaPath = fx$ts,
                               ManifestPath = fx$mf, StrictPreflight = FALSE,
                               StopOnFileError = FALSE, SourceFingerprintMode = "none",
                               UseSchemaCatalog = FALSE, ...))
  list(checkpoint = ck, output = out)
}
