#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2L) {
  stop("Usage: repoquet.R <init|validate|schema|finalize|load|audit> <project-or-config-path>", call. = FALSE)
}

command <- tolower(args[1])
target <- normalizePath(args[2], winslash = "/", mustWork = command != "init")

source_path <- Sys.getenv("REPOQUET_SOURCE", unset = "R/repoquet.R")
if (file.exists(source_path)) {
  source(source_path)
} else if (requireNamespace("repoquet", quietly = TRUE)) {
  suppressPackageStartupMessages(library(repoquet))
} else {
  stop("Install repoquet or set REPOQUET_SOURCE to the workflow R file.")
}

if (command == "init") {
  create_repository_project(target, profile = if (length(args) >= 3L) args[3] else "generic")
  quit(save = "no", status = 0L)
}

config_path <- if (dir.exists(target)) file.path(target, "repository_config.R") else target
cfg <- load_repository_config(config_path)
paths <- RepositoryInitialize(cfg$FormattedDBPath)
MDT <- openxlsx::read.xlsx(cfg$MDTPath, sheet = "Sheet1")

if (command == "validate") {
  ValidateMDTPreflight(MDT, strict = TRUE, ParquetBasePath = paths$ParquetBasePath)
} else if (command %in% c("schema", "catalog")) {
  PrepareSchemaRegistry(MDT, MasterDBPath = cfg$MasterDBPath,
                        ObservationPath = paths$SchemaObservationPath,
                        SchemaReviewPath = paths$SchemaReviewPath,
                        n_workers = cfg$n_workers,
                        SchemaRegistryPath = paths$SchemaRegistryPath)
  message("Open StartHere in SchemaReview.xlsx, complete its required decisions, then run 'finalize'.")
} else if (command == "finalize") {
  FinalizeSchemaRegistry(paths$SchemaReviewPath, paths$TableSchemaPath, strict = TRUE)
} else if (command == "load") {
  result <- ParquetBackEndCreate(
    MDT = MDT, DBLoad = sort(unique(MDT$Database)), MasterDBPath = cfg$MasterDBPath,
    completed_checkpoint = load_checkpoint(paths$CheckpointPath),
    CheckpointPath = paths$CheckpointPath, ParquetBasePath = paths$ParquetBasePath,
    LogPath = paths$LogPath, n_workers = cfg$n_workers, PartitionBy = cfg$PartitionBy,
    RAMThreshold = cfg$RAMThreshold, SAV_ROW_THRESHOLD = cfg$SAV_ROW_THRESHOLD,
    SAV_CHUNK_SIZE = cfg$SAV_CHUNK_SIZE, SchemaRegistryPath = paths$SchemaRegistryPath,
    TableSchemaPath = paths$TableSchemaPath, ManifestPath = paths$ManifestPath,
    UseSchemaCatalog = TRUE,
    MaxCoerceNAPct = cfg$MaxCoerceNAPct,
    SourceFingerprintMode = cfg$SourceFingerprintMode,
    StopOnFileError = TRUE, ReturnRunResult = TRUE)
  print(result)
} else if (command == "audit") {
  print(audit_repository(MDT, paths$ParquetBasePath, paths$CheckpointPath,
                         paths$ManifestPath)$issues)
} else {
  stop(sprintf("Unknown command: %s", command), call. = FALSE)
}
