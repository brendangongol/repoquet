#' @keywords internal
"_PACKAGE"

######################################################################################################################
######################################################################################################################
#### Database management functions ###################################################################################
######################################################################################################################
######################################################################################################################

# Imports
#' @import data.table
#' @import arrow
#' @import future
#' @import future.apply
#' @importFrom DBI dbExecute dbIsValid dbDisconnect
#' @importFrom haven read_sav
#' @importFrom glue glue
#' @importFrom data.table setDT set fread
NULL

utils::globalVariables(c(
  ".", "AppliesTo", "Candidate", "CandidateMergeKey", "CanonicalColumn",
  "CanonicalType", "ChunkStem", "Column", "Column_Left", "Column_Right",
  "ColumnPattern", "Database", "Dir",
  "DirKey", "DuckDBTable", "DuckDBTable_Left", "DuckDBTable_Right", "Enabled",
  "ExplicitMergeKey", "ExplicitGroup", "FileType", "FileTypeLower", "FromClass", "InferredType",
  "KeySet", "MemBurden", "N", "NDestroyed", "NLogical", "NPhysical",
  "NPresent", "NTypes", "Normalized", "Notes", "OutputStem", "ParquetPath",
  "PartitionsOnDisk", "Path", "PctOutOfDomain", "PhysicalTable",
  "PhysicalTableName", "Priority", "Profile", "Raw", "RegistryOverride", "RegistryPattern", "Row",
  "RelationGroup", "RepositoryKey", "Role", "RoleNormalized", "Role_Left", "Role_Right", "Rule",
  "Severity", "Source", "SourcePath", "Status", "SurveyStatus", "SurveyMessage",
  "ReaderWarning", "ReaderWarningClass", "ReaderWarningSeverity",
  "ReaderRepairCount", "ReaderRepairLines", "ReaderRepairPolicy",
  "ObservationKind", "IsPartitionColumn", "ObservedType",
  "InferenceConfidence", "PartitionKey", "PartitionValue", "ApprovedType",
  "RecommendedType", "DataRecommendedType", "ObservedTypes", "MergeGroup",
  "MergeReviewed", "RequiresReview", "Decision", "PolicyPattern", "PolicyType",
  "PolicyRole", "PolicyStatus", "PolicyConflict", "PolicyApplied",
  "DecisionOrigin", "RequiredAction", "ActionRequired", "Blocking", "Outcome",
  "CompatibilityApplied", "CompatibilitySignature",
  "RecommendedCommonType", "ApprovedCommonType", "SuggestedRole", "Scope",
  "DeclaredEncoding", "DetectedEncoding", "EncodingConfidence", "EncodingUsed",
  "EncodingDetectionMethod", "EncodingValidationStatus",
  "SourceFingerprint", "TableName", "ToClass",
  "TypeSet", "code", "column_name", "database_table", "diagnosis_column",
  "n_keysets", "n_raw", "n_rows", "n_typesets", "pct_of_table"
))

# Internal globals
# LogPath, CheckpointPath, ParquetBasePath, MasterDBPath, SAV_CHUNK_SIZE,
# SAV_ROW_THRESHOLD, and n_workers are expected to exist in the calling
# environment (set by the loader script before sourcing this file).

################################################################################
#### Log message function ######################################################
################################################################################
#' Write a timestamped message to the console and a log file
#'
#' Opens the log file atomically on every call (open, write, flush, close) so
#' the entry reaches disk immediately.  A persistent file connection is
#' intentionally avoided because R's connection table can be disturbed by
#' C-level errors from \pkg{haven} or \pkg{arrow}, making \code{isOpen()}
#' unreliable after such errors.
#' @param msg Character scalar. The message text to log.
#' @param log_path Character scalar. Path to the log file.  Defaults to
#'   \code{LogPath}, which must exist in the calling environment.
#' @return \code{invisible(NULL)}.  Called for its side effects.
#' @examples
#' \dontrun{
#' tmp_log <- tempfile(fileext = ".txt")
#' log_msg("Loading started", log_path = tmp_log)
#' log_msg("Custom log entry", log_path = tmp_log)
#' readLines(tmp_log)
#' unlink(tmp_log)
#' }
#' @export
.log_env <- new.env(parent = emptyenv())
.log_env$buffers <- new.env(parent = emptyenv())
.run_env <- new.env(parent = emptyenv())
.run_env$run_id <- NA_character_
.run_env$log_path <- NA_character_

new_repository_run_id <- function() {
  paste0(format(Sys.time(), "%Y%m%dT%H%M%S"), "_", Sys.getpid(), "_",
         sprintf("%06d", sample.int(999999L, 1L)))
}

#' Start a run-scoped logging context
#' @export
begin_repository_run <- function(LogPath = NULL, RunId = NULL) {
  previous <- list(run_id = .run_env$run_id, log_path = .run_env$log_path)
  .run_env$run_id <- if (is.null(RunId) || is.na(RunId[1]) || !nzchar(RunId[1])) {
    new_repository_run_id()
  } else {
    as.character(RunId[1])
  }
  .run_env$log_path <- if (is.null(LogPath) || is.na(LogPath[1]) || !nzchar(LogPath[1])) {
    NA_character_
  } else {
    as.character(LogPath[1])
  }
  invisible(previous)
}

restore_repository_run <- function(previous) {
  if (is.null(previous)) return(invisible(NULL))
  .run_env$run_id <- previous$run_id %||% NA_character_
  .run_env$log_path <- previous$log_path %||% NA_character_
  invisible(NULL)
}

resolve_run_id <- function(run_id = NULL) {
  if (!is.null(run_id) && length(run_id) > 0L && !is.na(run_id[1]) && nzchar(run_id[1])) {
    return(as.character(run_id[1]))
  }
  if (!is.na(.run_env$run_id) && nzchar(.run_env$run_id)) .run_env$run_id else NA_character_
}

log_buffer_get <- function(log_path) {
  value <- .log_env$buffers[[log_path]]
  if (is.null(value)) character(0) else value
}

log_buffer_set <- function(log_path, value) {
  if (length(value) == 0L) {
    if (exists(log_path, envir = .log_env$buffers, inherits = FALSE)) {
      rm(list = log_path, envir = .log_env$buffers)
    }
  } else {
    .log_env$buffers[[log_path]] <- value
  }
  invisible(NULL)
}

resolve_log_path <- function(log_path = NULL) {
  if (!is.null(log_path) && length(log_path) > 0L && !is.na(log_path[1]) && nzchar(log_path[1])) {
    return(as.character(log_path[1]))
  }
  frames <- sys.frames()
  for (i in rev(seq_along(frames))) {
    if (exists("LogPath", envir = frames[[i]], inherits = FALSE)) {
      candidate <- get("LogPath", envir = frames[[i]], inherits = FALSE)
      if (length(candidate) > 0L && !is.na(candidate[1]) && nzchar(candidate[1])) return(as.character(candidate[1]))
    }
  }
  if (exists("LogPath", envir = .GlobalEnv, inherits = FALSE)) {
    candidate <- get("LogPath", envir = .GlobalEnv, inherits = FALSE)
    if (length(candidate) > 0L && !is.na(candidate[1]) && nzchar(candidate[1])) return(as.character(candidate[1]))
  }
  if (!is.na(.run_env$log_path) && nzchar(.run_env$log_path)) return(.run_env$log_path)
  file.path(tempdir(), "repoquet_load_log.txt")
}

#' Timestamped atomic log write with connectivity-loss buffering
#'
#' Writes a timestamped message to the console and appends it to
#' \code{log_path}. If the file write fails (e.g. network path temporarily
#' unreachable), the message is held in an in-session memory buffer and
#' flushed to disk automatically on the next call that succeeds. This
#' means no messages are lost within a session when the network hiccups
#' during a large Parquet write -- the buffer drains as soon as
#' connectivity is restored, preserving message order and keeping a
#' single authoritative log file.
#'
#' @param msg    Character scalar. Message body (timestamp is prepended).
#' @param log_path Character. Path to the log file. Defaults to \code{LogPath}.
#' @return \code{invisible(NULL)}. Called for side effects.
#' @export
log_msg <- function(msg, log_path = NULL, run_id = NULL) {
  log_path <- resolve_log_path(log_path)
  run_id <- resolve_run_id(run_id)
  run_tag <- if (is.na(run_id)) "" else sprintf(" [run_id=%s]", run_id)
  line <- sprintf("[%s]%s %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), run_tag, msg)
  cat(line, "\n", sep = "")

  write_err <- tryCatch({
    dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
    fcon <- file(log_path, open = "at")
    on.exit(try(close(fcon), silent = TRUE), add = TRUE)
    buffered <- log_buffer_get(log_path)
    if (length(buffered) > 0L) {
      n_flushed <- length(buffered)
      writeLines(buffered, fcon)
      log_buffer_set(log_path, character(0))
      cat(sprintf("[log] %d buffered message(s) flushed to disk.\n", n_flushed))
    }

    writeLines(line, fcon)
    flush(fcon)
    NULL
  }, error = function(e) e)

  if (!is.null(write_err)) {
    buffered <- c(log_buffer_get(log_path), line)
    log_buffer_set(log_path, buffered)
    if (length(buffered) == 1L) {
      cat(sprintf("[log] Write to '%s' failed (%s). Buffering messages until connectivity restored.\n",
                  log_path, write_err$message))
    } else {
      cat(sprintf("[log] Still buffering -- %d message(s) pending.\n",
                  length(buffered)))
    }
  }
  invisible(NULL)
}

flush_log_buffer <- function(log_path = NULL) {
  log_path <- resolve_log_path(log_path)
  buffered <- log_buffer_get(log_path)
  if (length(buffered) == 0L) {
    cat("[log] No buffered messages to flush.\n")
    return(invisible(TRUE))
  }
  result <- tryCatch({
    dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
    fcon <- file(log_path, open = "at")
    on.exit(try(close(fcon), silent = TRUE), add = TRUE)
    writeLines(buffered, fcon)
    flush(fcon)
    n <- length(buffered)
    log_buffer_set(log_path, character(0))
    cat(sprintf("[log] Flushed %d buffered message(s) to '%s'.\n", n, log_path))
    TRUE
  }, error = function(e) {
    cat(sprintf("[log] Flush failed: %s -- %d message(s) still buffered.\n",
                e$message, length(log_buffer_get(log_path))))
    FALSE
  })
  invisible(result)
}

repository_table_name_for_row <- function(row_meta) {
  explicit <- if ("PhysicalTableName" %in% names(row_meta)) as.character(row_meta$PhysicalTableName[1]) else NA_character_
  if (!is.na(explicit) && nzchar(trimws(explicit))) return(trimws(explicit))
  paste(as.character(row_meta$Database[1]), as.character(row_meta$TableName[1]), sep = "_")
}

repository_table_names <- function(MDT) {
  vapply(seq_len(nrow(MDT)), function(i) repository_table_name_for_row(MDT[i, , drop = FALSE]), character(1))
}

.fingerprint_env <- new.env(parent = emptyenv())

#' Fingerprint a source file for checkpoint invalidation
#' @export
source_fingerprint <- function(path, mode = c("metadata", "sha256", "none")) {
  mode <- match.arg(mode)
  info <- file.info(path)
  if (nrow(info) == 0L || is.na(info$size[1])) {
    return(list(path = path, size = NA_real_, mtime_utc = NA_character_, sha256 = NA_character_,
                fingerprint = NA_character_, mode = mode))
  }
  size <- as.numeric(info$size[1])
  mtime_utc <- format(as.POSIXct(info$mtime[1], tz = "UTC"), "%Y-%m-%dT%H:%M:%OS6Z", tz = "UTC")
  sha <- NA_character_
  if (mode == "sha256") {
    if (!requireNamespace("digest", quietly = TRUE)) {
      stop("SourceFingerprintMode='sha256' requires the 'digest' package.")
    }
    cache_key <- paste(normalizePath(path, winslash = "/", mustWork = FALSE), size, mtime_utc, sep = "||")
    sha <- .fingerprint_env[[cache_key]]
    if (is.null(sha)) {
      sha <- digest::digest(file = path, algo = "sha256", serialize = FALSE)
      .fingerprint_env[[cache_key]] <- sha
    }
  }
  fp <- switch(mode,
               none = NA_character_,
               metadata = paste0("meta:", size, ":", mtime_utc),
               sha256 = paste0("sha256:", sha))
  list(path = path, size = size, mtime_utc = mtime_utc, sha256 = sha,
       fingerprint = fp, mode = mode)
}

source_path_for_row <- function(row_meta, MasterDBPath) {
  file.path(MasterDBPath, as.character(row_meta$MDBDir[1]), as.character(row_meta$Path[1]))
}

repository_checkpoint_key <- function(MDT, MasterDBPath = NULL,
                                      SourceFingerprintMode = c("none", "metadata", "sha256")) {
  SourceFingerprintMode <- match.arg(SourceFingerprintMode)
  required <- c("Database", "TableName", "MDBDir", "Path")
  missing_required <- setdiff(required, names(MDT))
  if (length(missing_required) > 0L) {
    stop(sprintf("Cannot build repository checkpoint key; missing columns: %s",
                 paste(missing_required, collapse = ", ")))
  }
  #### Keep classic YEAR keys bit-identical to the historical format. For   ####
  #### generalized partitions, include each key name as well as its value so ####
  #### changing SITE=MGH to FACILITY=MGH cannot falsely reuse a checkpoint. ####
  partition_identity <- vapply(seq_len(nrow(MDT)), function(i) {
    spec <- partition_spec_for_row(MDT[i, , drop = FALSE])
    if (identical(spec$keys, "YEAR")) {
      paste(spec$values, collapse = ";")
    } else {
      paste(paste0(spec$keys, "=", spec$values), collapse = ";")
    }
  }, character(1))
  base_keys <- paste(MDT$Database, MDT$TableName, partition_identity, MDT$MDBDir, MDT$Path, sep = "||")
  if (SourceFingerprintMode == "none") return(base_keys)
  if (is.null(MasterDBPath) || !nzchar(MasterDBPath)) {
    stop("MasterDBPath is required when SourceFingerprintMode is not 'none'.")
  }
  fingerprints <- vapply(seq_len(nrow(MDT)), function(i) {
    source_fingerprint(source_path_for_row(MDT[i, , drop = FALSE], MasterDBPath),
                       mode = SourceFingerprintMode)$fingerprint
  }, character(1))
  paste0(base_keys, "||SOURCE=", fingerprints)
}

repository_checkpoint_legacy_key <- function(MDT) {
  vals <- vapply(seq_len(nrow(MDT)), function(i) {
    paste(partition_spec_for_row(MDT[i, , drop = FALSE])$values, collapse = ";")
  }, character(1))
  paste(MDT$Database, MDT$TableName, vals, MDT$MDBDir, MDT$Path, sep = "||")
}

checkpoint_completed_mask <- function(MDT, completed_checkpoint, accept_legacy = TRUE,
                                      MasterDBPath = NULL,
                                      SourceFingerprintMode = c("none", "metadata", "sha256")) {
  SourceFingerprintMode <- match.arg(SourceFingerprintMode)
  if (length(completed_checkpoint) == 0L || nrow(MDT) == 0L) return(rep(FALSE, nrow(MDT)))
  checkpoint_compare <- if (SourceFingerprintMode == "none") {
    sub("\\|\\|SOURCE=.*$", "", as.character(completed_checkpoint))
  } else {
    as.character(completed_checkpoint)
  }
  keys <- repository_checkpoint_key(MDT, MasterDBPath = MasterDBPath,
                                    SourceFingerprintMode = SourceFingerprintMode)
  hit <- keys %in% checkpoint_compare
  #### Legacy acceptance (value-only keys, bare paths) exists for migration ####
  #### only: value-only keys cannot distinguish SITE=MGH from FACILITY=MGH. ####
  #### Run migrate_checkpoint_keys() once, then legacy entries disappear    ####
  #### and this branch stops matching anything.                             ####
  if (isTRUE(accept_legacy)) {
    legacy_keys <- repository_checkpoint_legacy_key(MDT)
    path_counts <- table(MDT$Path)
    legacy_path_complete <- MDT$Path %in% checkpoint_compare & as.integer(path_counts[MDT$Path]) == 1L
    hit <- hit | legacy_keys %in% checkpoint_compare | legacy_path_complete
  }
  hit
}

#' Upgrade legacy checkpoint entries to content-aware source fingerprints
#' @export
upgrade_checkpoint_source_fingerprints <- function(checkpoint, MDT, MasterDBPath,
                                                   SourceFingerprintMode = c("metadata", "sha256")) {
  SourceFingerprintMode <- match.arg(SourceFingerprintMode)
  if (length(checkpoint) == 0L || nrow(MDT) == 0L) return(unique(checkpoint))
  current <- repository_checkpoint_key(MDT, MasterDBPath, SourceFingerprintMode)
  base <- repository_checkpoint_key(MDT, SourceFingerprintMode = "none")
  legacy <- repository_checkpoint_legacy_key(MDT)
  candidates <- c(base, legacy)
  replacements <- rep(current, 2L)
  out <- checkpoint
  idx <- match(out, candidates)
  out[!is.na(idx)] <- replacements[idx[!is.na(idx)]]
  path_counts <- table(MDT$Path)
  unique_path_rows <- which(as.integer(path_counts[MDT$Path]) == 1L)
  pidx <- match(out, MDT$Path[unique_path_rows])
  out[!is.na(pidx)] <- current[unique_path_rows[pidx[!is.na(pidx)]]]
  unique(out)
}

#' Rewrite legacy checkpoint entries into the generalized key format
#'
#' Checkpoints written before generalized keys existed contain value-only
#' identities (\code{DB||Table||2019||dir||path}) or bare paths. Those formats
#' cannot distinguish two partition schemes that share values (e.g.
#' \code{SITE=MGH} vs \code{FACILITY=MGH}), so
#' \code{\link{checkpoint_completed_mask}} only tolerates them as a migration
#' bridge. This helper rewrites every legacy entry the current MDT can explain
#' into the generalized format, after which \code{accept_legacy = FALSE} can be
#' used. Entries no MDT row explains are left untouched and reported --
#' investigate those with \code{\link{audit_repository}}.
#' @param CheckpointPath Character. Checkpoint .rds path.
#' @param MDT Data frame. Current Master Database Table.
#' @param DryRun Logical. TRUE (default) reports without writing.
#' @return Invisibly, list(n_migrated, n_unexplained).
#' @export
migrate_checkpoint_keys <- function(CheckpointPath, MDT, DryRun = TRUE) {
  checkpoint <- load_checkpoint(path = CheckpointPath)
  if (length(checkpoint) == 0L) {
    log_msg("[MIGRATE] Checkpoint is empty -- nothing to migrate.")
    return(invisible(list(n_migrated = 0L, n_unexplained = 0L)))
  }
  new_keys <- repository_checkpoint_key(MDT)
  legacy_keys <- repository_checkpoint_legacy_key(MDT)
  path_counts <- table(MDT$Path)
  unique_paths <- names(path_counts)[path_counts == 1L]
  migrated <- checkpoint
  hit_legacy <- match(checkpoint, legacy_keys)
  migrated[!is.na(hit_legacy)] <- new_keys[hit_legacy[!is.na(hit_legacy)]]
  hit_path <- match(checkpoint, intersect(MDT$Path, unique_paths))
  path_rows <- match(intersect(MDT$Path, unique_paths), MDT$Path)
  migrated[!is.na(hit_path)] <- new_keys[path_rows[hit_path[!is.na(hit_path)]]]
  n_migrated <- sum(migrated != checkpoint)
  unexplained <- setdiff(migrated, new_keys)
  log_msg(sprintf("[MIGRATE]%s %d legacy checkpoint entrie(s) rewritten to generalized keys; %d entrie(s) match no current MDT row (left untouched).",
                  if (DryRun) " (dry run)" else "", n_migrated, length(unexplained)))
  if (!DryRun && n_migrated > 0L) {
    save_checkpoint(unique(migrated), CheckpointPath)
    log_msg("[MIGRATE] Checkpoint saved. You may now call loaders with accept_legacy = FALSE semantics.")
  }
  invisible(list(n_migrated = n_migrated, n_unexplained = length(unexplained)))
}

regex_escape <- function(x) {
  gsub("([][{}()+*^$|\\\\?.])", "\\\\\\1", x)
}

parquet_safe_stem <- function(source_path) {
  gsub("[^a-zA-Z0-9]", "_", basename(source_path))
}

parquet_output_stem <- function(source_path, partition_dir = NULL, MaxFileStemTruncate = FALSE) {
  safe_stem <- parquet_safe_stem(source_path)
  if (isTRUE(MaxFileStemTruncate) && !is.null(partition_dir) && nzchar(partition_dir)) {
    max_stem <- max(10L, 255L - nchar(partition_dir) - 9L)
    if (nchar(safe_stem) > max_stem) safe_stem <- substr(safe_stem, 1L, max_stem)
  }
  safe_stem
}

parquet_chunk_stem <- function(source_path, partition_dir = NULL,
                               TerminalHivePartition = FALSE,
                               MaxFileStemTruncate = FALSE) {
  safe_stem <- gsub("[^a-zA-Z0-9]", "_", tools::file_path_sans_ext(basename(source_path)))
  if (isTRUE(MaxFileStemTruncate) && !is.null(partition_dir) && nzchar(partition_dir)) {
    suffix_len <- if (isTRUE(TerminalHivePartition)) {
      nchar(file.path(sprintf("batch_id=%s_%05d", "", 1L), "data.parquet"))
    } else {
      nchar("_00001.parquet")
    }
    suffix_len <- suffix_len + 1L
    max_stem <- max(10L, 255L - nchar(partition_dir) - suffix_len)
    if (nchar(safe_stem) > max_stem) safe_stem <- substr(safe_stem, 1L, max_stem)
  }
  safe_stem
}

#' Verify that a source file truly contains zero data rows
#'
#' Distinguishes a legitimately empty source (readable, 0 rows) from a failed
#' read, which also surfaces as an empty data frame in the safe readers. Only
#' a successful re-read confirming 0 rows returns TRUE; any read error returns
#' FALSE so genuine failures keep failing loudly.
#' @export
verify_source_empty <- function(full_path, reader, reader_options = list()) {
  tryCatch({
    rd <- get_file_reader(reader)
    n <- if (!is.null(rd$count_rows)) {
      call_reader(rd, "count_rows", full_path, reader_options = reader_options)
    } else {
      nrow(call_reader(rd, "read_full", full_path, reader_options = reader_options))
    }
    isTRUE(n == 0L)
  }, error = function(e) FALSE)
}

write_csv_safely <- function(x, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp <- tempfile(pattern = paste0(basename(path), ".tmp_"), tmpdir = dirname(path))
  data.table::fwrite(x, tmp)
  replace_file_safely(tmp, path)
  invisible(path)
}

write_xlsx_safely <- function(x, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp <- tempfile(pattern = paste0(basename(path), ".tmp_"), tmpdir = dirname(path),
                  fileext = paste0(".", tools::file_ext(path)))
  openxlsx::write.xlsx(x, file = tmp, overwrite = TRUE)
  replace_file_safely(tmp, path)
  invisible(path)
}

write_arrow_table_safely <- function(arrow_tbl, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  tmp <- tempfile(pattern = paste0(basename(path), ".tmp_"), tmpdir = dirname(path),
                  fileext = ".parquet")
  tryCatch({
    arrow::write_parquet(arrow_tbl, tmp)
    replace_file_safely(tmp, path)
  }, error = function(e) {
    if (file.exists(tmp)) unlink(tmp)
    stop(e)
  })
  invisible(path)
}

replace_file_safely <- function(tmp, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  backup <- tempfile(pattern = paste0(basename(path), ".bak_"), tmpdir = dirname(path))
  had_old <- file.exists(path)
  old_backed_up <- FALSE
  tryCatch({
    if (had_old) {
      old_backed_up <- file.rename(path, backup)
      if (!old_backed_up) {
        old_backed_up <- file.copy(path, backup, overwrite = TRUE)
        if (!old_backed_up || !file.remove(path)) {
          stop(sprintf("Could not move existing file aside before replacement: %s", path))
        }
      }
    }
    ok <- file.rename(tmp, path)
    if (!ok) {
      ok <- file.copy(tmp, path, overwrite = TRUE)
      if (ok) unlink(tmp)
    }
    if (!ok || !file.exists(path)) stop(sprintf("Could not replace file: %s", path))
    if (old_backed_up && file.exists(backup)) unlink(backup)
    invisible(TRUE)
  }, error = function(e) {
    if (had_old && old_backed_up && !file.exists(path) && file.exists(backup)) {
      file.rename(backup, path)
    }
    stop(e)
  }, finally = {
    if (file.exists(tmp)) unlink(tmp)
  })
}

################################################################################
#### Hive partition specification ##############################################
################################################################################
#### Each MDT row maps one source file to exactly one hive partition          ####
#### directory. The optional MDT columns PartitionKey / PartitionValue drive  ####
#### this: blank or absent means the classic YEAR partition using the row's   ####
#### Year value, so existing workbooks behave identically. Multi-level        ####
#### partitions use ";" in both fields ("SITE;YEAR" / "MGH;2019" ->           ####
#### SITE=MGH/year-style nested directories).                                 ####

#' Sanitize a value for use in a hive partition directory name
#' @export
sanitize_partition_value <- function(x) {
  x <- trimws(as.character(x))
  gsub("[^A-Za-z0-9_.\\-]", "_", x)
}

#' Resolve the hive partition keys and values for one MDT row
#'
#' Returns the partition specification driving where a source file's Parquet
#' output lands. Defaults reproduce the historical behavior: partition key
#' \code{YEAR} with the row's \code{Year} as its value. When
#' \code{PartitionValue} is blank the key must be \code{YEAR} (there is nothing
#' to default a non-year value to).
#' @param row_meta One row of the MDT (data.frame or data.table).
#' @return A list with \code{keys} (canonical uppercase character vector),
#'   \code{values} (sanitized character vector, same length), and \code{dir}
#'   (the relative partition directory, e.g. \code{"site=MGH/year=2019"}).
#'   Directory key names are lowercased to match the historical \code{year=}
#'   layout; DuckDB matches hive partition columns case-insensitively.
#' @seealso \code{\link{ValidateMDTPreflight}} which enforces per-table key
#'   consistency before any file is written.
#' @export
partition_spec_for_row <- function(row_meta) {
  split_spec <- function(x) trimws(strsplit(as.character(x), ";", fixed = TRUE)[[1]])
  raw_key <- if ("PartitionKey" %in% names(row_meta)) as.character(row_meta$PartitionKey[1]) else NA_character_
  raw_val <- if ("PartitionValue" %in% names(row_meta)) as.character(row_meta$PartitionValue[1]) else NA_character_
  keys <- if (is.na(raw_key) || !nzchar(trimws(raw_key))) "YEAR" else canonical_colnames(split_spec(raw_key))
  invalid_keys <- !grepl("^[A-Z][A-Z0-9_]*$", keys)
  if (any(invalid_keys)) {
    stop(sprintf("Invalid PartitionKey name(s) for %s: %s. Use letters, digits, and underscores, beginning with a letter.",
                 row_meta$Path[1], paste(keys[invalid_keys], collapse = ", ")))
  }
  if (anyDuplicated(keys)) {
    stop(sprintf("PartitionKey contains duplicate level name(s) for %s: %s.",
                 row_meta$Path[1], paste(keys[duplicated(keys)], collapse = ", ")))
  }
  if (is.na(raw_val) || !nzchar(trimws(raw_val))) {
    if (!identical(keys, "YEAR")) {
      stop(sprintf("PartitionValue is blank for %s but PartitionKey is '%s'; only the default YEAR partition can fall back to the Year column.",
                   paste(row_meta$Path[1], collapse = ""), paste(keys, collapse = ";")))
    }
    fallback_year <- if ("Year" %in% names(row_meta)) row_meta$Year[1] else NA
    if (is.na(fallback_year) || !nzchar(trimws(as.character(fallback_year)))) {
      stop(sprintf("Row for %s has neither a PartitionValue nor a Year column to fall back to. Populate PartitionKey/PartitionValue in the MDT workbook.",
                   row_meta$Path[1]))
    }
    values <- as.character(fallback_year)
  } else {
    values <- split_spec(raw_val)
  }
  if (length(keys) != length(values)) {
    stop(sprintf("PartitionKey ('%s') and PartitionValue ('%s') disagree on the number of levels for %s.",
                 paste(keys, collapse = ";"), paste(values, collapse = ";"), row_meta$Path[1]))
  }
  values <- sanitize_partition_value(values)
  if (any(!nzchar(values))) {
    stop(sprintf("Empty partition value after sanitization for %s (PartitionKey '%s').",
                 row_meta$Path[1], paste(keys, collapse = ";")))
  }
  list(keys = keys, values = values,
       dir = paste(paste0(tolower(keys), "=", values), collapse = "/"))
}

partition_types_for_row <- function(row_meta, spec = partition_spec_for_row(row_meta)) {
  raw <- if ("PartitionType" %in% names(row_meta)) as.character(row_meta$PartitionType[1]) else NA_character_
  if (is.na(raw) || !nzchar(trimws(raw))) {
    return(ifelse(spec$keys == "YEAR", "integer", "character"))
  }
  types <- trimws(strsplit(raw, ";", fixed = TRUE)[[1]])
  if (length(types) != length(spec$keys)) {
    stop(sprintf("PartitionType has %d value(s) but PartitionKey has %d for %s.",
                 length(types), length(spec$keys), row_meta$Path[1]))
  }
  types <- vapply(types, normalize_type_name, character(1))
  if (any(!is_allowed_canonical_type(types))) {
    stop(sprintf("Unsupported PartitionType for %s: %s", row_meta$Path[1], paste(types, collapse = ";")))
  }
  types
}

table_partition_types <- function(rows) {
  specs <- lapply(seq_len(nrow(rows)), function(i) partition_spec_for_row(rows[i, , drop = FALSE]))
  resolved <- lapply(seq_len(nrow(rows)), function(i) partition_types_for_row(rows[i, , drop = FALSE], specs[[i]]))
  signatures <- vapply(resolved, paste, collapse = ";", character(1))
  if (length(unique(signatures)) != 1L) {
    stop(sprintf("Table %s/%s declares inconsistent PartitionType values: %s",
                 rows$Database[1], rows$TableName[1], paste(unique(signatures), collapse = " vs ")))
  }
  resolved[[1]]
}

#' Validate a physical partition column's contents against the workbook value
#'
#' The writers drop partition columns from the data because hive injects them
#' from the directory name at read time. Before that column is destroyed, its
#' contents must agree with the workbook's PartitionValue -- otherwise a
#' mis-labeled MDT row (e.g. a copy-paste year error) would silently assign
#' every row of the file to the wrong partition with the physical evidence
#' deleted. Values are compared after the same canonicalization the directory
#' names use; NA and empty-string entries carry no information and are
#' tolerated. Any other disagreement is a hard error naming the file, the
#' expected value, and examples of what was found.
#' @export
validate_partition_column_values <- function(df, partition_keys, partition_values,
                                             source_label = "<unknown source>") {
  if (is.null(partition_values) || length(partition_values) == 0L) return(invisible(TRUE))
  keys <- canonical_colnames(partition_keys)
  if (length(keys) != length(partition_values)) return(invisible(TRUE))
  canon <- function(v) {
    v <- v[!is.na(v)]
    if (length(v) == 0L) return(character(0))
    if (is.numeric(v)) {
      iv <- suppressWarnings(as.integer(round(v)))
      s <- ifelse(!is.na(iv) & abs(v - iv) < 1e-9, as.character(iv), as.character(v))
    } else {
      s <- trimws(as.character(v))
      s <- s[nzchar(s)]
      if (length(s) == 0L) return(character(0))
    }
    unique(sanitize_partition_value(s))
  }
  df_canon <- canonical_colnames(names(df))
  for (i in seq_along(keys)) {
    ci <- match(keys[i], df_canon)
    if (is.na(ci)) next
    found <- canon(df[[ci]])
    expected <- sanitize_partition_value(as.character(partition_values[i]))
    bad <- setdiff(found, expected)
    if (length(bad) > 0L) {
      stop(sprintf("Partition column %s in %s contains value(s) [%s] that disagree with the workbook partition value '%s'. Refusing to drop the column and silently relabel these rows -- fix the MDT row or the source file.",
                   keys[i], source_label, paste(utils::head(bad, 5L), collapse = ", "), expected))
    }
  }
  invisible(TRUE)
}

#' Resolve the (validated) partition keys shared by all rows of one table
#'
#' All MDT rows of a Database/TableName group must declare the identical
#' partition key list -- otherwise the table's directory tree would disagree
#' with itself about its partition columns and the DuckDB view would break.
#' @param rows_tbl MDT rows for a single Database/TableName group.
#' @return Character vector of canonical partition key names (e.g. "YEAR").
#' @export
table_partition_keys <- function(rows_tbl) {
  specs <- lapply(seq_len(nrow(rows_tbl)), function(i) partition_spec_for_row(rows_tbl[i, ]))
  key_sets <- unique(vapply(specs, function(s) paste(s$keys, collapse = ";"), character(1)))
  if (length(key_sets) > 1L) {
    stop(sprintf("Table %s/%s mixes partition key sets (%s). All rows of a table must share one PartitionKey.",
                 paste(unique(rows_tbl$Database), collapse = ","), paste(unique(rows_tbl$TableName), collapse = ","),
                 paste(key_sets, collapse = " vs ")))
  }
  strsplit(key_sets, ";", fixed = TRUE)[[1]]
}

##########################
#### Check run status ####
##########################
#' Check MDT completion status against the loaded checkpoint
#'
#' Compares the Master Database Table (\code{MDT}) against the completed-files
#' checkpoint to identify which files have not yet been loaded.  Also detects
#' duplicate \code{Year:basename(Path)} entries in \code{MDT}, which would
#' cause silent overwrite collisions during loading.
#' @param MDT Data frame. Master Database Table with at minimum columns
#'   \code{Year} and \code{Path}.
#' @param CheckpointPath Character. Path to the checkpoint \code{.rds} file,
#'   read via \code{\link{load_checkpoint}}.
#' @param verbose Logical. If \code{TRUE} (default), logs or prints the number
#'   of already-completed files.
#' @param logStatus Logical. If \code{TRUE} (default), messages are written via
#'   \code{\link{log_msg}}; otherwise they are printed to the console.
#' @return A data frame: the subset of \code{MDT} rows whose \code{Path} has
#'   not yet appeared in the checkpoint (i.e. files still pending).
#' @seealso \code{\link{load_checkpoint}}, \code{\link{SummaryVerification}}
#' @examples
#' \dontrun{
#' MDT <- data.frame(Year = 2018:2019,
#'                   Path = c("DEMO_2018_Core.sav", "DEMO_2019_Core.sav"),
#'                   stringsAsFactors = FALSE)
#' tmp_cp <- tempfile(fileext = ".rds")
#' saveRDS("DEMO_2018_Core.sav", tmp_cp)   # 2018 already done
#' missing <- MDTCompleteStatus(MDT, tmp_cp)
#' missing$Path   # "DEMO_2019_Core.sav"
#' unlink(tmp_cp)
#' }
#' @export
MDTCompleteStatus <- function(MDT, CheckpointPath, verbose = TRUE, logStatus = TRUE){
  t <- repository_checkpoint_key(MDT)
  print(paste("There are", length(t[duplicated(t)]), "name duplications in MDT"))
  if(length(t[duplicated(t)]) > 0){
    print(t[duplicated(t)])
  }
  completed_checkpoint  <- load_checkpoint(path = CheckpointPath)
  if(verbose){
    if (length(completed_checkpoint) > 0) {
      if(logStatus){
      log_msg(sprintf("Resuming - %d files already completed from prior run", length(completed_checkpoint)))
      } else {
      sprintf("Resuming - %d files already completed from prior run", length(completed_checkpoint))
      }
      }
  }
  completed_mask <- checkpoint_completed_mask(MDT, completed_checkpoint)
  print(paste("There are", nrow(MDT[!completed_mask,]), "files in MDT that have not been completed"))
  Missing <- MDT[!completed_mask,];
  return(Missing)
}

#' Scan source directories for files not yet in the MDT workbook
#'
#' New-release onboarding helper: walks every source directory the workbook
#' already references, finds loader-compatible files (\code{.sav}/\code{.csv})
#' that have no MDT row, and emits candidate rows with the Database, TableName
#' and year guessed from filename patterns. Nothing is written to the workbook
#' -- the return value (optionally saved via \code{OutputPath}) is a proposal
#' for a human to review, correct, and paste into DBSetupV2.xlsx.
#'
#' Guessing rules: \code{Database} is the workbook's dominant database for
#' that \code{MDBDir}; \code{TableName} is the longest known table name of
#' that database whose normalized form appears in the filename;
#' \code{PartitionValue} is the four-digit year found in the basename (falling
#' back to the deepest year-bearing directory component). Rows where either
#' guess fails are flagged \code{NeedsReview = TRUE}.
#' @param MasterDBPath Character. Root directory of the source files.
#' @param MDT Data frame. Current Master Database Table.
#' @param extensions File extensions to consider (default sav, csv).
#' @param OutputPath Optional .xlsx/.csv path to save the candidate rows to.
#' @return data.table of candidate MDT rows: Database, MDBDir, Path,
#'   TableName, FileType, PartitionKey, PartitionValue, NeedsReview, Note.
#' @export
scan_for_new_source_files <- function(MasterDBPath, MDT, extensions = supported_file_types(),
                                      OutputPath = NULL) {
  MDTdt <- data.table::as.data.table(MDT)
  norm_token <- function(x) gsub("[^A-Z0-9]", "", toupper(x))
  guess_year <- function(rel_path) {
    base_hit <- regmatches(basename(rel_path), gregexpr("(19|20)[0-9]{2}", basename(rel_path)))[[1]]
    if (length(base_hit) > 0L) return(base_hit[length(base_hit)])
    dir_bits <- strsplit(dirname(rel_path), "/", fixed = TRUE)[[1]]
    for (bit in rev(dir_bits)) {
      hit <- regmatches(bit, gregexpr("(19|20)[0-9]{2}", bit))[[1]]
      if (length(hit) > 0L) return(hit[length(hit)])
    }
    NA_character_
  }
  out_rows <- list()
  for (mdb in unique(as.character(MDTdt$MDBDir))) {
    root <- file.path(MasterDBPath, mdb)
    if (!dir.exists(root)) {
      log_msg(sprintf("[SCAN] MDBDir not found on disk, skipped: %s", root))
      next
    }
    found <- list.files(root, recursive = TRUE, full.names = FALSE)
    found <- gsub("\\\\", "/", found)
    found <- found[tolower(tools::file_ext(found)) %in% tolower(extensions)]
    known <- gsub("\\\\", "/", as.character(MDTdt[MDTdt$MDBDir == mdb, ]$Path))
    new_rel <- setdiff(found, known)
    if (length(new_rel) == 0L) next
    db_tab <- sort(table(as.character(MDTdt[MDTdt$MDBDir == mdb, ]$Database)), decreasing = TRUE)
    db_guess <- names(db_tab)[1]
    known_tables <- unique(as.character(MDTdt[MDTdt$Database == db_guess, ]$TableName))
    known_norm <- norm_token(known_tables)
    ord <- order(nchar(known_norm), decreasing = TRUE)   # longest name wins
    known_tables <- known_tables[ord]; known_norm <- known_norm[ord]
    for (rel in sort(new_rel)) {
      stem_norm <- norm_token(tools::file_path_sans_ext(basename(rel)))
      tbl_guess <- NA_character_
      for (k in seq_along(known_norm)) {
        if (nzchar(known_norm[k]) && grepl(known_norm[k], stem_norm, fixed = TRUE)) {
          tbl_guess <- known_tables[k]; break
        }
      }
      yr_guess <- guess_year(rel)
      needs_review <- is.na(tbl_guess) || is.na(yr_guess)
      note <- paste(c(if (is.na(tbl_guess)) "no known TableName matched the filename",
                      if (is.na(yr_guess)) "no 4-digit year found in path"), collapse = "; ")
      out_rows[[length(out_rows) + 1L]] <- data.table::data.table(
        Database = db_guess, MDBDir = mdb, Path = rel,
        TableName = tbl_guess, FileType = tolower(tools::file_ext(rel)),
        PartitionKey = "year", PartitionValue = yr_guess,
        NeedsReview = needs_review, Note = if (nzchar(note)) note else NA_character_)
    }
  }
  out <- if (length(out_rows) > 0L) data.table::rbindlist(out_rows) else
    data.table::data.table(Database = character(), MDBDir = character(), Path = character(),
                           TableName = character(), FileType = character(), PartitionKey = character(),
                           PartitionValue = character(), NeedsReview = logical(), Note = character())
  log_msg(sprintf("[SCAN] %d candidate new source file(s) found (%d flagged NeedsReview).",
                  nrow(out), sum(out$NeedsReview)))
  if (!is.null(OutputPath) && nrow(out) > 0L) {
    if (is_excel_workbook_path(OutputPath)) write_xlsx_safely(list(NewSourceFiles = out), OutputPath)
    else write_csv_safely(out, OutputPath)
    log_msg(sprintf("[SCAN] Candidate rows written for review: %s", OutputPath))
  }
  out[]
}

################################################################################
#### Parquet helper functions ##################################################
################################################################################
canonical_type_to_arrow <- function(type, fallback = NULL) {
  type <- normalize_type_name(type)
  if (grepl("^decimal\\([0-9]+,[0-9]+\\)$", type)) {
    nums <- as.integer(strsplit(sub("^decimal\\(|\\)$", "", type), ",", fixed = TRUE)[[1]])
    return(arrow::decimal128(nums[1], nums[2]))
  }
  switch(type,
         character = arrow::utf8(), integer = arrow::int32(), int64 = arrow::int64(),
         numeric = arrow::float64(), logical = arrow::bool(), Date = arrow::date32(),
         POSIXct = arrow::timestamp("us", timezone = "UTC"), time = arrow::time64("us"),
         duration = arrow::duration("us"), binary = arrow::binary(),
         list = arrow::list_of(arrow::utf8()), fallback)
}

arrow_schema_from_classes <- function(arrow_tbl, col_classes = NULL) {
  if (is.null(col_classes) || length(col_classes) == 0L) return(arrow_tbl$schema)
  names(col_classes) <- canonical_colnames(names(col_classes))
  fields <- lapply(arrow_tbl$schema$fields, function(field) {
    target <- col_classes[[field$name]]
    arrow::field(field$name, if (is.null(target)) field$type else canonical_type_to_arrow(target, field$type),
                 nullable = field$nullable)
  })
  arrow::schema(fields)
}

#' Write a data frame to a hive-partitioned Parquet file
#'
#' Writes \code{df} to a Parquet file under a \code{year=<year_val>}
#' subdirectory of \code{file.path(ParquetBasePath, table_name)}.  The output
#' filename is derived from \code{source_path} with non-alphanumeric characters
#' replaced by underscores, so each source SAV/CSV file produces a uniquely
#' named Parquet file.
#' Before writing, the function:
#' \enumerate{
#'   \item Converts \code{df} to a \code{data.table} in-place via
#'     \code{strip_haven()} if needed.
#'   \item Coerces any column where the agreed class in \code{col_classes} is
#'     \code{"character"} but the actual class differs.
#'   \item Replaces \code{Inf} / \code{-Inf} values in numeric columns with
#'     \code{NA_real_} (Arrow cannot serialise non-finite doubles).
#' }
#' @param df A data frame or data.table to write.
#' @param ParquetBasePath Character. Root directory of the Parquet store.
#' @param table_name Character. Table name used as the first directory level
#'   (e.g. \code{"NIS_Core"}).
#' @param year_val Integer or character. Year value used as the hive
#'   partition key (e.g. \code{2019}).
#' @param source_path Character. Original source file path; used to derive
#'   the output filename.
#' @param col_classes Named list (optional). Maps column names to their
#'   agreed R class strings (e.g. \code{list(AGE = "integer")}).
#' @return \code{invisible(out_path)} where \code{out_path} is the full path
#'   of the written Parquet file.
#' @seealso \code{\link{safe_read_sav}}, \code{\link{strip_haven}}
#' @examples
#' \dontrun{
#' tmp_base <- tempfile("parquet_demo_")
#' dir.create(tmp_base)
#' set.seed(1)
#' my_df <- data.frame(AGE = sample(18:90, 100, replace = TRUE),
#'                     SEX = sample(c("M", "F"), 100, replace = TRUE),
#'                     COST = round(rnorm(100, mean = 5000, sd = 1500), 2) )
#' out_path <- write_year_parquet(df = my_df,
#'                                ParquetBasePath = tmp_base,
#'                                table_name = "DEMO_Core",
#'                                year_val = 2020,
#'                                source_path = "DEMO_2020_Core.sav" )
#' arrow::read_parquet(out_path)
#' unlink(tmp_base, recursive = TRUE)
#' }
#' @export
write_year_parquet <- function(df, ParquetBasePath, table_name, year_val, source_path,
                               col_classes = NULL, MaxFileStemTruncate = FALSE,
                               partition_keys = "YEAR", partition_values = as.character(year_val),
                               max_coerce_na_pct = NULL){
  partition_dir <- paste(paste0(tolower(partition_keys), "=", sanitize_partition_value(partition_values)), collapse = "/")
  year_dir  <- file.path(ParquetBasePath, table_name, partition_dir)
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  safe_stem <- parquet_output_stem(source_path, partition_dir = year_dir,
                                   MaxFileStemTruncate = MaxFileStemTruncate)
  fname    <- paste0(safe_stem, ".parquet")
  out_path <- file.path(year_dir, fname)
  if (!data.table::is.data.table(df)) df <- strip_haven(df)
  data.table::setDT(df)
  df <- canonicalize_dataframe_names(df)
  #### Partition columns live in the directory names; hive injects them at ####
  #### read time, so they must not also be written into the file. Their    ####
  #### contents must agree with the workbook value before they are dropped. ####
  validate_partition_column_values(df, partition_keys, partition_values,
                                   source_label = basename(source_path))
  for (pk in intersect(canonical_colnames(partition_keys), names(df))) df[, (pk) := NULL]
  df <- enforce_col_classes(df, col_classes, max_coerce_na_pct = max_coerce_na_pct)
  num_cols <- names(df)[sapply(df, is.numeric)]
  for(col in num_cols){
    vals     <- df[[col]]
    bad_idx  <- which(!is.finite(vals) & !is.na(vals))
    if (length(bad_idx) > 0) {
      data.table::set(df, i = bad_idx, j = col, value = NA_real_)
    }
    rm(vals)
  }
  gc(verbose = FALSE)
  arrow_tbl <- arrow::as_arrow_table(df)
  target_schema <- arrow_schema_from_classes(arrow_tbl, col_classes)
  arrow_tbl <- tryCatch(arrow_tbl$cast(target_schema), error = function(e) {
    stop(sprintf("Could not cast %s to the resolved physical Parquet schema: %s",
                 basename(source_path), conditionMessage(e)))
  })
  rm(df)
  write_attempts <- 3L
  write_wait_s   <- c(0L, 10L, 30L)
  tmp_path <- NULL
  for (attempt in seq_len(write_attempts)) {
    write_err <- tryCatch({
      if (attempt > 1L) {
        log_msg(sprintf("[WRITE RETRY] Attempt %d/%d after %ds pause: %s",
                        attempt, write_attempts, write_wait_s[attempt], basename(out_path)))
        Sys.sleep(write_wait_s[attempt])
      }
      tmp_path <- tempfile(pattern = paste0(basename(out_path), ".tmp_"),
                           tmpdir = year_dir, fileext = ".parquet")
      arrow::write_parquet(arrow_tbl, tmp_path)
      replace_file_safely(tmp_path, out_path)
      tmp_path <- NULL
      NULL
    }, error = function(e) e)
    if (is.null(write_err)) break
    if (!is.null(tmp_path) && file.exists(tmp_path)) unlink(tmp_path)
    if (attempt == write_attempts)
      stop(write_err)
    log_msg(sprintf("[WRITE RETRY] Write attempt %d failed: %s", attempt, write_err$message))
  }
  invisible(out_path)
}

#####################################################################
#### Register a DuckDB VIEW over all Parquet files for one table ####
#####################################################################
#' Register a DuckDB VIEW over all Parquet files for one table
#'
#' Creates (or replaces) a DuckDB \code{VIEW} named \code{table_name} that
#' reads all \code{*.parquet} files under the corresponding directory via
#' \code{read_parquet()} with hive partitioning and schema union enabled.
#' This allows DuckDB to query all years of a table as a single virtual table.
#' @param con A DBI connection to an open DuckDB database.
#' @param table_name Character. Name of the table / view (e.g. \code{"NIS_Core"}).
#'   Must correspond to a subdirectory of \code{ParquetBasePath}.
#' @return \code{invisible(NULL)}.  Called for its side effects.
#' @details
#' Backslashes in the path are normalised to forward slashes before
#' interpolation into the SQL string because DuckDB interprets backslashes as
#' SQL escape sequences.
#' @examples
#' \dontrun{
#' if (requireNamespace("duckdb", quietly = TRUE)) {
#'   tmp_base <- tempfile("parquet_demo_")
#'   dir.create(tmp_base)
#'   set.seed(1)
#'   my_df <- data.frame(AGE = sample(18:90, 50, replace = TRUE),
#'                       SEX = sample(c("M", "F"), 50, replace = TRUE) )
#'   #### Register_parquet_view reads ParquetBasePath from the calling     ####
#'   #### environment, so it must be assigned before calling the function. ####
#'   ParquetBasePath <- tmp_base
#'   write_year_parquet(my_df, ParquetBasePath, "DEMO_Core", 2020, "DEMO_2020_Core.sav")
#'   con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
#'   register_parquet_view(con, "DEMO_Core")
#'   print(DBI::dbGetQuery(con, "SELECT COUNT(*) AS n FROM DEMO_Core"))
#'   DBI::dbDisconnect(con, shutdown = TRUE)
#'   unlink(tmp_base, recursive = TRUE) }
#' }
#' @export
register_parquet_view <- function(con, ParquetBasePath, table_name, schema_registry = NULL, validate = TRUE,
                                  strict_validation = TRUE, table_schema = NULL) {
  parquet_dir <- gsub("\\\\", "/", file.path(ParquetBasePath, table_name))
  #### A table whose source files were all verified empty completes and     ####
  #### checkpoints without writing any Parquet. read_parquet() errors on a  ####
  #### fileless glob, so skip the view (loudly) instead of failing the      ####
  #### whole registration pass.                                             ####
  n_parquet <- length(list.files(parquet_dir, pattern = "\\.parquet$", recursive = TRUE, ignore.case = TRUE))
  if (n_parquet == 0L) {
    log_msg(sprintf("[VIEW SKIPPED] %s: no parquet files under %s (all source files may be verified-empty). No view created.",
                    table_name, parquet_dir))
    return(invisible(FALSE))
  }
  qtbl <- quote_duckdb_ident(table_name)
  qpath <- quote_duckdb_string(paste0(parquet_dir, "/**/*.parquet"))
  partition_types <- hive_partition_types(table_name, table_schema, parquet_dir)
  hive_types_sql <- if (length(partition_types) > 0L) {
    entries <- paste(sprintf("%s: %s", quote_duckdb_string(names(partition_types)),
                             unname(partition_types)), collapse = ", ")
    paste0(", hive_types = {", entries, "}")
  } else {
    ""
  }
  #### DuckDB otherwise guesses DATE/BIGINT from directory text. Explicit ####
  #### hive_types keeps partition columns aligned with the schema catalog. ####
  try(DBI::dbExecute(con, "SET hive_types_autocast = false"), silent = TRUE)
  projection <- if (length(partition_types) > 0L) {
    source_names <- names(partition_types)
    aliases <- canonical_colnames(source_names)
    paste0("* EXCLUDE (", paste(vapply(source_names, quote_duckdb_ident, character(1)), collapse = ", "), "), ",
           paste(sprintf("CAST(%s AS %s) AS %s",
                         vapply(source_names, quote_duckdb_ident, character(1)),
                         unname(partition_types),
                         vapply(aliases, quote_duckdb_ident, character(1))), collapse = ", "))
  } else {
    "*"
  }
  DBI::dbExecute(con, glue::glue("CREATE OR REPLACE VIEW {qtbl} AS
                       SELECT {projection} FROM read_parquet({qpath},
                       hive_partitioning = true, union_by_name = true{hive_types_sql})"))
  if (isTRUE(validate)) {
    validate_duckdb_table(con, table_name, schema_registry = schema_registry,
                          strict = strict_validation, table_schema = table_schema)
  }
  invisible(TRUE)
}

#' Register DuckDB views for multiple tables
#'
#' Loops over \code{tables_written} and calls \code{\link{register_parquet_view}}
#' for each, logging or printing progress after each registration.
#' @param con A DBI connection to an open DuckDB database.
#' @param ParquetBasePath Character. Root directory of the Parquet store.
#' @param tables_written Character vector of table names to register.
#' @param verbose Logical. If \code{TRUE} (default), logs or prints a
#'   confirmation message for each registered view.
#' @param logStatus Logical. If \code{TRUE} (default), messages are written via
#'   \code{\link{log_msg}}; otherwise they are printed to the console.
#' @return \code{invisible(NULL)}.  Called for its side effects.
#' @seealso \code{\link{register_parquet_view}}
#' @keywords internal
register_parquet_view_compile <- function(con, ParquetBasePath, tables_written, verbose = TRUE, logStatus = TRUE,
                                          SchemaRegistryPath = NULL, schema_registry = NULL,
                                          validate = TRUE, strict_validation = TRUE,
                                          TableSchemaPath = NULL, table_schema = NULL,
                                          LogPath = NULL, RunId = NULL){
  previous_run <- if (!is.null(LogPath) || !is.null(RunId)) begin_repository_run(LogPath, RunId) else NULL
  if (!is.null(previous_run)) on.exit(restore_repository_run(previous_run), add = TRUE)
  if (is.null(schema_registry)) schema_registry <- load_schema_registry(SchemaRegistryPath, create_if_missing = FALSE)
  if (is.null(table_schema) && !is.null(TableSchemaPath) && nzchar(TableSchemaPath)) {
    catalog <- load_table_schema_catalog(TableSchemaPath, strict = strict_validation)
    if (is.null(catalog) && isTRUE(strict_validation)) {
      stop(sprintf("Strict DuckDB validation requires a readable table schema catalog: %s", TableSchemaPath))
    }
    if (!is.null(catalog)) table_schema <- catalog$table_schema
  }
  for(tbl in tables_written){
    registered <- register_parquet_view(con, ParquetBasePath = ParquetBasePath, table_name = tbl,
                                        schema_registry = schema_registry,
                                        validate = validate, strict_validation = strict_validation,
                                        table_schema = table_schema)
    if(verbose){
      status_msg <- if (isTRUE(registered)) sprintf("View registered: %s", tbl) else sprintf("View skipped (no parquet files): %s", tbl)
      if(logStatus){
        log_msg(status_msg)
      } else {
        print(status_msg)
      }
    }
  }
}

#' Print a post-load summary of completed files and failures
#'
#' Reads the checkpoint and the log file to report how many files completed
#' successfully and how many \code{[FAIL]} entries appear in the log.
#' @param MDT Data frame. Master Database Table (used to identify completed
#'   paths).
#' @param CheckpointPath Character. Path to the checkpoint \code{.rds} file.
#' @param LogPath Character. Path to the plain-text log file produced by
#'   \code{\link{log_msg}}.
#' @param logStatus Logical. If \code{TRUE} (default), output is written via
#'   \code{\link{log_msg}}; otherwise it is printed to the console.
#' @return \code{invisible(NULL)}.  Called for its side effects.
#' @seealso \code{\link{MDTCompleteStatus}}, \code{\link{load_checkpoint}}
#' @keywords internal
SummaryVerification <- function(MDT, CheckpointPath, LogPath, logStatus = TRUE,
                                RunId = NULL, MasterDBPath = NULL,
                                SourceFingerprintMode = c("none", "metadata", "sha256")){
  SourceFingerprintMode <- match.arg(SourceFingerprintMode)
  msg <- function(x) if (logStatus) log_msg(x) else print(x)
  msg("=== Load Summary ===")
  completed_checkpoint <- load_checkpoint(path = CheckpointPath)
  completed_files_complete <- MDT[checkpoint_completed_mask(
    MDT, completed_checkpoint, accept_legacy = SourceFingerprintMode == "none",
    MasterDBPath = MasterDBPath, SourceFingerprintMode = SourceFingerprintMode),]$Path
  msg(sprintf("Files completed : %d", length(completed_files_complete)))
  #### Load failures and errors ####
  Log <- if (file.exists(LogPath)) readLines(LogPath) else character(0)
  if (!is.null(RunId) && length(RunId) > 0L && !is.na(RunId[1]) && nzchar(RunId[1])) {
    Log <- Log[grepl(sprintf("[run_id=%s]", RunId[1]), Log, fixed = TRUE)]
  }
  load_failures_complete <- Log[grepl("\\[FAIL\\]", Log)]
  load_errors_complete   <- Log[grepl("\\[ERROR\\]", Log)]
  msg(sprintf("Number of [FAIL] (0 rows): %d", length(load_failures_complete)))
  if (length(load_failures_complete) > 0) {
    msg("[FAIL] entries (files that will retry on next run):")
    for (f in load_failures_complete) msg(sprintf("  - %s", f))
  }
  msg(sprintf("Number of [ERROR] (write/network failures): %d", length(load_errors_complete)))
  if (length(load_errors_complete) > 0) {
    msg("[ERROR] entries (files that will retry on next run):")
    for (f in load_errors_complete) msg(sprintf("  - %s", f))
  }
  load_warnings <- Log[grepl("\\[WARN\\]", Log)]
  msg(sprintf("Number of Integrity Warnings: %d", length(load_warnings)))
  if (length(load_warnings) > 0) {
    msg("Integrity Warnings (files that may be truncated -- verify row counts):")
    for (w in load_warnings) msg(sprintf("  - %s", w))
  }
}

#' List all registered DuckDB views
#'
#' Calls \code{dbListTables()} and logs or prints the names and count of all
#' tables / views currently registered in the DuckDB connection.
#' @param con A DBI connection to an open DuckDB database.
#' @param verbose Logical. If \code{TRUE} (default), logs or prints each view
#'   name.
#' @param logStatus Logical. If \code{TRUE} (default), output is written via
#'   \code{\link{log_msg}}; otherwise it is printed to the console.
#' @return Character vector of table/view names (invisibly from
#'   \code{dbListTables()}).
#' @seealso \code{\link{DBDimPerTable}}, \code{\link{register_parquet_view}}
#' @export
DBViewSummary <- function(con, verbose = TRUE, logStatus = TRUE){
  tables <- DBI::dbListTables(con)
  if(verbose){
    if(logStatus){
    log_msg(sprintf("Registered views (%d):", length(tables)))
    for(t in tables){ log_msg(sprintf("  - %s", t)) }
    } else {
      print(sprintf("Registered views (%d):", length(tables)))
      for(t in tables){ print(sprintf("  - %s", t)) }
    }
  }
  return(tables)
}

#' Report row and column counts for every registered DuckDB view
#'
#' Queries each table in the DuckDB connection for its row count
#' (\code{COUNT(*)}) and column count (\code{DESCRIBE}), assembles a summary
#' \code{data.table}, and optionally sorts by memory burden
#' (\code{Nrow * Ncol}).
#' @param con A DBI connection to an open DuckDB database.
#' @param verbose Logical. If \code{TRUE} (default), logs or prints each
#'   table's row count as it is queried.
#' @param logStatus Logical. If \code{TRUE} (default), output is written via
#'   \code{\link{log_msg}}; otherwise it is printed to the console.
#' @param orderByMemBurden Logical. If \code{TRUE} (default), the returned
#'   table is sorted descending by \code{Nrow * Ncol} so the most memory-
#'   intensive tables appear first.
#' @return A \code{data.table} with columns \code{Table}, \code{Nrow},
#'   \code{Ncol}, and \code{MemBurden}.
#' @seealso \code{\link{DBViewSummary}}
#' @export
DBDimPerTable <- function(con, verbose = TRUE, logStatus = TRUE, orderByMemBurden = TRUE){
  (tables <- DBI::dbListTables(con))
  CountTable <- data.table()
  for(tbl in tables){
    qtbl <- quote_duckdb_ident(tbl)
    n_rows <- DBI::dbGetQuery(con, paste("SELECT COUNT(*) AS n FROM", qtbl))$n
    Ncol <- nrow(DBI::dbGetQuery(con, paste("DESCRIBE", qtbl)))
    CountTable <- rbind(CountTable, data.table(Table = tbl, Nrow = n_rows, Ncol = Ncol))
    if(verbose){
      if(logStatus){
      log_msg(sprintf("%s row count: %s", tbl, formatC(n_rows, format = "d", big.mark = ",")))
      } else {
        print(sprintf("%s row count: %s", tbl, formatC(n_rows, format = "d", big.mark = ",")))
      }
      }
  }
  CountTable$MemBurden <- CountTable$Nrow*CountTable$Ncol
  if(orderByMemBurden){ CountTable <- CountTable[order(MemBurden, decreasing = TRUE),] }
  return(CountTable)
}


################################################################################
#### Schema normalization helpers ##############################################
################################################################################
# Canonicalize names once, early, and everywhere. HCUP/SPSS source files are
# conventionally upper-case, so upper-case is used as the single warehouse schema.
canonical_colnames <- function(x) {
  x <- trimws(as.character(x))
  x <- toupper(x)
  x[x == ""] <- "X"
  x
}

normalize_type_name <- function(x) {
  x <- trimws(as.character(x)[1])
  if (length(x) == 0L || is.na(x) || !nzchar(x)) return("character")
  if (grepl("^decimal\\([0-9]+,[0-9]+\\)$", tolower(x))) return(tolower(x))
  switch(tolower(x),
         "double" = "numeric",
         "numeric" = "numeric",
         "integer" = "integer",
         "integer64" = "int64",
         "int64" = "int64",
         "logical" = "logical",
         "unknown" = "unknown",
         "factor" = "character",
         "ordered" = "character",
         "date" = "Date",
         "posixct" = "POSIXct",
         "posixlt" = "POSIXct",
         "hms" = "time",
         "difftime" = "duration",
         "raw" = "binary",
         "list" = "list",
         "character" = "character",
         x)
}

allowed_canonical_types <- function() {
  c("character", "integer", "int64", "numeric", "logical", "Date", "POSIXct",
    "time", "duration", "binary", "list", "decimal(p,s)")
}

is_allowed_canonical_type <- function(x) {
  x <- vapply(x, normalize_type_name, character(1))
  x %in% setdiff(allowed_canonical_types(), "decimal(p,s)") |
    grepl("^decimal\\([0-9]+,[0-9]+\\)$", x)
}

promote_types <- function(types, col_name = NULL) {
  types <- unique(vapply(types, normalize_type_name, character(1)))
  types <- types[!is.na(types) & nzchar(types) & types != "unknown"]
  if (length(types) == 0L) return("character")
  if ("character" %in% types) return("character")
  if (all(types == "logical")) return("logical")
  if (all(types %in% c("integer", "int64"))) return(if ("int64" %in% types) "int64" else "integer")
  decimal_types <- types[grepl("^decimal\\([0-9]+,[0-9]+\\)$", types)]
  if (length(decimal_types) > 0L) {
    if (all(types %in% c("integer", "int64", "numeric", decimal_types))) {
      if (length(unique(decimal_types)) == 1L && !"numeric" %in% types) return(decimal_types[1])
      return("numeric")
    }
    return("character")
  }
  if (any(types %in% c("Date", "POSIXct"))) {
    if (all(types %in% c("Date", "POSIXct"))) return(if ("POSIXct" %in% types) "POSIXct" else "Date")
    return("character")
  }
  if (all(types %in% c("logical", "integer"))) return("integer")
  if (all(types %in% c("logical", "integer", "int64", "numeric"))) {
    if ("numeric" %in% types) return("numeric")
    if ("int64" %in% types) return("int64")
    return("integer")
  }
  if (length(types) == 1L && types %in% c("time", "duration", "binary", "list")) return(types)
  "character"
}

make_typed_na <- function(col_class, n = 1L) {
  col_class <- normalize_type_name(col_class)
  if (grepl("^decimal\\(", col_class)) return(rep(NA_real_, n))
  switch(col_class,
         "integer" = rep(NA_integer_, n),
         "int64" = {
           if (!requireNamespace("bit64", quietly = TRUE)) stop("Canonical type int64 requires package 'bit64'.")
           bit64::as.integer64(rep(NA_character_, n))
         },
         "numeric" = rep(NA_real_, n),
         "logical" = rep(NA, n),
         "Date" = rep(as.Date(NA), n),
         "POSIXct" = rep(as.POSIXct(NA, tz = "UTC"), n),
         "time" = structure(rep(NA_real_, n), class = c("hms", "difftime"), units = "secs"),
         "duration" = as.difftime(rep(NA_real_, n), units = "secs"),
         "binary" = rep(list(NULL), n),
         "list" = rep(list(NULL), n),
         "character" = rep(NA_character_, n),
         rep(NA_character_, n))
}

coerce_to_class <- function(x, target_class) {
  target_class <- normalize_type_name(target_class)
  characterize <- function(v) {
    if (is.numeric(v)) {
      out <- format(v, scientific = FALSE, trim = TRUE, digits = 22)
      out[is.na(v)] <- NA_character_
      return(out)
    }
    as.character(v)
  }
  if (grepl("^decimal\\(", target_class)) return(as.numeric(x))
  switch(target_class,
         "character" = characterize(x),
         "integer"   = as.integer(x),
         "int64"     = {
           if (!requireNamespace("bit64", quietly = TRUE)) stop("Canonical type int64 requires package 'bit64'.")
           bit64::as.integer64(as.character(x))
         },
         "numeric"   = as.numeric(x),
         "logical"   = as.logical(x),
         "Date"      = as.Date(x),
         "POSIXct"   = as.POSIXct(x, tz = attr(x, "tzone") %||% "UTC"),
         "time"      = structure(as.numeric(x), class = c("hms", "difftime"), units = "secs"),
         "duration"  = as.difftime(as.numeric(x), units = "secs"),
         "binary"    = if (is.list(x)) x else lapply(x, charToRaw),
         "list"      = if (is.list(x)) x else as.list(x),
         stop(sprintf("Unsupported canonical type: %s", target_class)))
}

.canonical_encoding_name <- function(encoding) {
  if (is.null(encoding) || length(encoding) == 0L || is.na(encoding[1]) ||
      !nzchar(trimws(as.character(encoding[1])))) return("auto")
  value <- toupper(gsub("_", "-", trimws(as.character(encoding[1]))))
  switch(value,
         "AUTO" = "auto",
         "UTF8" = "UTF-8", "UTF-8" = "UTF-8", "ASCII" = "UTF-8",
         "WINDOWS-1252" = "windows-1252", "CP1252" = "windows-1252",
         "WINDOWS1252" = "windows-1252",
         "LATIN1" = "ISO-8859-1", "LATIN-1" = "ISO-8859-1",
         "ISO8859-1" = "ISO-8859-1", "ISO-8859-1" = "ISO-8859-1",
         trimws(as.character(encoding[1])))
}

.delimited_binary_connection <- function(path) {
  if (grepl("\\.(gz|gzip)$", path, ignore.case = TRUE)) gzfile(path, open = "rb") else file(path, open = "rb")
}

.read_encoding_sample <- function(path, sample_bytes = 4L * 1024L^2L) {
  con <- .delimited_binary_connection(path)
  on.exit(close(con), add = TRUE)
  readBin(con, what = "raw", n = as.integer(sample_bytes))
}

#' Resolve a delimited source file's character encoding without modifying it
#'
#' Reads a bounded raw-byte sample from a source opened read-only. An explicit
#' encoding wins; otherwise a BOM, strict UTF-8 validation, and ICU detection
#' are used in that order. The returned encoding is used to convert parsed text
#' in memory, while the source file remains byte-for-byte unchanged.
.resolve_source_encoding <- function(path, declared_encoding = NULL,
                                     sample_bytes = 4L * 1024L^2L) {
  if (!file.exists(path)) stop("Source file not found while resolving encoding: ", path)
  declared <- .canonical_encoding_name(declared_encoding)
  bytes <- .read_encoding_sample(path, sample_bytes = sample_bytes)
  bom <- if (length(bytes) >= 3L && identical(bytes[1:3], as.raw(c(0xEF, 0xBB, 0xBF)))) {
    "UTF-8"
  } else if (length(bytes) >= 2L && identical(bytes[1:2], as.raw(c(0xFF, 0xFE)))) {
    "UTF-16LE"
  } else if (length(bytes) >= 2L && identical(bytes[1:2], as.raw(c(0xFE, 0xFF)))) {
    "UTF-16BE"
  } else {
    NA_character_
  }
  if (declared != "auto") {
    return(list(DeclaredEncoding = declared, DetectedEncoding = bom,
                EncodingConfidence = 1, EncodingUsed = declared,
                DetectionMethod = "declared"))
  }
  if (!is.na(bom)) {
    if (bom != "UTF-8") {
      stop(sprintf("Delimited source %s uses %s. UTF-16 delimited input is not byte-compatible with fread; declare a custom reader for this source.",
                   basename(path), bom))
    }
    return(list(DeclaredEncoding = "auto", DetectedEncoding = bom,
                EncodingConfidence = 1, EncodingUsed = bom,
                DetectionMethod = "bom"))
  }
  if (length(bytes) == 0L || isTRUE(stringi::stri_enc_isutf8(list(bytes))[1])) {
    return(list(DeclaredEncoding = "auto", DetectedEncoding = "UTF-8",
                EncodingConfidence = 1, EncodingUsed = "UTF-8",
                DetectionMethod = "strict_utf8"))
  }
  detected <- stringi::stri_enc_detect(list(bytes))[[1]]
  if (is.null(detected) || nrow(detected) == 0L) {
    stop("Unable to detect the source encoding for: ", path)
  }
  control_bytes <- as.integer(bytes) >= 0x80L & as.integer(bytes) <= 0x9FL
  if (any(control_bytes) && any(tolower(detected$Encoding) == "windows-1252")) {
    chosen <- detected[tolower(detected$Encoding) == "windows-1252", , drop = FALSE][1, ]
  } else {
    chosen <- detected[which.max(detected$Confidence), , drop = FALSE]
  }
  encoding <- .canonical_encoding_name(chosen$Encoding[1])
  if (toupper(encoding) %in% c("UTF-16LE", "UTF-16BE", "UTF-32LE", "UTF-32BE")) {
    stop(sprintf("Detected %s for %s. This encoding requires a custom delimited reader.", encoding, basename(path)))
  }
  list(DeclaredEncoding = "auto", DetectedEncoding = encoding,
       EncodingConfidence = as.numeric(chosen$Confidence[1]),
       EncodingUsed = encoding, DetectionMethod = "icu")
}

.resolve_delimited_reader_options <- function(path, reader_options = list()) {
  if (!is.null(reader_options$.EncodingInfo)) return(reader_options)
  info <- .resolve_source_encoding(path, reader_options$Encoding %||% "auto")
  reader_options$.EncodingInfo <- info
  reader_options$Encoding <- info$EncodingUsed
  reader_options
}

.normalize_utf8_vector <- function(x, source_encoding = "UTF-8", context = "character data") {
  if (!is.character(x)) return(x)
  from <- .canonical_encoding_name(source_encoding)
  if (from == "auto") stop("An unresolved 'auto' encoding reached UTF-8 normalization.")
  converted <- suppressWarnings(iconv(x, from = from, to = "UTF-8", sub = NA_character_))
  failed <- !is.na(x) & is.na(converted)
  if (any(failed)) {
    first <- which(failed)[1]
    stop(sprintf("UTF-8 conversion failed for %s at value %d using source encoding %s.",
                 context, first, from))
  }
  valid <- is.na(converted) | validUTF8(converted)
  if (any(!valid)) {
    stop(sprintf("UTF-8 validation failed for %s after conversion from %s.", context, from))
  }
  Encoding(converted[!is.na(converted)]) <- "UTF-8"
  converted
}

normalize_character_encoding <- function(df, source_encoding = NULL) {
  if (!data.table::is.data.table(df)) data.table::setDT(df)
  enc <- .canonical_encoding_name(source_encoding)
  if (enc == "auto") enc <- "UTF-8"
  char_cols <- names(df)[vapply(df, is.character, logical(1))]
  #### Convert everything first, then mutate the table. A failed conversion ####
  #### therefore leaves the original bytes intact for an automatic retry.  ####
  new_names <- .normalize_utf8_vector(names(df), enc, "column names")
  converted <- lapply(char_cols, function(col) {
    .normalize_utf8_vector(df[[col]], enc, sprintf("column %s", col))
  })
  names(converted) <- char_cols
  names(df) <- new_names
  for (col in char_cols) data.table::set(df, j = col, value = converted[[col]])
  df
}

.detect_character_data_encoding <- function(df) {
  char_cols <- names(df)[vapply(df, is.character, logical(1))]
  candidates <- c(list(names(df)), unname(as.list(df[, char_cols, with = FALSE])))
  raw_values <- list()
  for (values in candidates) {
    bad <- !is.na(values) & !validUTF8(values)
    if (!any(bad)) next
    selected <- utils::head(values[bad], 100L)
    raw_values <- c(raw_values, lapply(selected, charToRaw))
    if (length(raw_values) >= 100L) break
  }
  if (length(raw_values) == 0L) return(NULL)
  bytes <- do.call(c, raw_values)
  detected <- stringi::stri_enc_detect(list(bytes))[[1]]
  if (is.null(detected) || nrow(detected) == 0L) return(NULL)
  control_bytes <- as.integer(bytes) >= 0x80L & as.integer(bytes) <= 0x9FL
  if (any(control_bytes) && any(tolower(detected$Encoding) == "windows-1252")) {
    chosen <- detected[tolower(detected$Encoding) == "windows-1252", , drop = FALSE][1, ]
  } else {
    chosen <- detected[which.max(detected$Confidence), , drop = FALSE]
  }
  list(Encoding = .canonical_encoding_name(chosen$Encoding[1]),
       Confidence = as.numeric(chosen$Confidence[1]))
}

.normalize_delimited_frame <- function(df, reader_options, path) {
  info <- reader_options$.EncodingInfo
  attempt <- tryCatch(normalize_character_encoding(df, info$EncodingUsed), error = identity)
  if (!inherits(attempt, "error")) {
    attr(attempt, "repoquet_encoding_info") <- info
    return(attempt)
  }
  if (!identical(info$DeclaredEncoding, "auto")) stop(attempt)
  fallback <- .detect_character_data_encoding(df)
  if (is.null(fallback) || fallback$Encoding == "UTF-8") stop(attempt)
  converted <- normalize_character_encoding(df, fallback$Encoding)
  info$DetectedEncoding <- fallback$Encoding
  info$EncodingUsed <- fallback$Encoding
  info$EncodingConfidence <- fallback$Confidence
  info$DetectionMethod <- "content_retry"
  attr(converted, "repoquet_encoding_info") <- info
  converted
}

canonicalize_dataframe_names <- function(df) {
  if (!data.table::is.data.table(df)) data.table::setDT(df)
  old_names <- names(df)
  new_names <- canonical_colnames(old_names)
  data.table::setnames(df, old_names, new_names)
  dup_names <- unique(new_names[duplicated(new_names)])
  if (length(dup_names) > 0L) {
    for (nm in dup_names) {
      idx <- which(names(df) == nm)
      keep <- idx[1]
      drop <- idx[-1]
      for (j in drop) {
        lhs <- df[[keep]]
        rhs <- df[[j]]
        if (!identical(class(lhs)[1], class(rhs)[1])) {
          target <- promote_types(c(class(lhs)[1], class(rhs)[1]), nm)
          lhs <- coerce_to_class(lhs, target)
          rhs <- coerce_to_class(rhs, target)
        }
        overlap <- !is.na(lhs) & !is.na(rhs)
        if (any(overlap)) {
          equal <- lhs[overlap] == rhs[overlap]
          equal[is.na(equal)] <- FALSE
          if (!all(equal)) {
            bad_row <- which(overlap)[which(!equal)[1]]
            stop(sprintf(paste0("Column-name canonicalization would merge conflicting columns '%s' into '%s'. ",
                                "The first disagreement is at row %d; rename or reconcile the source columns explicitly."),
                         paste(old_names[which(new_names == nm)], collapse = "' and '"), nm, bad_row))
          }
        }
        miss <- is.na(lhs)
        if (any(miss)) lhs[miss] <- rhs[miss]
        data.table::set(df, j = keep, value = lhs)
      }
      #### Delete by position, highest first. Deleting by duplicated name can ####
      #### remove the retained canonical column or assign to it twice.        ####
      for (j in rev(drop)) data.table::set(df, j = j, value = NULL)
    }
  }
  df
}

################################################################################
#### Coercion damage accounting ################################################
################################################################################
#### Run-scoped collector: every NA-introducing coercion is recorded here    ####
#### (in addition to the log line, which scrolls away on long runs) so       ####
#### ParquetBackEndCreate can write one reviewable per-column report at the  ####
#### end of the run.                                                         ####
.coerce_env <- new.env(parent = emptyenv())
.coerce_env$records <- list()

#' @export
coercion_report_reset <- function() {
  .coerce_env$records <- list()
  invisible(NULL)
}

coercion_report_collect <- function(column, from_class, to_class, n_destroyed, n_present) {
  .coerce_env$records[[length(.coerce_env$records) + 1L]] <- data.table::data.table(
    Column = column, FromClass = from_class, ToClass = to_class,
    NDestroyed = as.numeric(n_destroyed), NPresent = as.numeric(n_present))
  invisible(NULL)
}

#' Write the aggregated coercion-damage report for the current run
#' @export
coercion_report_write <- function(path) {
  if (length(.coerce_env$records) == 0L) return(invisible(NULL))
  rec <- data.table::rbindlist(.coerce_env$records)
  agg <- rec[, .(NDestroyed = sum(NDestroyed), NPresent = sum(NPresent),
                 PctDestroyed = round(100 * sum(NDestroyed) / max(1, sum(NPresent)), 4)),
             by = .(Column, FromClass, ToClass)]
  data.table::setorder(agg, -NDestroyed)
  write_csv_safely(agg, path)
  log_msg(sprintf("[COERCE REPORT] %d column(s) had values destroyed by type coercion this run -- details: %s",
                  nrow(agg), path))
  invisible(path)
}

enforce_col_classes <- function(df, col_classes = NULL, max_coerce_na_pct = NULL) {
  if (!data.table::is.data.table(df)) data.table::setDT(df)
  df <- canonicalize_dataframe_names(df)
  if (is.null(col_classes)) return(df)
  names(col_classes) <- canonical_colnames(names(col_classes))
  for (col in intersect(names(col_classes), names(df))) {
    agreed <- normalize_type_name(col_classes[[col]])
    actual <- normalize_type_name(class(df[[col]])[1])
    decimal_compatible <- grepl("^decimal\\(", agreed) && identical(actual, "numeric")
    if (identical(agreed, actual) || decimal_compatible) next
    na_before <- sum(is.na(df[[col]]))
    n_present <- length(df[[col]]) - na_before
    converted <- tryCatch(coerce_to_class(df[[col]], agreed), error = function(e) e)
    if (inherits(converted, "error")) {
      stop(sprintf("Column %s could not be coerced from %s to %s: %s",
                   col, actual, agreed, conditionMessage(converted)))
    }
    converted_class <- normalize_type_name(class(converted)[1])
    converted_decimal_compatible <- grepl("^decimal\\(", agreed) && identical(converted_class, "numeric")
    if (!identical(converted_class, agreed) && !converted_decimal_compatible) {
      stop(sprintf("Column %s coercion did not produce the agreed class: expected %s, got %s.",
                   col, agreed, converted_class))
    }
    data.table::set(df, j = col, value = converted)
    rm(converted)
    na_introduced <- sum(is.na(df[[col]])) - na_before
    if (na_introduced > 0L) {
      coercion_report_collect(col, actual, agreed, na_introduced, n_present)
      pct_destroyed <- 100 * na_introduced / max(1L, n_present)
      if (!is.null(max_coerce_na_pct) && is.finite(max_coerce_na_pct) && pct_destroyed > max_coerce_na_pct) {
        stop(sprintf("Column %s: coercing %s -> %s destroyed %.1f%% of present values (%d of %d), which exceeds the coercion NA threshold (%.1f%%). Fix the column's type in the schema registry/catalog, or raise MaxCoerceNAPct.",
                     col, actual, agreed, pct_destroyed, na_introduced, n_present, max_coerce_na_pct))
      }
      log_msg(sprintf("[COERCE WARNING] %s: %s -> %s set %d value(s) to NA. If this column holds codes/text, add it to the schema registry as character.",
                      col, actual, agreed, na_introduced))
    }
  }
  df
}

##########################################################
#### Add a year column to a data table if it's absent ####
##########################################################
#' Add a \code{year} column to a data frame if absent
#'
#' Checks for both \code{"year"} and \code{"YEAR"} column names (case
#' insensitive).  If neither is present, adds a \code{year} column with the
#' scalar value \code{year_value}.
#' @param df A data frame or data.table.
#' @param year_value Integer or character. The year value to assign.
#' @return The input \code{df} with a \code{year} column guaranteed to be
#'   present.
#' @examples
#' \dontrun{
#' set.seed(1)
#' df1 <- data.frame(AGE = sample(18:90, 5), SEX = sample(c("M","F"), 5, replace = TRUE))
#' df1 <- add_year_if_missing(df1, 2019)
#' stopifnot("year" %in% names(df1))
#' #### Already has a year column -- left untouched ####
#' df2 <- data.frame(AGE = sample(18:90, 5), year = 2018L)
#' df2 <- add_year_if_missing(df2, 2019)
#' unique(df2$year)  # still 2018, not overwritten
#' }
#' @export
add_year_if_missing <- function(df, year_value) {
  if (!data.table::is.data.table(df)) data.table::setDT(df)
  df <- canonicalize_dataframe_names(df)
  if (!"YEAR" %in% colnames(df)) {
    data.table::set(df, j = "YEAR", value = as.integer(year_value))
  } else {
    data.table::set(df, j = "YEAR", value = as.integer(df[["YEAR"]]))
  }
  return(df)
}
################################################################################
#### Data cleaning helpers #####################################################
################################################################################
#' Strip all \pkg{haven} S3 attributes from a data frame
#'
#' \code{haven::read_sav()} returns a tibble whose columns carry the
#' \code{haven_labelled} S3 class.  Operations such as \code{setDT()},
#' \code{data.table::set()}, and Arrow's \code{Table$create()} dispatch on
#' that class and can trigger \emph{"recursive indexing failed at level 2"}.
#'
#' This function converts the data frame to a \code{data.table} in-place
#' (zero-copy via \code{setDT()}), then replaces each \code{haven_labelled}
#' column with its declared base type (\code{double}, \code{integer}, or
#' \code{character}) using \code{data.table::set()}.  Peak additional RAM is
#' one column at a time.
#' @param df A data frame, tibble, or data.table returned by
#'   \code{haven::read_sav()}.
#' @return The same object (a \code{data.table}) with all \code{haven_labelled}
#'   classes removed and columns coerced to their declared base types.
#' @details
#' \code{type.convert()} is deliberately \emph{not} used here because i
#' re-infers types from values, making the result chunk-dependent.  For example
#' a procedure-code column containing only digits in one chunk would be inferred
#' as \code{integer}, but a chunk containing alphanumeric ICD-10 codes would be
#' inferred as \code{character}, causing Arrow schema conflicts across chunks.
#' Using the declared base type from the \code{haven_labelled} class vector is
#' deterministic across all chunks of the same file.
#' @seealso \code{\link{align_columns}}, \code{\link{safe_read_sav}}
#' @examples
#' \dontrun{
#' set.seed(1)
#' df <- data.frame(sex = haven::labelled(sample(1:2, 10, replace = TRUE),
#'                                        labels = c(Male = 1, Female = 2)),
#'                  age = haven::labelled(sample(18:90, 10, replace = TRUE),
#'                                        label = "Age in years") )
#' sapply(df, function(x) class(x)[1]) # "haven_labelled"
#' df <- strip_haven(df)
#' sapply(df, function(x) class(x)[1]) # "integer"/"numeric"
#' }
#' @export
################################################################################
#### Pluggable file readers ####################################################
################################################################################
#### FileType in the workbook selects a reader from this registry, so the    ####
#### pipeline is not tied to SPSS+CSV: Stata, SAS, transport files, Parquet, ####
#### RDS, and gzipped delimited files ship as built-ins, and a user can      ####
#### register their own with register_file_reader(). Contract:               ####
####   read_full(path)   -> data.frame/data.table; MUST error on failure     ####
####                        (dispatch strategies rely on the error signal).  ####
####   read_header(path) -> character vector of raw column names.            ####
####   read_sample(path) -> data.frame of the first rows for type inference  ####
####                        (declared-type formats need few rows; delimited  ####
####                        formats sample deep).                            ####
####   count_rows(path)  -> integer row count (may error; treated as NA).    ####
####   has_labels        -> TRUE when read_labels_header(path) returns a     ####
####                        0-row frame carrying variable/value label attrs. ####
####   chunkable         -> TRUE when the loader may stream the file in      ####
####                        memory-bounded chunks.                           ####
.reader_registry <- new.env(parent = emptyenv())

#' Register a file reader for a workbook FileType
#' @export
register_file_reader <- function(type, read_full, read_header, read_sample,
                                 count_rows = NULL, has_labels = FALSE,
                                 read_labels_header = NULL, chunkable = FALSE,
                                 read_chunk = NULL) {
  type <- tolower(trimws(type))
  if (!nzchar(type)) stop("Reader type must be a non-empty string.")
  required_functions <- list(read_full = read_full, read_header = read_header, read_sample = read_sample)
  bad_required <- names(required_functions)[!vapply(required_functions, is.function, logical(1))]
  if (length(bad_required) > 0L) {
    stop(sprintf("Reader '%s' requires function(s): %s.", type, paste(bad_required, collapse = ", ")))
  }
  optional_functions <- list(count_rows = count_rows, read_labels_header = read_labels_header,
                             read_chunk = read_chunk)
  bad_optional <- names(optional_functions)[vapply(optional_functions, function(x) !is.null(x) && !is.function(x), logical(1))]
  if (length(bad_optional) > 0L) {
    stop(sprintf("Reader '%s' has non-function optional callback(s): %s.", type, paste(bad_optional, collapse = ", ")))
  }
  if (isTRUE(chunkable) && is.null(read_chunk) && !identical(type, "sav")) {
    stop(sprintf("Reader '%s' declares chunkable=TRUE but provides no read_chunk callback.", type))
  }
  if (isTRUE(chunkable) && is.null(count_rows) && !identical(type, "sav")) {
    stop(sprintf("Reader '%s' declares chunkable=TRUE but provides no count_rows callback.", type))
  }
  .reader_registry[[type]] <- list(type = type, read_full = read_full, read_header = read_header,
                                   read_sample = read_sample, count_rows = count_rows,
                                   has_labels = isTRUE(has_labels),
                                   read_labels_header = read_labels_header,
                                   chunkable = isTRUE(chunkable),
                                   read_chunk = read_chunk)
  invisible(type)
}

#' @export
supported_file_types <- function() sort(ls(.reader_registry))

#' @export
get_file_reader <- function(type) {
  rd <- .reader_registry[[tolower(trimws(type))]]
  if (is.null(rd)) {
    stop(sprintf("No file reader registered for FileType '%s'. Registered types: %s. Add one with register_file_reader().",
                 type, paste(supported_file_types(), collapse = ", ")))
  }
  rd
}

reader_supports_labels <- function(type) {
  isTRUE(tryCatch(get_file_reader(type)$has_labels, error = function(e) FALSE))
}

call_reader <- function(reader, method, path, reader_options = list(), ...) {
  rd <- if (is.list(reader)) reader else get_file_reader(reader)
  fun <- rd[[method]]
  if (is.null(fun) || !is.function(fun)) {
    stop(sprintf("Reader '%s' does not implement %s().", rd$type %||% "<custom>", method))
  }
  aliases <- c(Encoding = "encoding", Timezone = "tz", DateFormat = "date_format",
               DateTimeFormat = "datetime_format")
  aliased_options <- reader_options
  for (nm in intersect(names(aliases), names(reader_options))) {
    aliased_options[[aliases[[nm]]]] <- reader_options[[nm]]
  }
  #### Explicit loader arguments are appended last and win over workbook  ####
  #### options, so ReaderOptions cannot replace path/offset/schema controls. ####
  extras <- c(aliased_options, list(reader_options = reader_options), list(...))
  extras <- extras[!duplicated(names(extras), fromLast = TRUE)]
  formal_names <- names(formals(fun))
  if (!"..." %in% formal_names) extras <- extras[names(extras) %in% formal_names]
  do.call(fun, c(list(path), extras))
}

parse_reader_options_json <- function(x) {
  if (is.null(x) || length(x) == 0L || is.na(x[1]) || !nzchar(trimws(x[1]))) return(list())
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("The ReaderOptions MDT column requires package 'jsonlite'.")
  }
  out <- jsonlite::fromJSON(as.character(x[1]), simplifyVector = TRUE)
  if (!is.list(out) || is.data.frame(out)) stop("ReaderOptions must be a JSON object.")
  out
}

reader_options_for_row <- function(row_meta) {
  out <- if ("ReaderOptions" %in% names(row_meta)) parse_reader_options_json(row_meta$ReaderOptions[1]) else list()
  fields <- c("Encoding", "Delimiter", "Quote", "NAStrings", "DecimalMark",
              "DateFormat", "DateTimeFormat", "Timezone", "ReadMode",
              "KeepLeadingZeros", "MalformedRowPolicy", "ContinuationColumn",
              "ContinuationJoin")
  for (field in fields) {
    if (!field %in% names(row_meta)) next
    value <- row_meta[[field]][1]
    if (length(value) == 0L || is.na(value) || !nzchar(trimws(as.character(value)))) next
    if (identical(field, "NAStrings")) {
      raw <- trimws(as.character(value))
      value <- if (startsWith(raw, "[")) {
        if (!requireNamespace("jsonlite", quietly = TRUE)) stop("JSON NAStrings requires package 'jsonlite'.")
        unlist(jsonlite::fromJSON(raw), use.names = FALSE)
      } else {
        trimws(strsplit(raw, ";", fixed = TRUE)[[1]])
      }
    }
    out[[field]] <- value
  }
  out
}

fread_col_classes <- function(col_classes, header = NULL) {
  if (is.null(col_classes) || length(col_classes) == 0L) return(NULL)
  names(col_classes) <- canonical_colnames(names(col_classes))
  if (is.null(header)) header <- names(col_classes)
  canon_header <- canonical_colnames(header)
  target <- vapply(canon_header, function(nm) normalize_type_name(col_classes[[nm]] %||% ""), character(1))
  fread_type <- vapply(target, function(tp) {
    if (grepl("^decimal\\(", tp)) return("numeric")
    switch(tp, character = "character", integer = "integer", int64 = "integer64",
           numeric = "numeric", logical = "logical", "character")
  }, character(1))
  stats::setNames(fread_type, header)
}

fread_col_classes_positional <- function(col_classes, header) {
  named <- fread_col_classes(col_classes, header)
  if (is.null(named)) return(NULL)
  split(seq_along(named), unname(named))
}

delimited_fread_args <- function(reader_options = list(), col_classes = NULL, header = NULL) {
  sep <- reader_options$Delimiter %||% NULL
  if (identical(sep, "\\t")) sep <- "\t"
  keep_leading_zeros <- reader_options$KeepLeadingZeros %||% TRUE
  if (is.character(keep_leading_zeros)) {
    keep_leading_zeros <- tolower(trimws(keep_leading_zeros[1])) %in% c("true", "t", "yes", "y", "1")
  }
  args <- list(na.strings = reader_options$NAStrings %||% c("NA", "NULL"),
               #### fread parses ASCII delimiters from the original bytes. ####
               #### Character values are decoded strictly and normalized   ####
               #### to UTF-8 immediately after parsing.                    ####
               encoding = "unknown",
               colClasses = fread_col_classes(col_classes, header),
               keepLeadingZeros = isTRUE(keep_leading_zeros))
  if (!is.null(sep) && nzchar(sep)) args$sep <- sep
  if (!is.null(reader_options$Quote)) args$quote <- as.character(reader_options$Quote)
  if (!is.null(reader_options$DecimalMark)) args$dec <- as.character(reader_options$DecimalMark)
  args[!vapply(args, is.null, logical(1))]
}

.delimited_repair_settings <- function(reader_options = list()) {
  policy <- tolower(trimws(as.character(reader_options$MalformedRowPolicy %||% "error")[1]))
  policy <- gsub("-", "_", policy, fixed = TRUE)
  if (policy %in% c("append", "append_to_previous")) policy <- "append_previous"
  if (!policy %in% c("error", "append_previous")) {
    stop("MalformedRowPolicy must be 'error' or 'append_previous'.")
  }
  column <- trimws(as.character(reader_options$ContinuationColumn %||% "")[1])
  if (policy == "append_previous" && !nzchar(column)) {
    stop("ContinuationColumn is required when MalformedRowPolicy='append_previous'.")
  }
  join <- as.character(reader_options$ContinuationJoin %||% " ")[1]
  if (is.na(join)) join <- " "
  list(Policy = policy, Column = column, Join = join)
}

.uses_delimited_logical_stream <- function(reader_options = list()) {
  identical(.delimited_repair_settings(reader_options)$Policy, "append_previous")
}

.fread_with_structural_errors <- function(args, path) {
  structural_warning <- NULL
  out <- withCallingHandlers(
    do.call(data.table::fread, args),
    warning = function(w) {
      message <- conditionMessage(w)
      if (grepl("Stopped early on line [0-9]+\\. Expected [0-9]+ fields", message, perl = TRUE)) {
        structural_warning <<- message
        invokeRestart("muffleWarning")
      }
    }
  )
  if (!is.null(structural_warning)) {
    stop(sprintf(paste0("Delimited structure error in %s: %s. The source was not modified. ",
                        "For a verified continuation line, set MalformedRowPolicy='append_previous' ",
                        "and ContinuationColumn in ReaderOptions."),
                 basename(path), structural_warning), call. = FALSE)
  }
  out
}

.delimited_record_shape <- function(record, delimiter, quote_char = "\"") {
  if (nchar(delimiter, type = "chars") != 1L) stop("Delimited readers require a one-character delimiter.")
  quote_enabled <- !is.null(quote_char) && length(quote_char) > 0L &&
    !is.na(quote_char[1]) && nzchar(quote_char[1])
  quote_char <- if (quote_enabled) substr(as.character(quote_char[1]), 1L, 1L) else ""
  n <- nchar(record, type = "chars")
  fields <- 1L
  in_quotes <- FALSE
  field_start <- TRUE
  i <- 1L
  while (i <= n) {
    ch <- substr(record, i, i)
    if (quote_enabled && ch == quote_char) {
      if (in_quotes) {
        if (i < n && substr(record, i + 1L, i + 1L) == quote_char) {
          i <- i + 2L
          field_start <- FALSE
          next
        }
        in_quotes <- FALSE
        field_start <- FALSE
      } else if (field_start) {
        in_quotes <- TRUE
      } else {
        field_start <- FALSE
      }
    } else if (!in_quotes && ch == delimiter) {
      fields <- fields + 1L
      field_start <- TRUE
    } else if (in_quotes || !ch %in% c(" ", "\t")) {
      field_start <- FALSE
    }
    i <- i + 1L
  }
  list(Fields = fields, Balanced = !in_quotes)
}

.detect_delimited_separator <- function(header_line, path, reader_options = list()) {
  configured <- reader_options$Delimiter %||% NULL
  if (identical(configured, "\\t")) configured <- "\t"
  if (!is.null(configured) && nzchar(as.character(configured[1]))) {
    configured <- as.character(configured[1])
    if (nchar(configured, type = "chars") != 1L) stop("Delimiter must be exactly one character.")
    return(configured)
  }
  if (grepl("\\.tsv(?:\\.(?:gz|gzip))?$", path, ignore.case = TRUE)) return("\t")
  quote_char <- reader_options$Quote %||% "\""
  candidates <- c(",", "\t", "|", ";", ":")
  counts <- vapply(candidates, function(sep) {
    .delimited_record_shape(header_line, sep, quote_char)$Fields
  }, integer(1))
  best <- which.max(counts)
  if (counts[best] <= 1L) {
    stop(sprintf("Unable to determine a multi-column delimiter from the header of %s.", basename(path)))
  }
  candidates[best]
}

.open_delimited_text_connection <- function(path, encoding) {
  if (grepl("\\.(gz|gzip)$", path, ignore.case = TRUE)) {
    gzfile(path, open = "rt", encoding = encoding)
  } else if (grepl("\\.bz2$", path, ignore.case = TRUE)) {
    bzfile(path, open = "rt", encoding = encoding)
  } else {
    file(path, open = "rt", encoding = encoding)
  }
}

.delimited_header_info <- function(path, reader_options = list()) {
  reader_options <- .resolve_delimited_reader_options(path, reader_options)
  con <- .open_delimited_text_connection(path, reader_options$.EncodingInfo$EncodingUsed)
  on.exit(close(con), add = TRUE)
  physical_line <- 0L
  header_line <- NULL
  repeat {
    line <- readLines(con, n = 1L, warn = FALSE)
    if (length(line) == 0L) break
    physical_line <- physical_line + 1L
    if (nzchar(trimws(line[1]))) {
      header_line <- line[1]
      break
    }
  }
  if (is.null(header_line)) stop("Delimited source has no non-empty header: ", path)
  delimiter <- .detect_delimited_separator(header_line, path, reader_options)
  args <- delimited_fread_args(reader_options)
  args$sep <- delimiter
  args$colClasses <- NULL
  header_df <- .fread_with_structural_errors(
    c(list(text = header_line, nrows = 0L, header = TRUE), args), path)
  header_df <- normalize_character_encoding(header_df, "UTF-8")
  header <- names(header_df)
  if (length(header) < 2L) stop("Delimited source header resolved to fewer than two columns: ", path)
  list(Header = header, HeaderLine = header_line, HeaderPhysicalLine = physical_line,
       Delimiter = delimiter, ReaderOptions = reader_options)
}

.decode_continuation_value <- function(record, delimiter, reader_options, path) {
  args <- delimited_fread_args(reader_options)
  args$sep <- delimiter
  args$colClasses <- NULL
  value <- .fread_with_structural_errors(
    c(list(text = record, header = FALSE, nrows = 1L), args), path)
  if (nrow(value) != 1L || ncol(value) != 1L) {
    stop("Continuation repair expected exactly one text field.")
  }
  as.character(value[[1]][1])
}

.parse_delimited_logical_batch <- function(header_info, records, continuations,
                                           col_classes = NULL, path) {
  if (length(records) == 0L) return(data.table::data.table())
  options <- header_info$ReaderOptions
  repair <- .delimited_repair_settings(options)
  parse_classes <- col_classes
  if (!is.null(parse_classes)) {
    names(parse_classes) <- canonical_colnames(names(parse_classes))
    parse_classes[[canonical_colnames(repair$Column)]] <- "character"
  }
  args <- delimited_fread_args(options, col_classes = parse_classes,
                               header = header_info$Header)
  args$sep <- header_info$Delimiter
  text <- paste(c(header_info$HeaderLine, records), collapse = "\n")
  df <- .fread_with_structural_errors(c(list(text = text, header = TRUE), args), path)
  if (nrow(df) != length(records)) {
    stop(sprintf("Logical-record parser produced %d rows from %d assembled records in %s.",
                 nrow(df), length(records), basename(path)))
  }
  target <- match(canonical_colnames(repair$Column), canonical_colnames(names(df)))
  if (is.na(target)) {
    stop(sprintf("ContinuationColumn '%s' is not present in %s.", repair$Column, basename(path)))
  }
  if (!is.character(df[[target]])) data.table::set(df, j = target, value = as.character(df[[target]]))
  repaired_rows <- which(lengths(continuations) > 0L)
  for (i in repaired_rows) {
    addition <- paste(as.character(continuations[[i]]), collapse = repair$Join)
    current <- df[[target]][i]
    combined <- if (is.na(current) || !nzchar(current)) addition else paste0(current, repair$Join, addition)
    data.table::set(df, i = i, j = target, value = combined)
  }
  df <- normalize_character_encoding(df, "UTF-8")
  canonicalize_dataframe_names(df)
}

.stream_delimited_logical_records <- function(path, reader_options = list(),
                                               chunk_size = 1000000L,
                                               max_rows = Inf, col_classes = NULL,
                                               callback) {
  if (!is.function(callback)) stop("Logical-record streaming requires a callback function.")
  header_info <- .delimited_header_info(path, reader_options)
  options <- header_info$ReaderOptions
  repair <- .delimited_repair_settings(options)
  if (repair$Policy != "append_previous") {
    stop("Logical-record streaming currently requires MalformedRowPolicy='append_previous'.")
  }
  chunk_size <- max(1L, as.integer(chunk_size))
  max_rows <- if (is.infinite(max_rows)) Inf else max(0, as.numeric(max_rows))
  expected_fields <- length(header_info$Header)
  quote_char <- options$Quote %||% "\""
  con <- .open_delimited_text_connection(path, options$.EncodingInfo$EncodingUsed)
  on.exit(close(con), add = TRUE)

  records <- character(0)
  continuations <- list()
  pending_record <- NULL
  pending_continuations <- character(0)
  pending_line <- NA_integer_
  current_record <- NULL
  current_start <- NA_integer_
  physical_line <- 0L
  logical_rows <- 0L
  repair_lines <- integer(0)
  done <- FALSE

  flush_records <- function() {
    if (length(records) == 0L || done) return(invisible(NULL))
    remaining <- if (is.infinite(max_rows)) length(records) else
      min(length(records), max(0, as.integer(max_rows - logical_rows)))
    if (remaining <= 0L) {
      done <<- TRUE
      return(invisible(NULL))
    }
    selected <- seq_len(remaining)
    df <- .parse_delimited_logical_batch(header_info, records[selected],
                                         continuations[selected], col_classes, path)
    callback(df)
    logical_rows <<- logical_rows + nrow(df)
    if (remaining < length(records)) {
      records <<- records[-selected]
      continuations <<- continuations[-selected]
    } else {
      records <<- character(0)
      continuations <<- list()
    }
    if (!is.infinite(max_rows) && logical_rows >= max_rows) done <<- TRUE
    invisible(NULL)
  }

  finalize_pending <- function() {
    if (is.null(pending_record) || done) return(invisible(NULL))
    records <<- c(records, pending_record)
    continuations[[length(records)]] <<- pending_continuations
    pending_record <<- NULL
    pending_continuations <<- character(0)
    pending_line <<- NA_integer_
    if (length(records) >= chunk_size) flush_records()
    invisible(NULL)
  }

  repeat {
    lines <- readLines(con, n = 10000L, warn = FALSE)
    if (length(lines) == 0L || done) break
    for (line in lines) {
      physical_line <- physical_line + 1L
      if (physical_line <= header_info$HeaderPhysicalLine) next
      if (is.null(current_record) && !nzchar(trimws(line))) next
      if (is.null(current_record)) {
        current_record <- line
        current_start <- physical_line
      } else {
        current_record <- paste(current_record, line, sep = "\n")
      }
      shape <- .delimited_record_shape(current_record, header_info$Delimiter, quote_char)
      if (!shape$Balanced) next
      complete <- current_record
      start_line <- current_start
      current_record <- NULL
      current_start <- NA_integer_

      if (shape$Fields == expected_fields) {
        finalize_pending()
        if (done) break
        pending_record <- complete
        pending_line <- start_line
      } else if (shape$Fields == 1L && !is.null(pending_record)) {
        value <- .decode_continuation_value(complete, header_info$Delimiter, options, path)
        pending_continuations <- c(pending_continuations, value)
        repair_lines <- c(repair_lines, start_line)
      } else {
        stop(sprintf(paste0("Malformed delimited record at physical line %d in %s: expected %d fields, found %d. ",
                            "Automatic repair only accepts a one-field continuation following a complete record."),
                     start_line, basename(path), expected_fields, shape$Fields), call. = FALSE)
      }
    }
  }
  if (!done && !is.null(current_record)) {
    stop(sprintf("Unclosed quoted field beginning at physical line %d in %s.",
                 current_start, basename(path)), call. = FALSE)
  }
  if (!done) {
    finalize_pending()
    flush_records()
  }
  list(
    Header = header_info$Header,
    LogicalRows = as.numeric(logical_rows),
    PhysicalLinesRead = as.numeric(physical_line),
    RepairCount = as.numeric(length(repair_lines)),
    RepairLines = repair_lines,
    RepairPolicy = repair$Policy,
    ContinuationColumn = repair$Column,
    EncodingInfo = options$.EncodingInfo
  )
}

.collect_delimited_logical_records <- function(path, reader_options = list(),
                                               max_rows = Inf, col_classes = NULL) {
  chunks <- list()
  diagnostics <- .stream_delimited_logical_records(
    path, reader_options = reader_options,
    chunk_size = if (is.infinite(max_rows)) 100000L else max(1L, min(100000L, as.integer(max_rows))),
    max_rows = max_rows, col_classes = col_classes,
    callback = function(df) chunks[[length(chunks) + 1L]] <<- df)
  out <- if (length(chunks) == 0L) {
    header <- .delimited_header_info(path, reader_options)$Header
    data.table::as.data.table(stats::setNames(replicate(length(header), character(), simplify = FALSE),
                                              canonical_colnames(header)))
  } else {
    data.table::rbindlist(chunks, use.names = TRUE)
  }
  attr(out, "repoquet_encoding_info") <- diagnostics$EncodingInfo
  attr(out, "repoquet_delimited_diagnostics") <- diagnostics
  out
}

#### Shared implementations ##################################################
#### Haven-family full read: strip label classes, sanitize encodings. Errors ####
#### propagate to the caller.                                                ####
read_haven_full <- function(path, read_fun, source_encoding = NULL) {
  df <- read_fun(path)
  df <- strip_haven(df)
  normalize_character_encoding(df, source_encoding)
}

read_sav_with_options <- function(path, reader_options = list(), ...) {
  call_reader(list(type = "sav_inner", read = haven::read_sav), "read", path,
              reader_options = reader_options, ...)
}

read_delimited_full <- function(path, col_classes = NULL, reader_options = list()) {
  reader_options <- .resolve_delimited_reader_options(path, reader_options)
  if (.uses_delimited_logical_stream(reader_options)) {
    return(.collect_delimited_logical_records(path, reader_options, Inf, col_classes))
  }
  args <- c(list(file = path), delimited_fread_args(reader_options, col_classes = col_classes))
  df <- .fread_with_structural_errors(args, path)
  df <- .normalize_delimited_frame(df, reader_options, path)
  canonicalize_dataframe_names(df)
}

.read_delimited_header <- function(path, reader_options = list()) {
  reader_options <- .resolve_delimited_reader_options(path, reader_options)
  if (.uses_delimited_logical_stream(reader_options)) {
    info <- .delimited_header_info(path, reader_options)
    header <- info$Header
    attr(header, "repoquet_encoding_info") <- info$ReaderOptions$.EncodingInfo
    return(header)
  }
  df <- .fread_with_structural_errors(
    c(list(file = path, nrows = 0L), delimited_fread_args(reader_options)), path)
  df <- .normalize_delimited_frame(df, reader_options, path)
  names(df)
}

.read_delimited_sample <- function(path, reader_options = list()) {
  reader_options <- .resolve_delimited_reader_options(path, reader_options)
  if (.uses_delimited_logical_stream(reader_options)) {
    return(.collect_delimited_logical_records(path, reader_options, 100000L))
  }
  df <- .fread_with_structural_errors(
    c(list(file = path, nrows = 100000L), delimited_fread_args(reader_options)), path)
  df <- .normalize_delimited_frame(df, reader_options, path)
  canonicalize_dataframe_names(df)
}

.count_delimited_rows <- function(path, reader_options = list()) {
  reader_options <- .resolve_delimited_reader_options(path, reader_options)
  if (.uses_delimited_logical_stream(reader_options)) {
    count <- 0
    .stream_delimited_logical_records(
      path, reader_options = reader_options, chunk_size = 100000L,
      callback = function(df) count <<- count + nrow(df))
    return(as.numeric(count))
  }
  nrow(.fread_with_structural_errors(
    c(list(file = path, select = 1L), delimited_fread_args(reader_options)), path))
}

.read_delimited_chunk <- function(path, offset, n_max, header, col_classes = NULL,
                                  reader_options = list()) {
  reader_options <- .resolve_delimited_reader_options(path, reader_options)
  args <- delimited_fread_args(reader_options)
  args$colClasses <- fread_col_classes_positional(col_classes, header)
  df <- .fread_with_structural_errors(
    c(list(file = path, skip = offset + 1L, nrows = n_max,
           header = FALSE, col.names = header), args), path)
  df <- .normalize_delimited_frame(df, reader_options, path)
  canonicalize_dataframe_names(df)
}

register_builtin_file_readers <- function() {
  #### Delimited family: fread auto-detects separators and reads .gz/.bz2   ####
  #### natively, so csv/tsv/txt/gz share one implementation. Declared-type  ####
  #### information does not exist, so type inference samples deep.          ####
  for (tp in c("csv", "tsv", "txt", "gz")) {
    register_file_reader(tp,
      read_full   = function(p, col_classes = NULL, reader_options = list()) read_delimited_full(p, col_classes, reader_options),
      read_header = function(p, reader_options = list()) .read_delimited_header(p, reader_options),
      read_sample = function(p, reader_options = list()) .read_delimited_sample(p, reader_options),
      count_rows  = function(p, reader_options = list()) .count_delimited_rows(p, reader_options),
      read_chunk = function(p, offset, n_max, header, col_classes = NULL, reader_options = list())
        .read_delimited_chunk(p, offset, n_max, header, col_classes, reader_options),
      chunkable = TRUE)
  }
  #### Haven family: column types and labels are declared in the header, so ####
  #### small samples are authoritative and label harvesting is supported.   ####
  register_file_reader("sav",
    read_full   = function(p, reader_options = list()) read_haven_full(
      p, function(path) call_reader(list(type = "sav_inner", read = haven::read_sav), "read", path,
                                    reader_options = reader_options)),
    read_header = function(p, reader_options = list()) names(call_reader(
      list(type = "sav_inner", read = haven::read_sav), "read", p,
      reader_options = reader_options, n_max = 0L)),
    read_sample = function(p, reader_options = list()) call_reader(
      list(type = "sav_inner", read = haven::read_sav), "read", p,
      reader_options = reader_options, n_max = 1000L),
    count_rows  = function(p) nrow(haven::read_sav(p, col_select = 1L)),
    has_labels  = TRUE,
    read_labels_header = function(p) haven::read_sav(p, n_max = 0L),
    chunkable   = TRUE)
  register_file_reader("dta",
    read_full   = function(p) read_haven_full(p, haven::read_dta),
    read_header = function(p) names(haven::read_dta(p, n_max = 0L)),
    read_sample = function(p) haven::read_dta(p, n_max = 1000L),
    count_rows  = function(p) nrow(haven::read_dta(p, col_select = 1L)),
    has_labels  = TRUE,
    read_labels_header = function(p) haven::read_dta(p, n_max = 0L))
  register_file_reader("sas7bdat",
    read_full   = function(p) read_haven_full(p, haven::read_sas),
    read_header = function(p) names(haven::read_sas(p, n_max = 0L)),
    read_sample = function(p) haven::read_sas(p, n_max = 1000L),
    count_rows  = function(p) nrow(haven::read_sas(p, col_select = 1L)),
    has_labels  = TRUE,
    read_labels_header = function(p) haven::read_sas(p, n_max = 0L))
  register_file_reader("xpt",
    read_full   = function(p) read_haven_full(p, haven::read_xpt),
    read_header = function(p) names(haven::read_xpt(p, n_max = 0L)),
    read_sample = function(p) haven::read_xpt(p, n_max = 1000L),
    count_rows  = function(p) nrow(haven::read_xpt(p, col_select = 1L)),
    has_labels  = TRUE,
    read_labels_header = function(p) haven::read_xpt(p, n_max = 0L))
  #### Columnar / serialized inputs (e.g. re-partitioning existing data).   ####
  register_file_reader("parquet",
    read_full   = function(p) data.table::as.data.table(arrow::read_parquet(p)),
    read_header = function(p) names(arrow::read_parquet(p, as_data_frame = FALSE)$schema),
    read_sample = function(p) utils::head(data.table::as.data.table(arrow::read_parquet(p)), 100000L),
    count_rows  = function(p) arrow::read_parquet(p, as_data_frame = FALSE)$num_rows)
  register_file_reader("rds",
    read_full   = function(p) { x <- readRDS(p); if (!is.data.frame(x)) stop("RDS file does not contain a data frame."); data.table::as.data.table(x) },
    read_header = function(p) names(readRDS(p)),
    read_sample = function(p) utils::head(data.table::as.data.table(readRDS(p)), 100000L),
    count_rows  = function(p) nrow(readRDS(p)))
  invisible(supported_file_types())
}
register_builtin_file_readers()

strip_haven <- function(df) {
  data.table::setDT(df)
  haven_cols <- names(df)[sapply(df, inherits, "haven_labelled")]
  for (col in haven_cols) {
    base_type <- class(df[[col]])[length(class(df[[col]]))]
    new_val <- switch(base_type,
                      "double"    = as.double(df[[col]]),
                      "numeric"   = as.numeric(df[[col]]),
                      "integer"   = as.integer(df[[col]]),
                      "character" = as.character(df[[col]]),
                      "logical"   = as.logical(df[[col]]),
                      as.character(df[[col]])
                      )
    data.table::set(df, j = col, value = new_val)
    rm(new_val)
  }
  canonicalize_dataframe_names(df)
}
#########################################
#### align all columns across tables ####
#########################################
#' Pad a data frame with typed \code{NA} columns to match a target column set
#'
#' Ensures that \code{df} contains every column listed in \code{all_cols}.
#' Columns present in \code{all_cols} but absent from \code{df} are added with
#' the appropriate typed \code{NA} value (e.g. \code{NA_integer_} for integer
#' columns).  This is required before row-binding data frames from different
#' years, which may not share exactly the same column set.
#' @param df A data frame or data.table.
#' @param all_cols Character vector of column names that must be
#'   present in the output.
#' @param comprehensive_sample Named list (optional). Maps column names to their
#'   R class strings, used to determine the type of added \code{NA} columns.
#'   When \code{NULL} or the column is not present in the list, defaults to
#'   \code{NA_character_}.
#' @return The input \code{df} (as a \code{data.table}) with all missing
#'   columns added as typed \code{NA} vectors.
#' @seealso \code{\link{build_col_classes}}, \code{\link{strip_haven}}
#' @examples
#' \dontrun{
#' set.seed(1)
#' df <- data.frame(AGE = sample(18:90, 5), SEX = sample(c("M","F"), 5, replace = TRUE))
#' all_cols <- c("AGE", "SEX", "RACE", "ZIPINC_QRTL")
#' #### Without type hints, missing columns default to NA_character_ ####
#' df1 <- align_columns(df, all_cols)
#' sapply(df1, class)
#' #### With comprehensive_sample, missing columns get the correct type ####
#' comprehensive_sample <- list(RACE = "integer", ZIPINC_QRTL = "numeric")
#' df2 <- align_columns(df, all_cols, comprehensive_sample)
#' sapply(df2, class)
#' }
#' @export
align_columns <- function(df, all_cols, comprehensive_sample = NULL, max_coerce_na_pct = NULL) {
  if (!data.table::is.data.table(df)) df <- strip_haven(df)
  data.table::setDT(df)
  df <- canonicalize_dataframe_names(df)
  all_cols <- unique(canonical_colnames(all_cols))
  if (!is.null(comprehensive_sample)) names(comprehensive_sample) <- canonical_colnames(names(comprehensive_sample))
  missing <- setdiff(all_cols, colnames(df))
  if(length(missing) > 0) {
    for (col in missing) {
      col_class <- if (!is.null(comprehensive_sample) && col %in% names(comprehensive_sample)) {
        comprehensive_sample[[col]]
      } else { "character" }
      data.table::set(df, j = col, value = make_typed_na(col_class, n = nrow(df)))
    }
  }
  df <- enforce_col_classes(df, comprehensive_sample, max_coerce_na_pct = max_coerce_na_pct)
  ordered_cols <- c(intersect(all_cols, colnames(df)), setdiff(colnames(df), all_cols))
  data.table::setcolorder(df, ordered_cols)
  return(df)
}
################################################################################
#### Column-class inference ####################################################
################################################################################
#' Infer the agreed R class for every column across a set of files
#'
#' Reads the first 100 rows from each file in parallel using
#' \code{future_lapply()}, strips \pkg{haven} attributes from SAV samples, and
#' for each column records the set of R classes seen across all files.  If all
#' files agree on a single non-character class, that class is returned;
#' otherwise \code{"character"} is returned as the safe common denominator.
#' Parallelism is scoped within this function: a \code{multisession} plan is
#' activated immediately before \code{future_lapply()} and restored to the
#' prior plan immediately after.  This prevents workers from remaining alive
#' during the sequential Parquet write phase, which would cause file-lock
#' conflicts on Windows.
#' @param files Character vector of file paths (full paths when
#'   \code{base_path = ""}, relative otherwise).
#' @param base_path Character scalar. Prepended to each element of \code{files}
#'   when non-empty.  Pass \code{""} when \code{files} already contains full
#'   paths.
#' @param reader One of \code{"sav"} (default) or \code{"csv"}.  Controls
#'   which reader is used for the sample.
#' @return A named list where each element is a character scalar giving the
#'   agreed R class for that column (e.g. \code{list(AGE = "integer",
#'   DXCCS1 = "character")}).
#' @seealso \code{\link{build_comprehensive}}, \code{\link{align_columns}}
#' @examples
#' \dontrun{
#' set.seed(1)
#' tmp_dir <- tempfile("savfiles_")
#' dir.create(tmp_dir)
#' for (yr in 2018:2019) {
#'   df <- data.frame(AGE = sample(18:90, 50, replace = TRUE),
#'                    SEX = haven::labelled(sample(1:2, 50, replace = TRUE), c(Male = 1, Female = 2)),
#'                    DX1 = sample(c("A123", "B456", "9876"), 50, replace = TRUE) )
#'   haven::write_sav(df, file.path(tmp_dir, sprintf("DEMO_%d_Core.sav", yr)))
#' }
#' files <- list.files(tmp_dir, full.names = TRUE)
#' col_classes <- build_col_classes(files = files, base_path = "", reader = "sav")
#' str(col_classes)
#' unlink(tmp_dir, recursive = TRUE)
#' }
# Run independent metadata scans in parallel, then retry only worker failures
# in the main R process. This protects mapped/network drives that are visible
# to the interactive session but not inherited by multisession workers.
.parallel_scan_with_serial_retry <- function(items, scan_one, n_workers = 1,
                                            future_packages = character(),
                                            is_failure = is.null,
                                            context = "source metadata scan") {
  if (length(items) == 0L) return(list())
  if (as.integer(n_workers) <= 1L) return(lapply(items, scan_one))

  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  future::plan(future::multisession, workers = as.integer(n_workers))
  parallel_error <- NULL
  results <- tryCatch(
    future.apply::future_lapply(items, scan_one, future.seed = NULL,
                                future.packages = future_packages),
    error = function(e) {
      parallel_error <<- conditionMessage(e)
      NULL
    }
  )
  future::plan(old_plan)

  if (is.null(results)) {
    log_msg(sprintf("[PARALLEL FALLBACK] %s failed before producing results (%s); retrying all %d item(s) serially.",
                    context, parallel_error, length(items)))
    return(lapply(items, scan_one))
  }

  failed <- vapply(results, is_failure, logical(1))
  if (!any(failed)) return(results)

  retry_indices <- which(failed)
  failure_messages <- unique(vapply(results[retry_indices], function(result) {
    if (is.list(result) && !is.null(result$error) && length(result$error) > 0L) {
      as.character(result$error[1])
    } else {
      "unknown worker failure"
    }
  }, character(1)))
  log_msg(sprintf("[PARALLEL FALLBACK] %s failed for %d item(s); retrying those item(s) serially in the main R process.",
                  context, length(retry_indices)))
  log_msg(sprintf("[PARALLEL FALLBACK] First worker error(s): %s",
                  paste(utils::head(failure_messages, 3L), collapse = " | ")))
  retried <- lapply(items[retry_indices], scan_one)
  results[retry_indices] <- retried
  recovered <- !vapply(retried, is_failure, logical(1))
  if (any(recovered)) {
    log_msg(sprintf("[PARALLEL FALLBACK] %s recovered %d of %d failed item(s) serially.",
                    context, sum(recovered), length(retry_indices)))
  }
  results
}

#' @export
build_col_classes <- function(files, base_path, n_workers = 1, reader = "sav",
                              reader_options = NULL){
  #### The registry encodes the right sample depth per format: declared-    ####
  #### type files (haven family) sample shallow, delimited files deep.      ####
  all_paths <- if (nchar(base_path) == 0) files else file.path(base_path, files)
  readers <- if (length(reader) == 1L) rep(reader, length(all_paths)) else reader
  if (length(readers) != length(all_paths)) stop("reader must have length 1 or match files.")
  if (is.null(reader_options)) reader_options <- rep(list(list()), length(all_paths))
  if (!is.list(reader_options) || length(reader_options) != length(all_paths)) {
    stop("reader_options must be a list with one element per file.")
  }
  scan_one <- function(i) {
    p <- all_paths[i]
    tryCatch({
      rd <- get_file_reader(readers[i])
      df <- call_reader(rd, "read_sample", p, reader_options = reader_options[[i]])
      #### strip_haven is a no-op on frames without labelled columns and    ####
      #### canonicalizes names either way.                                  ####
      df <- strip_haven(df)
      cls <- sapply(df, function(col) {
        #### fread represents an all-missing sample as logical even when   ####
        #### the real source type is unknown. Do not let that sentinel     ####
        #### override an observed type from another file. A logical column ####
        #### with at least one TRUE/FALSE value remains genuinely logical. ####
        if (is.logical(col) && !any(!is.na(col))) "unknown" else normalize_type_name(class(col)[1])
      })
      list(ok = TRUE, path = p, classes = cls, error = NA_character_)
    }, error = function(e) list(ok = FALSE, path = p, classes = character(0), error = conditionMessage(e)))
  }
  all_samples <- .parallel_scan_with_serial_retry(
    seq_along(all_paths), scan_one, n_workers = n_workers,
    future_packages = c("data.table", "haven"),
    is_failure = function(x) !is.list(x) || !isTRUE(x$ok),
    context = "column-class inference"
  )
  failed <- vapply(all_samples, function(x) !isTRUE(x$ok), logical(1))
  if (any(failed)) {
    failed_msg <- vapply(all_samples[failed], function(x) sprintf("%s (%s)", x$path, x$error), character(1))
    for (msg in failed_msg) log_msg(sprintf("[SCHEMA ERROR] Could not infer column classes from sample: %s", msg))
    stop(sprintf("Column-class inference failed for %d source file(s). See [SCHEMA ERROR] log entries.", sum(failed)))
  }
  col_types_seen <- list()
  for(sample_result in all_samples){
    sample <- sample_result$classes
    if(length(sample) == 0){ next }
    names(sample) <- canonical_colnames(names(sample))
    for(col in names(sample)) col_types_seen[[col]] <- unique(c(col_types_seen[[col]], sample[[col]]))
  }
  col_class_map <- lapply(names(col_types_seen), function(col) promote_types(col_types_seen[[col]], col))
  names(col_class_map) <- names(col_types_seen)
  col_class_map
}
################################################################################
#### File readers ##############################################################
################################################################################
#' Safely read a SAV file with UTF-8 sanitisation
#'
#' Wraps \code{haven::read_sav()} with \code{tryCatch}.  After loading,
#' \pkg{haven} S3 attributes are stripped via \code{\link{strip_haven}} and all
#' character columns are re-encoded to UTF-8 (replacing unmappable bytes with
#' their hex escape).
#' @param path Character. Full path to the \code{.sav} file.
#' @return A \code{data.table} with all columns in their declared base types, or
#'   an empty \code{data.frame()} if reading fails.
#' @seealso \code{\link{safe_read_sav_chunked}}, \code{\link{strip_haven}}
#' @examples
#' \dontrun{
#' set.seed(1)
#' tmp_sav <- tempfile(fileext = ".sav")
#' df <- data.frame(AGE = sample(18:90, 20, replace = TRUE),
#'                  SEX = haven::labelled(sample(1:2, 20, replace = TRUE), c(Male = 1, Female = 2)) )
#' haven::write_sav(df, tmp_sav)
#' df_loaded <- safe_read_sav(tmp_sav)
#' str(df_loaded) # SEX is now a plain integer, not haven_labelled
#' unlink(tmp_sav)
#' }
#' @export
safe_read_sav <- function(path){
  tryCatch({
    df <- haven::read_sav(path)
    df <- strip_haven(df)
    normalize_character_encoding(df)
  }, error = function(e) data.frame())
}

################################################################################################
#### Read a large SAV file in memory-bounded chunks and write to one Parquet file per chunk ####
################################################################################################
#' Read a large SAV file in memory-bounded chunks and write directly to Parquet
#'
#' For SAV files that exceed \code{SAV_ROW_THRESHOLD} rows, loading the entire
#' file into R at once would exhaust available RAM.  This function reads the
#' file in batches of \code{chunk_size} rows using \code{haven::read_sav(skip,
#' n_max)}, processes each chunk in-place (strip haven, UTF-8 sanitise, column
#' align, type enforce, non-finite replacement), converts it to an Arrow table,
#' and writes it as an individual numbered Parquet file directly into
#' \code{year_dir}.  Peak RAM is bounded to one chunk regardless of total file
#' size.
#'
#' Chunk files are named \code{<stem>_<NNNNN>.parquet} and stored directly in
#' the hive-partitioned \code{year=<year_val>/} directory.  DuckDB reads all
#' \code{*.parquet} files in that directory as a single virtual table via the
#' glob pattern in \code{\link{register_parquet_view}}.
#'
#' Total row count is read from the SPSS file header (\code{ncases} attribute
#' from \code{haven::read_sav(n_max = 0)}) before the chunk loop so that
#' \code{skip} never exceeds the declared row count (which would cause
#' \emph{"file did not contain the expected number of rows"}).
#'
#' Schema conflicts across chunks (e.g. a column inferred as \code{int32} in
#' one chunk and \code{string} in another due to mixed ICD-10 / ICD-9 codes)
#' are resolved by promoting the conflicting field to \code{utf8} and
#' re-casting all prior chunks.
#'
#' \strong{Adaptive chunk-size reduction:} if a chunk fails (most commonly
#' \emph{"cannot allocate vector of size ... Mb"} because \code{chunk_size} is
#' still too large for this table's column count/width), the chunk size is
#' reduced by \code{chunk_size_decrement} and the same offset is retried. Once
#' a working size is found it is reused for all remaining chunks of this file,
#' but \code{chunk_size} as passed to the next file by
#' \code{\link{generic_db_loader}} is unaffected. If \code{min_chunk_size} is
#' reached without success, the function falls back to \code{\link{safe_read_sav}}.
#' @param path Character. Full path to the \code{.sav} file.
#' @param chunk_size Integer. Number of rows per chunk.  Defaults to
#'   \code{SAV_CHUNK_SIZE} from the calling environment.
#' @param year_dir Character. Full path to the hive-partitioned year
#'   directory where chunk Parquet files will be written
#'   (e.g. \code{"/data/parquet/NIS_Core/year=2019"}).
#' @param out_path Character (optional). Reserved for compatibility; not
#'   used in the current implementation.
#' @param all_cols Character vector (optional). Union of all columns across
#'   years for this table, passed to \code{\link{align_columns}}.
#' @param col_classes Named list (optional). Column class map from
#'   \code{\link{build_col_classes}}, used for type enforcement.
#' @param year_val Integer or character (optional). Year value passed to
#'   \code{\link{add_year_if_missing}}.
#' @param chunk_size_decrement Integer (optional). Rows to subtract from the
#'   chunk size after a chunk fails (e.g. with a memory-allocation error).
#'   Defaults to 10\% of \code{chunk_size}. The reduction is local to this
#'   call because \code{chunk_size} itself is unmodified, so the next file loaded
#'   by \code{\link{generic_db_loader}} receives the original value.
#' @param min_chunk_size Integer (optional). Smallest chunk size that will be
#'   attempted before giving up on the chunked reader and falling back to
#'   \code{\link{safe_read_sav}}. Defaults to \code{chunk_size_decrement}
#'   (i.e. up to ~9 reductions from the original \code{chunk_size}).
#' @param MaxFileStemTruncate Logical. If \code{TRUE}, chunk file stems are
#'   shortened to reduce Windows path-length failures.
#' @return A list with \code{written = TRUE} and \code{n_rows} on success. A
#'   fatal chunking, schema, or row-count error stops the load so the source is
#'   not checkpointed.
#' @seealso \code{\link{safe_read_sav}}, \code{\link{strip_haven}},
#'   \code{\link{align_columns}}
#' @examples
#' \dontrun{
#' set.seed(1)
#' tmp_sav <- tempfile(fileext = ".sav")
#' df <- data.frame(AGE = sample(18:90, 250, replace = TRUE),
#'                  SEX = haven::labelled(sample(1:2, 250, replace = TRUE), c(Male = 1, Female = 2)) )
#' haven::write_sav(df, tmp_sav)
#' year_dir <- tempfile("year_2020_")
#' dir.create(year_dir, recursive = TRUE)
#' #### chunk_size = 100 over 250 rows -> 3 chunk files ####
#' result <- safe_read_sav_chunked(path = tmp_sav,
#'                                 chunk_size = 100L,
#'                                 year_dir = year_dir,
#'                                 year_val = 2020)
#' result$written
#' list.files(year_dir)
#' unlink(c(tmp_sav, year_dir), recursive = TRUE)
#' }
#' @export
safe_read_sav_chunked <- function(path, chunk_size = 1000000L, TerminalHivePartition = FALSE,
                                  year_dir = NULL, out_path = NULL, all_cols = NULL,
                                  col_classes = NULL, year_val = NULL,
                                  chunk_size_decrement = NULL, min_chunk_size = NULL,
                                  ManifestPath = NULL, Database = NULL, TableName = NULL,
                                  DuckDBTable = NULL, SourcePath = NULL, SchemaHash = NA_character_,
                                  partition_keys = "YEAR", partition_values = NULL,
                                  MaxFileStemTruncate = FALSE,
                                  accept_partial = FALSE, max_coerce_na_pct = NULL,
                                  reader_options = list(), RepositoryLock = NULL) {
  partition_keys <- canonical_colnames(partition_keys)
  direct_write <- !is.null(out_path) || TerminalHivePartition
  log_msg(sprintf("[CHUNKED] %s (TerminalHivePartition=%s, chunk_size=%d)", basename(path), TerminalHivePartition, chunk_size))
  chunk_decrement <- if(is.null(chunk_size_decrement)){ max(1L, as.integer(chunk_size * 0.1)) } else { as.integer(chunk_size_decrement) }
  chunk_floor <- if(is.null(min_chunk_size)){ chunk_decrement} else { as.integer(min_chunk_size) }
  current_chunk_size <- chunk_size
  if(!is.null(col_classes)) names(col_classes) <- canonical_colnames(names(col_classes))
  if(!is.null(all_cols)) all_cols <- unique(canonical_colnames(all_cols))
  written_chunk_files <- character(0)
  completion_status <- "completed"
  cleanup_chunk_outputs <- function() {
    paths <- unique(written_chunk_files[nzchar(written_chunk_files)])
    if (length(paths) == 0L) return(invisible(0L))
    remove_parquet_manifest_rows(ManifestPath = ManifestPath,
                                 SourcePath = SourcePath %||% path,
                                 ParquetPath = paths)
    targets <- if (TerminalHivePartition) unique(dirname(paths)) else paths
    targets <- targets[file.exists(targets)]
    if (length(targets) > 0L) unlink(targets, recursive = TRUE)
    invisible(length(targets))
  }
  tryCatch({
    total_rows <- tryCatch({
      meta <- read_sav_with_options(path, reader_options, n_max = 0L)
      nc   <- attr(meta, "ncases")
      rm(meta)
      if (!is.null(nc) && is.numeric(nc) && nc > 0) as.integer(nc) else NA_integer_
    }, error = function(e) NA_integer_)
    if (is.na(total_rows)) {
      log_msg(sprintf("[CHUNKED] ncases unavailable -- counting: %s", basename(path)))
      total_rows <- tryCatch(nrow(read_sav_with_options(path, reader_options, col_select = 1L)),
                             error = function(e) NA_integer_)
    }
    if (is.na(total_rows)) {
      log_msg(sprintf("[CHUNKED] Cannot determine row count -- switching to EOF-based chunking: %s", basename(path)))
    } else {
      log_msg(sprintf("[CHUNKED] %s: %d rows", basename(path), total_rows))
    }
    file_stem <- parquet_chunk_stem(path, partition_dir = year_dir,
                                    TerminalHivePartition = TerminalHivePartition,
                                    MaxFileStemTruncate = MaxFileStemTruncate)
    if (!is.null(year_dir) && dir.exists(year_dir)) {
      if (TerminalHivePartition) {
        stale_chunks <- list.dirs(year_dir, recursive = FALSE, full.names = TRUE)
        stale_chunks <- stale_chunks[grepl(paste0("^batch_id=", regex_escape(file_stem), "_[0-9]{5}$"),
                                           basename(stale_chunks), ignore.case = TRUE)]
      } else {
        stale_chunks <- list.files(year_dir,
                                   pattern = paste0("^", regex_escape(file_stem), "_[0-9]{5}\\.parquet$"),
                                   full.names = TRUE)
      }
      if (length(stale_chunks) > 0L) {
        stale_manifest_paths <- if (TerminalHivePartition) file.path(stale_chunks, "data.parquet") else stale_chunks
        remove_parquet_manifest_rows(ManifestPath = ManifestPath,
                                     SourcePath = SourcePath %||% path,
                                     ParquetPath = stale_manifest_paths)
        unlink(stale_chunks, recursive = TRUE)
        log_msg(sprintf("[CHUNKED] Removed %d stale chunk file/director%s before retrying %s",
                        length(stale_chunks), ifelse(length(stale_chunks) == 1L, "y", "ies"), basename(path)))
      }
    }
    ref_schema <- NULL
    offset <- 0L
    chunk_num <- 1L
    total_written <- 0L
    repeat {
      touch_repository_lock(RepositoryLock)
      if(!is.na(total_rows) && offset >= total_rows){ break }
      repeat {
        n_this_chunk <- if (is.na(total_rows)){ current_chunk_size
                        } else { min(current_chunk_size, total_rows - offset) }
        if (TerminalHivePartition) {
          batch_dir  <- file.path(year_dir, sprintf("batch_id=%s_%05d", file_stem, chunk_num))
          dir.create(batch_dir, recursive = TRUE, showWarnings = FALSE)
          chunk_file <- file.path(batch_dir, "data.parquet")
        } else {
          chunk_file <- file.path(year_dir, sprintf("%s_%05d.parquet", file_stem, chunk_num))
        }
        chunk_attempt <- tryCatch({
          df_chunk <- read_sav_with_options(path, reader_options, skip = offset, n_max = n_this_chunk)
          if (is.null(df_chunk) || nrow(df_chunk) == 0L) {
            list(status = "empty")
          } else {
            df_chunk <- strip_haven(df_chunk)
            df_chunk <- normalize_character_encoding(df_chunk, reader_options$Encoding %||% NULL)
            if (direct_write) {
              if (!is.null(year_val) && "YEAR" %in% partition_keys){ df_chunk <- add_year_if_missing(df_chunk, year_val) }
              if (!is.null(all_cols)){ df_chunk <- align_columns(df_chunk, all_cols, col_classes, max_coerce_na_pct = max_coerce_na_pct) }
            } else {
              df_chunk <- canonicalize_dataframe_names(df_chunk)
              df_chunk <- enforce_col_classes(df_chunk, col_classes, max_coerce_na_pct = max_coerce_na_pct)
            }
            num_cols <- names(df_chunk)[sapply(df_chunk, is.numeric)]
            for (col in num_cols) {
              vals    <- df_chunk[[col]]
              bad_idx <- which(!is.finite(vals) & !is.na(vals))
              if (length(bad_idx) > 0) data.table::set(df_chunk, i = bad_idx, j = col, value = NA_real_)
              rm(vals)
            }
            validate_partition_column_values(df_chunk, partition_keys, partition_values,
                                             source_label = basename(path))
            for (pk in intersect(partition_keys, names(df_chunk))) df_chunk[, (pk) := NULL]
            arrow_tbl <- arrow::as_arrow_table(df_chunk)
            rm(df_chunk)
            if (is.null(ref_schema)) {
              if (!is.null(col_classes)) {
                ref_schema <- arrow_schema_from_classes(arrow_tbl, col_classes)
              } else {
                ref_schema <- arrow_tbl$schema
              }
              arrow_tbl <- tryCatch(arrow_tbl$cast(ref_schema),
                                     error = function(e) {
                                       stop(sprintf("Initial chunk could not be cast to agreed schema: %s", e$message))
                                     })
            } else {
              arrow_tbl <- tryCatch(
                arrow_tbl$cast(ref_schema),
                error = function(e) {
                  log_msg(sprintf("[CHUNKED] Schema conflict chunk %d: %s -- promoting to string", chunk_num, e$message))
                  conflict_field <- regmatches(e$message, regexpr("(?<=Field )\\S+", e$message, perl = TRUE))
                  if (length(conflict_field) > 0) {
                    fi <- ref_schema$GetFieldIndex(conflict_field)
                    if (fi >= 0L) {
                      new_fields <- ref_schema$fields
                      new_fields[[fi + 1L]] <- arrow::field(conflict_field, arrow::utf8())
                      ref_schema <<- arrow::schema(new_fields)
                      #### Chunks already on disk carry the old type; re-cast them ####
                      #### so every file in this year directory agrees.            ####
                      for (prev_file in written_chunk_files) {
                        recast_err <- tryCatch({
                          prev_tbl <- arrow::read_parquet(prev_file, as_data_frame = FALSE)
                          write_arrow_table_safely(prev_tbl$cast(ref_schema), prev_file)
                          rm(prev_tbl)
                          NULL
                        }, error = function(e3) e3)
                        if (is.null(recast_err)) {
                          log_msg(sprintf("[CHUNKED] Re-cast prior chunk to promoted schema: %s", basename(prev_file)))
                        } else {
                          stop(sprintf("Could not re-cast prior chunk %s after schema promotion: %s",
                                       basename(prev_file), recast_err$message))
                        }
                      }
                    } }
                  tryCatch(arrow_tbl$cast(ref_schema),
                           error = function(e2) {
                             stop(sprintf("Current chunk could not be cast after schema promotion: %s", e2$message))
                           })
                })
            }
            chunk_write_attempts <- 3L
            chunk_wait_s <- c(0L, 10L, 30L)
            for (cattempt in seq_len(chunk_write_attempts)) {
              chunk_write_err <- tryCatch({
                if (cattempt > 1L) {
                  log_msg(sprintf("[CHUNKED WRITE RETRY] Attempt %d/%d after %ds: %s",
                                  cattempt, chunk_write_attempts, chunk_wait_s[cattempt], basename(chunk_file)))
                  Sys.sleep(chunk_wait_s[cattempt])
                }
                write_arrow_table_safely(arrow_tbl, chunk_file)
                #### Track the file before manifest update. If the manifest ####
                #### write fails, outer cleanup must still remove this chunk. ####
                written_chunk_files <- unique(c(written_chunk_files, chunk_file))
                NULL
              }, error = function(e) e)
              if (is.null(chunk_write_err)) break
              if (cattempt == chunk_write_attempts) stop(chunk_write_err)
              log_msg(sprintf("[CHUNKED WRITE RETRY] Chunk write attempt %d failed: %s", cattempt, chunk_write_err$message))
            }
            n_written <- arrow_tbl$num_rows
            rm(arrow_tbl)
            update_parquet_manifest(ManifestPath = ManifestPath, Database = Database, TableName = TableName,
                                    DuckDBTable = DuckDBTable, Year = year_val, SourcePath = SourcePath %||% path,
                                    ParquetPath = chunk_file, NRows = n_written,
                                    SchemaHash = SchemaHash, Status = "written",
                                    Notes = sprintf("chunk_%05d", chunk_num),
                                    PartitionKey = partition_keys, PartitionValue = partition_values)
            list(status = "ok", n_rows = n_written, chunk_file = chunk_file)
          }
        }, error = function(e) list(status = "error", message = e$message))

        if(chunk_attempt$status != "error"){ break }
        #### A partition-value disagreement or coercion-threshold breach is a ####
        #### data/workbook problem, not a transient read failure -- shrinking ####
        #### the chunk cannot fix it, so fail the file immediately instead of ####
        #### entering the retry loop.                                         ####
        if (grepl("disagree with the workbook partition value|exceeds the coercion NA threshold",
                  chunk_attempt$message)) {
          stop(chunk_attempt$message)
        }
        is_ncases_boundary <- offset > 0L &&
          grepl("did not contain the expected number of rows", chunk_attempt$message, fixed = TRUE)
        if (is_ncases_boundary || current_chunk_size <= chunk_floor) {
          if (is_ncases_boundary) {
            log_msg(sprintf(
              "[CHUNKED] %s chunk %d: skip=%d exceeds declared ncases -- switching to foreign::read.spss() for tail read",
              basename(path), chunk_num, offset))
          } else {
            log_msg(sprintf("[CHUNKED] %s: chunk %d failed even at floor size %d rows: %s -- switching to foreign::read.spss() for tail read",
                            basename(path), chunk_num, chunk_floor, chunk_attempt$message))
          }
          tail_df <- tryCatch({
            if (!requireNamespace("foreign", quietly = TRUE)){
              stop("package 'foreign' is required for tail read but is not installed") }
            full_df <- foreign::read.spss(path, to.data.frame = TRUE, use.value.labels = FALSE, reencode = "UTF-8")
            full_df <- as.data.frame(full_df)
            if (nrow(full_df) > offset) {
              tail_rows <- full_df[(offset + 1L):nrow(full_df), , drop = FALSE]
              rm(full_df)
              tail_rows
            } else {
              n_full <- nrow(full_df)
              rm(full_df)
              log_msg(sprintf("[CHUNKED] %s: foreign read returned %d rows, offset=%d -- no tail data",
                              basename(path), n_full, offset))
              NULL
            }
          }, error = function(e) {
            log_msg(sprintf("[CHUNKED] %s: foreign::read.spss() tail read failed: %s",
                            basename(path), e$message))
            NULL
          })
          if(!is.null(tail_df) && nrow(tail_df) > 0) {
            tail_df <- strip_haven(tail_df)
            factor_cols <- names(tail_df)[sapply(tail_df, is.factor)]
            for(col in factor_cols){
              data.table::set(tail_df, j = col, value = as.character(tail_df[[col]])) }
            tail_df <- normalize_character_encoding(tail_df, reader_options$Encoding %||% NULL)
            if(direct_write) {
              if(!is.null(year_val) && "YEAR" %in% partition_keys){ tail_df <- add_year_if_missing(tail_df, year_val) }
              if(!is.null(all_cols)){ tail_df <- align_columns(tail_df, all_cols, col_classes, max_coerce_na_pct = max_coerce_na_pct) }
            } else {
              tail_df <- canonicalize_dataframe_names(tail_df)
              tail_df <- enforce_col_classes(tail_df, col_classes, max_coerce_na_pct = max_coerce_na_pct)
            }
            #### Inf/NaN in numeric cols -- Arrow cannot serialise these ####
            num_cols_tail <- names(tail_df)[sapply(tail_df, is.numeric)]
            for(col in num_cols_tail){
              bad <- which(!is.finite(tail_df[[col]]) & !is.na(tail_df[[col]]))
              if (length(bad) > 0){ tail_df[[col]][bad] <- NA_real_ }
            }
            validate_partition_column_values(tail_df, partition_keys, partition_values,
                                             source_label = basename(path))
            for (pk in intersect(partition_keys, names(tail_df))) tail_df[, (pk) := NULL]
            tail_arrow <- arrow::as_arrow_table(tail_df)
            rm(tail_df)
            if(!is.null(ref_schema)){
              tail_arrow <- tryCatch(tail_arrow$cast(ref_schema),
                                     error = function(e) {
                                       stop(sprintf("Tail chunk could not be cast to agreed schema: %s", e$message))
                                     })
            }
            tail_file <- if(TerminalHivePartition){
              batch_dir <- file.path(year_dir, sprintf("batch_id=%s_%05d", file_stem, chunk_num))
              dir.create(batch_dir, recursive = TRUE, showWarnings = FALSE)
              file.path(batch_dir, "data.parquet")
            } else {
              file.path(year_dir, sprintf("%s_%05d.parquet", file_stem, chunk_num))
            }
            write_arrow_table_safely(tail_arrow, tail_file)
            written_chunk_files <- unique(c(written_chunk_files, tail_file))
            n_tail <- tail_arrow$num_rows
            rm(tail_arrow)
            update_parquet_manifest(ManifestPath = ManifestPath, Database = Database, TableName = TableName,
                                    DuckDBTable = DuckDBTable, Year = year_val, SourcePath = SourcePath %||% path,
                                    ParquetPath = tail_file, NRows = n_tail,
                                    SchemaHash = SchemaHash, Status = "written",
                                    Notes = sprintf("tail_chunk_%05d", chunk_num),
                                    PartitionKey = partition_keys, PartitionValue = partition_values)
            total_written <- total_written + n_tail
            log_msg(sprintf("[CHUNKED] %s tail chunk: %d rows written via foreign::read.spss() -> %s",
                            basename(path), n_tail, basename(tail_file)))
          } else {
            log_msg(sprintf("[CHUNKED] %s: tail read produced no additional rows -- file may end at declared ncases boundary",
                            basename(path)))
          }
          chunk_attempt <- list(status = "tail_complete")
          break
        }
        new_chunk_size <- max(chunk_floor, current_chunk_size - chunk_decrement)
        log_msg(sprintf("[CHUNKED] %s: chunk %d failed at chunk_size=%d (%s) -- reducing to %d rows for the remainder of this file and retrying",
                        basename(path), chunk_num, current_chunk_size, chunk_attempt$message, new_chunk_size))
        current_chunk_size <- new_chunk_size
        gc(verbose = FALSE)
      }
      if(chunk_attempt$status %in% c("empty", "tail_complete")){ break }
      total_written <- total_written + chunk_attempt$n_rows
      written_chunk_files <- unique(c(written_chunk_files, chunk_attempt$chunk_file))
      gc(verbose = FALSE)
      log_msg(sprintf("[CHUNKED] %s chunk %d: %d rows -> %s (chunk_size=%d)",
                      basename(path), chunk_num, n_this_chunk, basename(chunk_attempt$chunk_file), current_chunk_size))
      offset    <- offset + n_this_chunk
      chunk_num <- chunk_num + 1L
      touch_repository_lock(RepositoryLock)
    }
    if(total_written == 0L){
      #### Verified empty vs failed read: a successfully determined row     ####
      #### count of exactly 0 is a true fact about the source file, not a   ####
      #### failure -- record it and checkpoint so the file is not retried   ####
      #### on every run. An unknown row count with 0 rows written stays a   ####
      #### hard failure.                                                    ####
      if (!is.na(total_rows) && total_rows == 0L) {
        log_msg(sprintf("[WARN] file=%s | issue=verified_empty_source | declared_rows=0 | Recording as complete with 0 rows.", basename(path)))
        update_parquet_manifest(ManifestPath = ManifestPath, Database = Database, TableName = TableName,
                                DuckDBTable = DuckDBTable, Year = year_val, SourcePath = SourcePath %||% path,
                                ParquetPath = year_dir, NRows = 0L,
                                SchemaHash = SchemaHash, Status = "empty",
                                Notes = "verified_empty_source",
                                PartitionKey = partition_keys, PartitionValue = partition_values)
        return(list(written = TRUE, n_rows = 0L, status = "empty"))
      }
      stop(sprintf("Chunked reader wrote 0 rows for %s; file may be empty or unreadable.", basename(path)))
    }
    if(current_chunk_size != chunk_size){
      log_msg(sprintf("[CHUNKED] %s: completed using reduced chunk_size=%d (original=%d). This reduction applies only to this file.",
                      basename(path), current_chunk_size, chunk_size))
    }
    if(is.na(total_rows)){
      log_msg(sprintf(
        "[WARN] file=%s | issue=ncases_unavailable_eof_chunking_used | declared_ncases=NA | rows_written=%d | The SPSS header ncases was unavailable or misreported. The file may be truncated -- verify against HCUP documentation or re-download.",
        basename(path), total_written))
    } else if(total_written != total_rows){
      msg <- sprintf(
        "file=%s | issue=row_count_mismatch_possible_truncation | declared_ncases=%d | rows_written=%d | File appears truncated -- verify against HCUP documentation or re-download.",
        basename(path), total_rows, total_written)
      #### AcceptPartial is the per-row, human-recorded acknowledgment in    ####
      #### the MDT workbook that this file's truncation has been verified    ####
      #### and accepted. Without it, truncation stays fatal so a partial     ####
      #### load can never silently checkpoint.                               ####
      if (!isTRUE(accept_partial)) {
        log_msg(sprintf("[CHUNKED ERROR] %s", msg))
        stop(msg)
      }
      log_msg(sprintf("[WARN] %s | AcceptPartial=TRUE for this MDT row -- recording partial load as complete.", msg))
      completion_status <- "partial_accepted"
      update_parquet_manifest(ManifestPath = ManifestPath, Database = Database, TableName = TableName,
                              DuckDBTable = DuckDBTable, Year = year_val, SourcePath = SourcePath %||% path,
                              ParquetPath = year_dir, NRows = total_written,
                              SchemaHash = SchemaHash, Status = "partial_accepted",
                              Notes = sprintf("declared_ncases=%d rows_written=%d", total_rows, total_written),
                              PartitionKey = partition_keys, PartitionValue = partition_values)
    }
    return(list(written = TRUE, n_rows = total_written, status = completion_status))
  }, error = function(e) {
    n_cleaned <- cleanup_chunk_outputs()
    if (n_cleaned > 0L) {
      log_msg(sprintf("[CHUNKED] Removed %d partial output%s after failed read of %s",
                      n_cleaned, ifelse(n_cleaned == 1L, "", "s"), basename(path)))
    }
    log_msg(sprintf("[CHUNKED] Post-loop error %s: %s", basename(path), e$message))
    stop(e)
  })
}

#' Stream a large delimited file to hive-partitioned Parquet in chunks
#'
#' Memory-bounded counterpart of \code{\link{safe_read_sav_chunked}} for the
#' delimited family (csv/tsv/txt/gz). Reuses the same machinery: stale-chunk
#' cleanup, per-chunk alignment against the agreed schema, partition-column
#' validation, a reference Arrow schema so every chunk writes identical
#' physical types, atomic writes, and per-chunk manifest rows. Simpler than
#' the SAV path by design: delimited files have no declared row count to
#' reconcile against, and a chunk read error fails the file rather than
#' entering a shrink-retry loop.
#'
#' Standard sources retain the fast line-offset reader. Sources configured
#' with \code{MalformedRowPolicy="append_previous"} use the same logical-record
#' stream as schema discovery, so quoted multiline fields and verified
#' one-field continuations remain memory bounded without intermediate files.
#' @export
read_delimited_chunked <- function(path, chunk_size = 1000000L, year_dir = NULL,
                                   all_cols = NULL, col_classes = NULL, year_val = NULL,
                                   TerminalHivePartition = FALSE,
                                   partition_keys = "YEAR", partition_values = NULL,
                                   max_coerce_na_pct = NULL,
                                   ManifestPath = NULL, Database = NULL, TableName = NULL,
                                   DuckDBTable = NULL, SourcePath = NULL, SchemaHash = NA_character_,
                                   MaxFileStemTruncate = FALSE,
                                   reader = "csv", reader_options = list(), RepositoryLock = NULL) {
  partition_keys <- canonical_colnames(partition_keys)
  if (!is.null(col_classes)) names(col_classes) <- canonical_colnames(names(col_classes))
  if (!is.null(all_cols)) all_cols <- unique(canonical_colnames(all_cols))
  rd <- get_file_reader(reader)
  if (is.null(rd$read_chunk)) stop(sprintf("Reader '%s' has no read_chunk callback.", reader))
  logical_stream <- .uses_delimited_logical_stream(reader_options)
  header <- call_reader(rd, "read_header", path, reader_options = reader_options)
  total_rows <- if (logical_stream) NA_real_ else
    as.numeric(call_reader(rd, "count_rows", path, reader_options = reader_options))
  log_msg(sprintf("[CHUNKED-DELIM] %s: %s (chunk_size=%d)", basename(path),
                  if (logical_stream) "streaming repaired logical records" else
                    paste(formatC(total_rows, format = "d", big.mark = ","), "rows"),
                  as.integer(chunk_size)))
  #### Pin fread's column types per chunk from the agreed classes. Without  ####
  #### this fread re-infers per chunk, so e.g. a character column that is   ####
  #### entirely empty within one chunk types as logical there (empty -> NA) ####
  #### while mixed chunks yield "" -- values would depend on chunk          ####
  #### composition.                                                         ####
  #### Positional list(type = indices) form: with header = FALSE fread      ####
  #### names columns V1..Vn before col.names applies, so NAMED colClasses   ####
  #### would silently never match.                                          ####
  chunk_col_classes <- NULL
  if (!is.null(col_classes)) {
    canon_hdr <- canonical_colnames(header)
    types <- vapply(canon_hdr, function(cn) {
      if (!is.null(col_classes[[cn]])) normalize_type_name(col_classes[[cn]]) else NA_character_
    }, character(1))
    ok <- which(types %in% c("character", "integer", "numeric", "logical"))
    if (length(ok) > 0L) chunk_col_classes <- split(ok, types[ok])
  }
  file_stem <- parquet_chunk_stem(path, partition_dir = year_dir,
                                  TerminalHivePartition = TerminalHivePartition,
                                  MaxFileStemTruncate = MaxFileStemTruncate)
  #### Stale chunks from a previous partial run of this file must go before ####
  #### rewriting, or leftover high-numbered chunks double-count rows.       ####
  if (!is.null(year_dir) && dir.exists(year_dir)) {
    stale <- if (TerminalHivePartition) {
      d <- list.dirs(year_dir, recursive = FALSE, full.names = TRUE)
      d[grepl(paste0("^batch_id=", regex_escape(file_stem), "_[0-9]{5}$"), basename(d), ignore.case = TRUE)]
    } else {
      list.files(year_dir, pattern = paste0("^", regex_escape(file_stem), "_[0-9]{5}\\.parquet$"), full.names = TRUE)
    }
    if (length(stale) > 0L) {
      remove_parquet_manifest_rows(ManifestPath = ManifestPath, SourcePath = SourcePath %||% path,
                                   ParquetPath = if (TerminalHivePartition) file.path(stale, "data.parquet") else stale)
      unlink(stale, recursive = TRUE)
      log_msg(sprintf("[CHUNKED-DELIM] Removed %d stale chunk output(s) before retrying %s", length(stale), basename(path)))
    }
  }
  written_files <- character(0)
  cleanup_outputs <- function() {
    paths <- unique(written_files[nzchar(written_files)])
    if (length(paths) == 0L) return(invisible(0L))
    remove_parquet_manifest_rows(ManifestPath = ManifestPath, SourcePath = SourcePath %||% path, ParquetPath = paths)
    targets <- if (TerminalHivePartition) unique(dirname(paths)) else paths
    targets <- targets[file.exists(targets)]
    if (length(targets) > 0L) unlink(targets, recursive = TRUE)
    invisible(length(targets))
  }
  ref_schema <- NULL
  offset <- 0L; chunk_num <- 1L; total_written <- 0L
  process_chunk <- function(df_chunk) {
    if (!is.data.frame(df_chunk)) stop(sprintf("Reader '%s' did not return a data frame chunk.", reader))
    data.table::setDT(df_chunk)
    #### Built-in delimited callbacks already return strict UTF-8. This   ####
    #### second pass enforces that reader contract without reinterpreting ####
    #### the normalized values as their original source encoding.         ####
    df_chunk <- normalize_character_encoding(df_chunk, "UTF-8")
    if (!is.null(year_val) && "YEAR" %in% partition_keys) df_chunk <- add_year_if_missing(df_chunk, year_val)
    if (!is.null(all_cols)) df_chunk <- align_columns(df_chunk, all_cols, col_classes,
                                                      max_coerce_na_pct = max_coerce_na_pct)
    num_cols <- names(df_chunk)[sapply(df_chunk, is.numeric)]
    for (col in num_cols) {
      bad_idx <- which(!is.finite(df_chunk[[col]]) & !is.na(df_chunk[[col]]))
      if (length(bad_idx) > 0L) data.table::set(df_chunk, i = bad_idx, j = col, value = NA_real_)
    }
    validate_partition_column_values(df_chunk, partition_keys, partition_values,
                                     source_label = basename(path))
    for (pk in intersect(partition_keys, names(df_chunk))) df_chunk[, (pk) := NULL]
    arrow_tbl <- arrow::as_arrow_table(df_chunk)
    rm(df_chunk)
    if (is.null(ref_schema)) {
      if (!is.null(col_classes)) {
        ref_schema <<- arrow_schema_from_classes(arrow_tbl, col_classes)
      } else {
        ref_schema <<- arrow_tbl$schema
      }
    }
    arrow_tbl <- arrow_tbl$cast(ref_schema)
    chunk_file <- if (TerminalHivePartition) {
      bd <- file.path(year_dir, sprintf("batch_id=%s_%05d", file_stem, chunk_num))
      dir.create(bd, recursive = TRUE, showWarnings = FALSE)
      file.path(bd, "data.parquet")
    } else {
      file.path(year_dir, sprintf("%s_%05d.parquet", file_stem, chunk_num))
    }
    write_arrow_table_safely(arrow_tbl, chunk_file)
    written_files <<- unique(c(written_files, chunk_file))
    n_written <- arrow_tbl$num_rows
    rm(arrow_tbl)
    update_parquet_manifest(ManifestPath = ManifestPath, Database = Database, TableName = TableName,
                            DuckDBTable = DuckDBTable, Year = year_val, SourcePath = SourcePath %||% path,
                            ParquetPath = chunk_file, NRows = n_written,
                            SchemaHash = SchemaHash, Status = "written",
                            Notes = sprintf("chunk_%05d", chunk_num),
                            PartitionKey = partition_keys, PartitionValue = partition_values)
    total_written <<- total_written + n_written
    log_msg(sprintf("[CHUNKED-DELIM] %s chunk %d: %d rows -> %s", basename(path), chunk_num,
                    n_written, basename(chunk_file)))
    chunk_num <<- chunk_num + 1L
    touch_repository_lock(RepositoryLock)
    gc(verbose = FALSE)
    invisible(NULL)
  }
  tryCatch({
    if (logical_stream) {
      diagnostics <- .stream_delimited_logical_records(
        path, reader_options = reader_options, chunk_size = chunk_size,
        col_classes = col_classes, callback = process_chunk)
      total_rows <- diagnostics$LogicalRows
      if (diagnostics$RepairCount > 0L) {
        log_msg(sprintf("[DELIMITED REPAIR] %s: appended %d continuation line(s) to %s (physical lines: %s)",
                        basename(path), as.integer(diagnostics$RepairCount),
                        diagnostics$ContinuationColumn,
                        paste(diagnostics$RepairLines, collapse = ",")))
      }
    } else {
      while (offset < total_rows) {
        touch_repository_lock(RepositoryLock)
        n_this <- min(as.integer(chunk_size), total_rows - offset)
        #### skip = offset + 1 skips the header line plus the rows already  ####
        #### written; column names come from the header read above.         ####
        df_chunk <- call_reader(rd, "read_chunk", path, reader_options = reader_options,
                                offset = offset, n_max = n_this, header = header,
                                col_classes = col_classes)
        process_chunk(df_chunk)
        offset <- offset + n_this
      }
    }
    if (total_written != total_rows) {
      stop(sprintf("file=%s | issue=row_count_mismatch | counted=%d | rows_written=%d",
                   basename(path), total_rows, total_written))
    }
    list(written = TRUE, n_rows = total_written, status = "completed")
  }, error = function(e) {
    n_cleaned <- cleanup_outputs()
    if (n_cleaned > 0L) log_msg(sprintf("[CHUNKED-DELIM] Removed %d partial output(s) after failure of %s",
                                        n_cleaned, basename(path)))
    stop(e)
  })
}

########################################################
#### Safely read a CSV file with UTF-8 sanitisation ####
########################################################
#' Safely read a CSV file with UTF-8 sanitisation.
#'
#' Wraps \code{data.table::fread()} with \code{tryCatch}.  All character
#' columns are re-encoded to UTF-8 after loading.
#' @param path Character. Full path to the \code{.csv} file.
#' @return A \code{data.table}, or an empty \code{data.frame()} if reading
#'   fails.
#' @seealso \code{\link{safe_read_sav}}
#' @examples
#' \dontrun{
#' set.seed(1)
#' tmp_csv <- tempfile(fileext = ".csv")
#' df <- data.frame(AGE = sample(18:90, 20, replace = TRUE),
#'                  SEX = sample(c("M", "F"), 20, replace = TRUE) )
#' write.csv(df, tmp_csv, row.names = FALSE)
#' df_loaded <- safe_read_csv(tmp_csv)
#' str(df_loaded)
#' unlink(tmp_csv)
#' }
#' @export
safe_read_csv <- function(path) {
  tryCatch({
    read_delimited_full(path, reader_options = list(Encoding = "auto"))
  }, error = function(e) data.frame())
}

################################################################################
#### Column inventory ##########################################################
################################################################################
#' Build the union of column names for each table across all source files
#'
#' Reads only the column headers (zero rows) from every file in parallel and
#' groups the results by table suffix.  The output is a named list where each
#' element is the union of all column names seen across every year-file for
#' that table.  This union is used by \code{\link{align_columns}} to ensure
#' that all year-files for a table share the same column set when row-bound
#' together.
#'
#' Parallelism is scoped within this function (see
#' \code{\link{build_col_classes}} for details).
#' @param files        Character vector of file paths (full paths when
#'   \code{base_path = ""}, relative otherwise).
#' @param base_path    Character scalar. Prepended to each element of
#'   \code{files} when non-empty.  Pass \code{""} when \code{files} already
#'   contains full paths.
#' @param suffixes     Character vector (parallel to \code{files}). Table-name
#'   suffix for each file (e.g. \code{"Core"}, \code{"Severity"}).
#' @param uni_suffixes Character vector. Unique values of \code{suffixes};
#'   used as the names of the returned list.
#' @param reader       Character scalar. One of \code{"sav"} or \code{"csv"}.
#' @return A named list where each element is a character vector of column
#'   names (the union across all files for that table suffix).
#' @seealso \code{\link{build_col_classes}}, \code{\link{align_columns}}
#' @examples
#' \dontrun{
#' set.seed(1)
#' tmp_dir <- tempfile("savfiles_")
#' dir.create(tmp_dir)
#' #### 2019 has an extra RACE column not present in 2018 ####
#' df_2018 <- data.frame(AGE = sample(18:90, 20, replace = TRUE),
#'                       SEX = sample(1:2, 20, replace = TRUE))
#' df_2019 <- data.frame(AGE = sample(18:90, 20, replace = TRUE),
#'                       SEX = sample(1:2, 20, replace = TRUE),
#'                       RACE = sample(1:5, 20, replace = TRUE))
#' haven::write_sav(df_2018, file.path(tmp_dir, "DEMO_2018_Core.sav"))
#' haven::write_sav(df_2019, file.path(tmp_dir, "DEMO_2019_Core.sav"))
#' files <- c("DEMO_2018_Core.sav", "DEMO_2019_Core.sav")
#' comprehensive <- build_comprehensive(files = files,
#'                                      base_path = tmp_dir,
#'                                      suffixes = c("Core", "Core"),
#'                                      uni_suffixes = "Core",
#'                                      reader = "sav" )
#' comprehensive # $Core contains AGE, SEX, RACE: the union across both years
#' unlink(tmp_dir, recursive = TRUE)
#' }
#' @export
build_comprehensive <- function(files, base_path, suffixes, uni_suffixes, reader, n_workers = 1,
                                reader_options = NULL, strict_read = TRUE){
  all_paths <- if(nchar(base_path) == 0){ files } else { file.path(base_path, files) }
  readers <- if (length(reader) == 1L) rep(reader, length(all_paths)) else reader
  if (length(readers) != length(all_paths)) stop("reader must have length 1 or match files.")
  if (is.null(reader_options)) reader_options <- rep(list(list()), length(all_paths))
  if (!is.list(reader_options) || length(reader_options) != length(all_paths)) {
    stop("reader_options must be a list with one element per file.")
  }
  #### Per-file tolerance: a missing/unreadable file must not abort the     ####
  #### whole database's header scan -- its columns stay unknown and the     ####
  #### file fails individually at load time instead.                        ####
  scan_one <- function(i) tryCatch({
    rd <- get_file_reader(readers[i])
    header <- canonical_colnames(call_reader(rd, "read_header", all_paths[i],
                                             reader_options = reader_options[[i]]))
    list(ok = TRUE, path = all_paths[i], header = header, error = NA_character_)
  }, error = function(e) {
    list(ok = FALSE, path = all_paths[i], header = character(0),
         error = conditionMessage(e))
  })
  header_results <- .parallel_scan_with_serial_retry(
    seq_along(all_paths), scan_one, n_workers = n_workers,
    future_packages = c("haven", "data.table"),
    is_failure = function(x) !is.list(x) || !isTRUE(x$ok),
    context = "schema header inference"
  )
  failed <- vapply(header_results, function(x) !isTRUE(x$ok), logical(1))
  if (any(failed)) {
    first_failure <- header_results[[which(failed)[1]]]
    log_msg(sprintf("[SCHEMA ERROR] build_comprehensive: %d file(s) unreadable during header scan (first: %s; reader: %s; cause: %s) -- their columns cannot be resolved safely.",
                    sum(failed), basename(first_failure$path), readers[which(failed)[1]],
                    first_failure$error))
    if (isTRUE(strict_read)) {
      stop(sprintf("Schema header inference failed for %d readable-path source file(s); first: %s; reader: %s; cause: %s",
                   sum(failed), first_failure$path, readers[which(failed)[1]],
                   first_failure$error))
    }
  }
  all_headers <- lapply(header_results, function(x) if (isTRUE(x$ok)) x$header else NULL)
  stats::setNames(lapply(uni_suffixes, function(s) {
    unique(unlist(all_headers[suffixes == s]))
  }), uni_suffixes)
}
################################################################################
#### Checkpoint system #########################################################
################################################################################
#' Load the completed-files checkpoint from disk
#'
#' Reads the RDS file at \code{path} and returns its contents.  If the file
#' does not exist, prints a warning and returns \code{character(0)} so the
#' loader starts fresh.
#' @param path Character. Path to the checkpoint \code{.rds} file.  Defaults
#'   to \code{CheckpointPath} from the calling environment.
#' @return A character vector of relative file paths that have already been
#'   successfully loaded, or \code{character(0)} if no checkpoint exists.
#' @seealso \code{\link{save_checkpoint}}
#' @examples
#' \dontrun{
#' tmp_checkpoint <- tempfile(fileext = ".rds")
#' #### No checkpoint exists yet -- returns character(0) ####
#' load_checkpoint(tmp_checkpoint)
#' #### Simulate two completed files and reload ####
#' saveRDS(c("DEMO_2018_Core.sav", "DEMO_2019_Core.sav"), tmp_checkpoint)
#' load_checkpoint(tmp_checkpoint)
#' unlink(tmp_checkpoint)
#' }
#' @export
load_checkpoint <- function(path){
  previous <- paste0(path, ".previous")
  current <- if (file.exists(path)) tryCatch(readRDS(path), error = function(e) e) else NULL
  if (!is.null(current) && !inherits(current, "error")) return(current)
  if (file.exists(previous)) {
    recovered <- tryCatch(readRDS(previous), error = function(e) e)
    if (!inherits(recovered, "error")) {
      log_msg(sprintf("[CHECKPOINT RECOVERY] Restoring the last verified checkpoint generation: %s", previous))
      file.copy(previous, path, overwrite = TRUE)
      return(recovered)
    }
  }
  if (inherits(current, "error")) stop(sprintf("Checkpoint is unreadable and no valid previous generation exists: %s", path))
  character(0)
}


#' Atomically save the completed-file checkpoint
#'
#' Writes the checkpoint to a temporary file in the same directory and then
#' renames it into place. This greatly reduces the chance of a corrupted
#' checkpoint if R stops during saveRDS() or a network drive briefly disconnects.
#' @param checkpoint Character vector of completed source-file keys/paths.
#' @param path Destination .rds checkpoint path.
#' @return invisible(TRUE) on success.
#' @export
save_checkpoint <- function(checkpoint, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  pending <- paste0(path, ".pending")
  previous <- paste0(path, ".previous")
  if (file.exists(pending)) unlink(pending)
  saveRDS(unique(checkpoint), pending)
  verified <- readRDS(pending)
  if (!identical(unique(checkpoint), verified)) stop("Checkpoint verification failed before replacement.")
  if (file.exists(path)) {
    if (!file.copy(path, previous, overwrite = TRUE, copy.date = TRUE)) {
      stop(sprintf("Could not preserve previous checkpoint generation: %s", previous))
    }
  }
  replace_file_safely(pending, path)
  final <- readRDS(path)
  if (!identical(verified, final)) stop("Checkpoint verification failed after replacement.")
  invisible(TRUE)
}

################################################################################
#### Single-writer repository lock #############################################
################################################################################
#### The atomic-write helpers protect individual files, but nothing stops    ####
#### two loader runs (e.g. two machines against the same network share)      ####
#### from interleaving checkpoint saves. The lock is a directory (mkdir is   ####
#### atomic on every platform and filesystem) containing an owner record;    ####
#### the loader heartbeats it after each completed file so a crashed run's   ####
#### lock goes stale and can be taken over.                                  ####

repository_lock_path_default <- function(ParquetBasePath) {
  file.path(dirname(ParquetBasePath), ".repository.lock")
}

#' Acquire the single-writer repository lock
#'
#' @param LockPath Directory path used as the lock (created atomically).
#' @param stale_minutes Numeric. A lock whose heartbeat is older than this is
#'   considered abandoned (crashed run) and is taken over with a log message.
#'   The loader heartbeats after every completed file, so set this comfortably
#'   above the longest single-file load you expect. Default 720 (12 h).
#' @param owner_note Optional free text recorded in the owner file.
#' @return A \code{repository_lock} object to pass to
#'   \code{\link{release_repository_lock}}. Stops if another live run holds
#'   the lock.
#' @export
acquire_repository_lock <- function(LockPath, stale_minutes = 720, owner_note = "") {
  dir.create(dirname(LockPath), recursive = TRUE, showWarnings = FALSE)
  token <- paste(Sys.info()[["nodename"]], Sys.getpid(),
                 format(Sys.time(), "%Y-%m-%d %H:%M:%S"), owner_note, sep = " | ")
  owner_file <- file.path(LockPath, "owner.txt")
  attempt <- function() {
    if (!dir.create(LockPath, showWarnings = FALSE)) return(FALSE)
    writeLines(token, owner_file)
    TRUE
  }
  if (attempt()) {
    log_msg(sprintf("[LOCK] Acquired repository lock: %s (%s)", LockPath, token))
    return(invisible(structure(list(path = LockPath, token = token), class = "repository_lock")))
  }
  hb <- file.mtime(owner_file)
  if (is.na(hb)) hb <- file.mtime(LockPath)
  age_min <- as.numeric(difftime(Sys.time(), hb, units = "mins"))
  holder <- tryCatch(paste(readLines(owner_file, warn = FALSE), collapse = " "),
                     error = function(e) "<unreadable>")
  if (!is.na(age_min) && age_min > stale_minutes) {
    quarantine <- paste0(LockPath, ".stale_", Sys.getpid(), "_", format(Sys.time(), "%Y%m%d%H%M%S"))
    log_msg(sprintf("[LOCK] Stale lock detected (heartbeat %.0f min old, holder: %s) -- claiming it for takeover.", age_min, holder))
    claimed <- file.rename(LockPath, quarantine)
    if (isTRUE(claimed) && attempt()) {
      unlink(quarantine, recursive = TRUE)
      log_msg(sprintf("[LOCK] Acquired repository lock after stale takeover: %s (%s)", LockPath, token))
      return(invisible(structure(list(path = LockPath, token = token), class = "repository_lock")))
    }
    if (isTRUE(claimed) && dir.exists(quarantine) && !dir.exists(LockPath)) file.rename(quarantine, LockPath)
  }
  stop(sprintf(paste0("Repository is locked by another loader run.\n  Holder: %s\n  Heartbeat age: %s min (stale after %d)\n  Lock: %s\n",
                      "If you are certain no other run is active, remove it with release_repository_lock(\"%s\", force = TRUE)."),
               holder, ifelse(is.na(age_min), "unknown", sprintf("%.0f", age_min)), as.integer(stale_minutes),
               LockPath, LockPath))
}

#' Heartbeat the repository lock so it does not go stale mid-run
#' @export
touch_repository_lock <- function(lock) {
  if (is.null(lock) || !inherits(lock, "repository_lock")) return(invisible(NULL))
  tryCatch(Sys.setFileTime(file.path(lock$path, "owner.txt"), Sys.time()), error = function(e) NULL)
  invisible(NULL)
}

#' Release the single-writer repository lock
#'
#' Pass the object returned by \code{\link{acquire_repository_lock}} (only the
#' owning run's token releases), or a path with \code{force = TRUE} to remove
#' an abandoned lock by hand.
#' @export
release_repository_lock <- function(lock, force = FALSE) {
  path <- if (inherits(lock, "repository_lock")) lock$path else as.character(lock)
  if (!dir.exists(path)) return(invisible(TRUE))
  if (!isTRUE(force)) {
    if (!inherits(lock, "repository_lock")) {
      stop("Pass the repository_lock object returned by acquire_repository_lock(), or use force = TRUE with a path.")
    }
    current <- tryCatch(readLines(file.path(path, "owner.txt"), warn = FALSE)[1], error = function(e) NA_character_)
    if (!identical(current, lock$token)) {
      log_msg(sprintf("[LOCK] Not releasing %s: it is now held by a different run (%s).", path, current))
      return(invisible(FALSE))
    }
  }
  unlink(path, recursive = TRUE)
  log_msg(sprintf("[LOCK] Released repository lock: %s", path))
  invisible(TRUE)
}

################################################################################
#### Repository state snapshots ################################################
################################################################################

#' Snapshot the repository's bookkeeping files before a run
#'
#' Copies the small state files that encode everything the loader knows --
#' checkpoint (.rds), manifest (.csv), schema catalog (plus its Labels sibling
#' when the catalog is CSV), and schema registry -- into a timestamped
#' subfolder of \code{BackupDir}. Together these are a few hundred KB, but
#' they are the difference between \code{\link{audit_repository}} findings
#' being merely detectable and being recoverable. Retention keeps the newest
#' \code{keep_last} snapshots and removes older ones.
#' @param CheckpointPath,ManifestPath,TableSchemaPath,SchemaRegistryPath State
#'   file paths; NULL or missing files are skipped silently.
#' @param BackupDir Character. Snapshot root, e.g.
#'   \code{<FormattedDBPath>/StateBackups}.
#' @param keep_last Integer. Snapshots to retain (default 20).
#' @return Invisibly, the snapshot directory path (or NULL if nothing to copy).
#' @export
snapshot_repository_state <- function(CheckpointPath = NULL, ManifestPath = NULL,
                                      TableSchemaPath = NULL, SchemaRegistryPath = NULL,
                                      BackupDir, keep_last = 20L) {
  candidates <- c(CheckpointPath, ManifestPath, TableSchemaPath, SchemaRegistryPath)
  if (!is.null(TableSchemaPath) && nzchar(TableSchemaPath) && !is_excel_workbook_path(TableSchemaPath)) {
    candidates <- c(candidates, label_catalog_path(TableSchemaPath))
  }
  candidates <- unique(candidates[!vapply(candidates, is.null, logical(1))])
  candidates <- candidates[nzchar(candidates) & file.exists(candidates)]
  if (length(candidates) == 0L) {
    log_msg("[SNAPSHOT] No state files exist yet -- nothing to snapshot.")
    return(invisible(NULL))
  }
  stamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  snap_dir <- file.path(BackupDir, stamp)
  #### A second snapshot within the same second lands in the same folder --  ####
  #### harmless, the copies are identical.                                   ####
  dir.create(snap_dir, recursive = TRUE, showWarnings = FALSE)
  ok <- file.copy(candidates, snap_dir, overwrite = TRUE, copy.date = TRUE)
  log_msg(sprintf("[SNAPSHOT] Saved %d/%d state file(s) to %s", sum(ok), length(candidates), snap_dir))
  #### Retention: newest keep_last snapshot folders survive.                 ####
  snaps <- list.dirs(BackupDir, recursive = FALSE, full.names = TRUE)
  snaps <- snaps[grepl("^[0-9]{8}_[0-9]{6}$", basename(snaps))]
  if (length(snaps) > keep_last) {
    drop <- sort(snaps)[seq_len(length(snaps) - keep_last)]
    unlink(drop, recursive = TRUE)
    log_msg(sprintf("[SNAPSHOT] Retention: removed %d old snapshot(s), keeping %d.", length(drop), as.integer(keep_last)))
  }
  invisible(snap_dir)
}

#' Migrate checkpoint and manifest entries after renaming a table in the MDT
#'
#' Changing a row's \code{TableName} in the workbook (e.g. normalizing
#' \code{NRD_CORE} to \code{NRD_Core} after a case-collision preflight error)
#' changes that row's checkpoint identity, so already-loaded files would be
#' re-ingested. This helper rewrites the affected checkpoint entries (both the
#' generalized and legacy key formats) and the manifest's TableName/DuckDBTable
#' fields in place, so completed files stay completed under the new name.
#' Run it once on the loading machine after editing the workbook; the MDT you
#' pass must already carry the NEW TableName.
#' @param CheckpointPath Character. Checkpoint .rds path.
#' @param MDT Data frame. Workbook rows already using \code{NewTableName}.
#' @param Database,OldTableName,NewTableName Character. The rename to migrate.
#' @param ManifestPath Character (optional). Manifest CSV to rewrite too.
#' @param DryRun Logical. TRUE (default) reports what would change.
#' @return Invisibly, list(n_checkpoint_migrated, n_manifest_migrated).
#' @export
rename_checkpoint_table <- function(CheckpointPath, MDT, Database, OldTableName, NewTableName,
                                    ManifestPath = NULL, DryRun = TRUE,
                                    ParquetBasePath = NULL,
                                    OldPhysicalTableName = NULL,
                                    NewPhysicalTableName = NULL) {
  MDTdt <- data.table::as.data.table(MDT)
  rows <- MDTdt[as.character(MDTdt$Database) == Database & as.character(MDTdt$TableName) == NewTableName, ]
  if (nrow(rows) == 0L) {
    stop(sprintf("No MDT rows found for %s/%s -- update the workbook to the new name before migrating.",
                 Database, NewTableName))
  }
  old_rows <- data.table::copy(rows); old_rows[, TableName := OldTableName]
  old_physical <- OldPhysicalTableName %||% paste(Database, OldTableName, sep = "_")
  new_physical <- NewPhysicalTableName %||% repository_table_name_for_row(rows[1, ])
  if ("PhysicalTableName" %in% names(old_rows)) old_rows[, PhysicalTableName := old_physical]
  old_keys <- c(repository_checkpoint_key(old_rows), repository_checkpoint_legacy_key(old_rows))
  new_keys <- c(repository_checkpoint_key(rows), repository_checkpoint_legacy_key(rows))
  checkpoint <- load_checkpoint(path = CheckpointPath)
  checkpoint_base <- sub("(\\|\\|SOURCE=.*)$", "", checkpoint)
  checkpoint_suffix <- ifelse(grepl("\\|\\|SOURCE=", checkpoint),
                              paste0("||SOURCE=", sub("^.*\\|\\|SOURCE=", "", checkpoint)), "")
  hit <- match(checkpoint_base, old_keys)
  n_ckpt <- sum(!is.na(hit))
  n_manifest <- 0L
  manifest <- NULL
  if (!is.null(ManifestPath) && file.exists(ManifestPath)) {
    manifest <- read_parquet_manifest(ManifestPath)
    if (nrow(manifest) > 0L) {
      m_hit <- manifest$Database == Database & manifest$TableName == OldTableName
      n_manifest <- sum(m_hit)
    }
  }
  log_msg(sprintf("[RENAME]%s %s/%s -> %s: %d checkpoint entrie(s), %d manifest row(s) to migrate.",
                  if (DryRun) " (dry run)" else "", Database, OldTableName, NewTableName, n_ckpt, n_manifest))
  if (!DryRun) {
    if (n_ckpt > 0L) {
      checkpoint[!is.na(hit)] <- paste0(new_keys[hit[!is.na(hit)]], checkpoint_suffix[!is.na(hit)])
      save_checkpoint(unique(checkpoint), CheckpointPath)
    }
    if (!is.null(ParquetBasePath) && !identical(old_physical, new_physical)) {
      old_dir <- file.path(ParquetBasePath, old_physical)
      new_dir <- file.path(ParquetBasePath, new_physical)
      if (dir.exists(old_dir)) {
        if (dir.exists(new_dir) && !identical(normalizePath(old_dir, mustWork = FALSE), normalizePath(new_dir, mustWork = FALSE))) {
          stop(sprintf("Cannot rename Parquet directory because the destination exists: %s", new_dir))
        }
        intermediate <- tempfile(pattern = ".table_rename_", tmpdir = ParquetBasePath)
        if (!file.rename(old_dir, intermediate) || !file.rename(intermediate, new_dir)) {
          if (dir.exists(intermediate) && !dir.exists(old_dir)) file.rename(intermediate, old_dir)
          stop(sprintf("Could not move Parquet directory %s -> %s", old_dir, new_dir))
        }
      }
    }
    if (n_manifest > 0L) {
      m_hit <- manifest$Database == Database & manifest$TableName == OldTableName
      manifest$TableName[m_hit] <- NewTableName
      manifest$DuckDBTable[m_hit] <- new_physical
      if (!is.null(ParquetBasePath) && "ParquetPath" %in% names(manifest)) {
        old_prefix <- normalize_repo_path(file.path(ParquetBasePath, old_physical))
        for (j in which(m_hit)) {
          rel <- normalize_repo_path(manifest$ParquetPath[j])
          if (startsWith(rel, paste0(old_prefix, "/")) || identical(rel, old_prefix)) {
            suffix <- substring(gsub("\\\\", "/", manifest$ParquetPath[j]), nchar(gsub("\\\\", "/", file.path(ParquetBasePath, old_physical))) + 1L)
            manifest$ParquetPath[j] <- paste0(gsub("\\\\", "/", file.path(ParquetBasePath, new_physical)), suffix)
          }
        }
      }
      write_parquet_manifest_atomic(manifest, ManifestPath)
    }
    log_msg(sprintf("[RENAME] Migration complete for %s/%s -> %s.", Database, OldTableName, NewTableName))
  }
  invisible(list(n_checkpoint_migrated = n_ckpt, n_manifest_migrated = n_manifest))
}

#' Reset one table so the loader rewrites it under the current schema registry
#'
#' Parquet files written before the schema registry existed (or under an older
#' registry) keep their original column types forever, because the checkpoint
#' marks their source files complete and the loader never revisits them. This
#' is how type mismatches such as \code{KEY_NIS} being \code{DOUBLE} in one
#' table and \code{VARCHAR} in another survive a registry fix. This function
#' clears everything the loader uses to consider the table done -- its Parquet
#' directory, its checkpoint entries (both key-based and legacy path-based),
#' and its manifest rows -- so the next \code{\link{ParquetBackEndCreate}} run
#' rebuilds it from source with the current registry types.
#' @param MDT Data frame. Master Database Table containing the rows for the
#'   table being reset (used to derive checkpoint keys).
#' @param Database Character. Database prefix (e.g. \code{"NIS"}).
#' @param TableName Character. Table suffix (e.g. \code{"Core"}); together
#'   these identify the DuckDB table \code{<Database>_<TableName>}.
#' @param ParquetBasePath Character. Root directory of the Parquet store.
#' @param CheckpointPath Character. Path to the checkpoint \code{.rds} file.
#' @param ManifestPath Character (optional). Parquet manifest CSV; its rows for
#'   this table are removed when provided.
#' @param DryRun Logical. If \code{TRUE} (default), only reports what would be
#'   deleted. Pass \code{FALSE} to actually delete.
#' @return Invisibly, a list with \code{parquet_dir}, \code{n_checkpoint_removed},
#'   and \code{n_manifest_removed}.
#' @seealso \code{\link{load_schema_registry}}, \code{\link{ParquetBackEndCreate}}
#' @export
reset_table_for_reload <- function(MDT, Database, TableName, ParquetBasePath,
                                   CheckpointPath, ManifestPath = NULL, DryRun = TRUE) {
  rows <- MDT[MDT$Database == Database & MDT$TableName == TableName, , drop = FALSE]
  if (nrow(rows) == 0L) {
    log_msg(sprintf("[RESET] No MDT rows match %s/%s -- nothing to reset.", Database, TableName))
    return(invisible(list(parquet_dir = NA_character_, n_checkpoint_removed = 0L, n_manifest_removed = 0L)))
  }
  table_name <- repository_table_name_for_row(rows[1, , drop = FALSE])
  parquet_dir <- file.path(ParquetBasePath, table_name)
  checkpoint <- load_checkpoint(path = CheckpointPath)
  checkpoint_base <- sub("\\|\\|SOURCE=.*$", "", checkpoint)
  stale <- checkpoint_base %in% c(repository_checkpoint_key(rows), repository_checkpoint_legacy_key(rows), rows$Path)
  manifest <- if (!is.null(ManifestPath) && file.exists(ManifestPath)) read_parquet_manifest(ManifestPath) else NULL
  stale_manifest <- if (!is.null(manifest) && nrow(manifest) > 0L) {
    manifest$Database == Database & manifest$TableName == TableName
  } else { logical(0) }
  log_msg(sprintf("[RESET]%s %s: %s parquet dir %s, %d checkpoint entrie(s), %d manifest row(s)",
                  if (DryRun) " (dry run)" else "", table_name,
                  if (DryRun) "would remove" else "removing",
                  parquet_dir, sum(stale), sum(stale_manifest)))
  if (!DryRun) {
    #### Destructive path: take the single-writer lock so a reset cannot    ####
    #### race a loader run on another machine.                              ####
    reset_lock <- acquire_repository_lock(repository_lock_path_default(ParquetBasePath),
                                          owner_note = sprintf("reset_table_for_reload %s", table_name))
    on.exit(release_repository_lock(reset_lock), add = TRUE)
    if (dir.exists(parquet_dir)) unlink(parquet_dir, recursive = TRUE)
    if (any(stale)) save_checkpoint(checkpoint[!stale], CheckpointPath)
    if (any(stale_manifest)) {
      remove_parquet_manifest_rows(ManifestPath, Database = Database, TableName = TableName)
    }
    log_msg(sprintf("[RESET] %s cleared. Re-run ParquetBackEndCreate with DBLoad = \"%s\" to rebuild it.",
                    table_name, Database))
  }
  invisible(list(parquet_dir = parquet_dir,
                 n_checkpoint_removed = sum(stale),
                 n_manifest_removed = sum(stale_manifest)))
}

################################################################################
#### Repository reconciliation (fsck) ##########################################
################################################################################

#### Normalize paths for cross-source comparison (manifest rows may mix      ####
#### separators; Windows filesystems are case-insensitive).                  ####
normalize_repo_path <- function(x) {
  out <- gsub("\\\\", "/", as.character(x))
  if (.Platform$OS.type == "windows") tolower(out) else out
}

#' Reconcile the four sources of truth: checkpoint, manifest, disk, DuckDB
#'
#' Non-destructive fsck for the Parquet repository. Cross-checks:
#' \itemize{
#'   \item \code{stale_checkpoint}: checkpoint entries matching no current MDT
#'     row (workbook rows renamed/removed after loading).
#'   \item \code{checkpointed_no_output}: MDT rows the checkpoint marks
#'     complete whose partition directory holds no Parquet -- unless the
#'     manifest records the file as verified \code{empty}.
#'   \item \code{manifest_missing_file}: manifest \code{written} rows whose
#'     Parquet file has vanished from disk (crash, manual deletion, sync loss).
#'   \item \code{orphan_parquet}: Parquet files on disk no manifest row claims
#'     (aborted runs from before manifest tracking, stray copies).
#'   \item \code{duckdb_count_mismatch}: per table, \code{COUNT(*)} over the
#'     hive directory vs the sum of manifest \code{written} row counts.
#'     Requires \code{con}; reads the Parquet directly so it does not depend
#'     on views being registered.
#' }
#' Also reports verified-\code{empty} and \code{partial_accepted} files as
#' informational context.
#' @param MDT Data frame. Master Database Table (current workbook).
#' @param ParquetBasePath Character. Root of the Parquet store.
#' @param CheckpointPath Character. Checkpoint .rds path.
#' @param ManifestPath Character (optional). Manifest CSV; several checks are
#'   skipped without it.
#' @param con Optional live DuckDB connection for count reconciliation.
#' @param verbose Logical. Log a per-check summary via \code{log_msg}.
#' @return Invisibly, a list: \code{issues} (summary data.table with Check,
#'   Severity, N) plus one detail data.table per check.
#' @export
audit_repository <- function(MDT, ParquetBasePath, CheckpointPath, ManifestPath = NULL,
                             con = NULL, verbose = TRUE, LogPath = NULL, RunId = NULL) {
  previous_run <- if (!is.null(LogPath) || !is.null(RunId)) begin_repository_run(LogPath, RunId) else NULL
  if (!is.null(previous_run)) on.exit(restore_repository_run(previous_run), add = TRUE)
  MDTdt <- data.table::as.data.table(MDT)
  issues <- data.table::data.table(Check = character(), Severity = character(), N = integer())
  add_issue <- function(check, severity, n) {
    issues <<- data.table::rbindlist(list(issues, data.table::data.table(Check = check, Severity = severity, N = as.integer(n))), fill = TRUE)
  }
  checkpoint <- load_checkpoint(path = CheckpointPath)
  mdt_keys <- repository_checkpoint_key(MDTdt)
  legacy_mdt_keys <- repository_checkpoint_legacy_key(MDTdt)
  manifest <- if (!is.null(ManifestPath) && file.exists(ManifestPath)) read_parquet_manifest(ManifestPath) else NULL

  #### 1. checkpoint entries no current MDT row explains ####
  legacy_paths <- MDTdt$Path[!duplicated(MDTdt$Path)]
  checkpoint_base <- sub("\\|\\|SOURCE=.*$", "", checkpoint)
  stale_mask <- !checkpoint_base %in% c(mdt_keys, legacy_mdt_keys, legacy_paths)
  stale_checkpoint <- data.table::data.table(CheckpointEntry = checkpoint[stale_mask])
  if (nrow(stale_checkpoint) > 0L) add_issue("stale_checkpoint", "warning", nrow(stale_checkpoint))

  #### 2. checkpointed rows with no on-disk output (and not verified empty) ####
  completed_mask <- checkpoint_completed_mask(MDTdt, checkpoint)
  no_output <- list()
  for (i in which(completed_mask)) {
    row_i <- MDTdt[i, ]
    pdir <- file.path(ParquetBasePath, repository_table_name_for_row(row_i),
                      partition_spec_for_row(row_i)$dir)
    source_manifest <- if (!is.null(manifest) && nrow(manifest) > 0L) {
      manifest[as.character(Database) == as.character(row_i$Database[1]) &
                 as.character(TableName) == as.character(row_i$TableName[1]) &
                 as.character(SourcePath) == as.character(row_i$Path[1])]
    } else { data.table::data.table() }
    is_recorded_empty <- nrow(source_manifest[Status == "empty"]) > 0L
    file_rows <- source_manifest[Status == "written" & grepl("\\.parquet$", ParquetPath, ignore.case = TRUE)]
    has_parquet <- if (nrow(file_rows) > 0L) {
      all(file.exists(file_rows$ParquetPath))
    } else {
      stem <- parquet_output_stem(row_i$Path[1])
      chunk_stem <- parquet_chunk_stem(row_i$Path[1])
      candidates <- if (dir.exists(pdir)) list.files(pdir, pattern = "\\.parquet$", recursive = TRUE,
                                                     full.names = TRUE, ignore.case = TRUE) else character(0)
      any(startsWith(tolower(basename(candidates)), tolower(stem)) |
            startsWith(tolower(basename(candidates)), tolower(chunk_stem)))
    }
    if (!has_parquet && !is_recorded_empty) {
      no_output[[length(no_output) + 1L]] <- data.table::data.table(
        Database = row_i$Database[1], TableName = row_i$TableName[1],
        Path = row_i$Path[1], PartitionDir = pdir)
    }
  }
  checkpointed_no_output <- if (length(no_output) > 0L) data.table::rbindlist(no_output) else
    data.table::data.table(Database = character(), TableName = character(), Path = character(), PartitionDir = character())
  if (nrow(checkpointed_no_output) > 0L) add_issue("checkpointed_no_output", "error", nrow(checkpointed_no_output))

  #### 3 + 4. manifest vs disk, both directions ####
  manifest_missing_file <- data.table::data.table(SourcePath = character(), ParquetPath = character(), Status = character())
  orphan_parquet <- data.table::data.table(ParquetPath = character())
  disk_files <- list.files(ParquetBasePath, pattern = "\\.parquet$", recursive = TRUE,
                           full.names = TRUE, ignore.case = TRUE)
  if (!is.null(manifest) && nrow(manifest) > 0L) {
    written <- manifest[manifest$Status %in% c("written"), ]
    if (nrow(written) > 0L) {
      gone <- !file.exists(written$ParquetPath)
      if (any(gone)) {
        manifest_missing_file <- written[gone, c("SourcePath", "ParquetPath", "Status"), with = FALSE]
        add_issue("manifest_missing_file", "error", sum(gone))
      }
    }
    #### Every real output file gets its own file-level "written" manifest   ####
    #### row (single-file and per-chunk alike). Dir-level rows (completed/   ####
    #### empty/partial_accepted) must NOT blanket-claim their directory --   ####
    #### that would hide genuine orphans sitting inside partition dirs.      ####
    claimed_files <- normalize_repo_path(manifest$ParquetPath[grepl("\\.parquet$", manifest$ParquetPath, ignore.case = TRUE)])
    disk_norm <- normalize_repo_path(disk_files)
    orphan_mask <- !(disk_norm %in% claimed_files)
    if (any(orphan_mask)) {
      orphan_parquet <- data.table::data.table(ParquetPath = disk_files[orphan_mask])
      add_issue("orphan_parquet", "warning", sum(orphan_mask))
    }
  }

  #### 5. DuckDB counts vs manifest sums, per table ####
  duckdb_count_mismatch <- data.table::data.table(DuckDBTable = character(), DiskRows = numeric(), ManifestRows = numeric())
  if (!is.null(con) && !is.null(manifest) && nrow(manifest) > 0L) {
    for (tb in sort(unique(manifest$DuckDBTable))) {
      tdir <- gsub("\\\\", "/", file.path(ParquetBasePath, tb))
      if (!dir.exists(tdir) ||
          length(list.files(tdir, pattern = "\\.parquet$", recursive = TRUE, ignore.case = TRUE)) == 0L) next
      disk_n <- tryCatch(DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM read_parquet({quote_duckdb_string(paste0(tdir, '/**/*.parquet'))}, hive_partitioning = true, union_by_name = true)"))$n,
                         error = function(e) NA_real_)
      man_n <- sum(manifest[manifest$DuckDBTable == tb & manifest$Status == "written", ]$NRows, na.rm = TRUE)
      if (!is.na(disk_n) && disk_n != man_n) {
        duckdb_count_mismatch <- data.table::rbindlist(list(duckdb_count_mismatch,
          data.table::data.table(DuckDBTable = tb, DiskRows = as.numeric(disk_n), ManifestRows = as.numeric(man_n))))
      }
    }
    if (nrow(duckdb_count_mismatch) > 0L) add_issue("duckdb_count_mismatch", "error", nrow(duckdb_count_mismatch))
  }

  #### informational context ####
  info_empty <- if (!is.null(manifest)) manifest[manifest$Status == "empty", ] else data.table::data.table()
  info_partial <- if (!is.null(manifest)) manifest[manifest$Status == "partial_accepted", ] else data.table::data.table()
  if (isTRUE(verbose)) {
    log_msg(sprintf("[AUDIT] Repository reconciliation: %d issue type(s) found%s",
                    nrow(issues), if (nrow(issues) == 0L) " -- checkpoint, manifest, disk and counts agree." else ":"))
    if (nrow(issues) > 0L) for (i in seq_len(nrow(issues))) {
      log_msg(sprintf("[AUDIT %s] %s: %d item(s)", toupper(issues$Severity[i]), issues$Check[i], issues$N[i]))
    }
    if (nrow(info_empty) > 0L) log_msg(sprintf("[AUDIT INFO] %d verified-empty file(s) on record.", nrow(info_empty)))
    if (nrow(info_partial) > 0L) log_msg(sprintf("[AUDIT INFO] %d partial_accepted file(s) on record.", nrow(info_partial)))
    if (is.null(manifest)) log_msg("[AUDIT INFO] No manifest found -- manifest/disk/count checks skipped.")
    if (is.null(con)) log_msg("[AUDIT INFO] No DuckDB connection supplied -- count reconciliation skipped.")
  }
  invisible(list(issues = issues,
                 stale_checkpoint = stale_checkpoint,
                 checkpointed_no_output = checkpointed_no_output,
                 manifest_missing_file = manifest_missing_file,
                 orphan_parquet = orphan_parquet,
                 duckdb_count_mismatch = duckdb_count_mismatch,
                 empty_files = info_empty,
                 partial_accepted_files = info_partial))
}

################################################################################
#### Schema registry ############################################################
################################################################################
#' Build the default repository schema registry
#'
#' The registry is intentionally pattern-based and focused on merge keys,
#' diagnosis/procedure/code columns, survey weights, and common analytic fields.
#' It should remain small; ordinary table-specific columns are still inferred
#' from the source files. The generic profile is an empty template with no
#' naming assumptions. Domain conventions are available only through explicit
#' profiles such as code{"hcup"} or user-authored rows.
#' @return data.table with ColumnPattern, CanonicalType, Role, AppliesTo, Notes.
#' @export
build_default_schema_registry <- function(profile = c("generic", "hcup")) {
  profile <- match.arg(profile)
  #### Generic repositories must not inherit naming assumptions: ID, KEY,  ####
  #### CODE, and WEIGHT can legitimately mean different things by domain.  ####
  #### The empty template remains user-extensible; HCUP conventions are an ####
  #### explicit opt-in profile.                                             ####
  if (profile == "generic") {
    return(data.table::data.table(
      Profile = character(), ColumnPattern = character(),
      CanonicalType = character(), Role = character(),
      AppliesTo = character(), Notes = character()))
  }
  data.table::data.table(
    Profile = "hcup",
    ColumnPattern = c(
      "^KEY$", "^KEY_[A-Z0-9]+$", "^HOSP_NIS$", "^HOSPID$", "^HOSP_[A-Z0-9]+$",
      "^VISITLINK$", "^NRD_VISITLINK$", "^DIED$", "^AGE$", "^LOS$",
      "^DISCWT$", "^TRENDWT$", "^HOSPWT$", "^PAY[0-9]$", "^PAY[0-9]_X$",
      "^DX[0-9]+$", "^I10_DX[0-9]+$", "^PR[0-9]+$", "^I10_PR[0-9]+$",
      "^ECODE[0-9]+$", "^CPT$", "^CPT[0-9]*$", "^ICD.*CODE$", "^.*_CODE$"
    ),
    CanonicalType = c(
      "character", "character", "character", "character", "character",
      "character", "character", "integer", "numeric", "numeric",
      "numeric", "numeric", "numeric", "integer", "character",
      "character", "character", "character", "character",
      "character", "character", "character", "character", "character"
    ),
    Role = c(
      "join_key", "join_key", "join_key", "join_key", "join_key",
      "join_key", "join_key", "analytic", "analytic", "analytic",
      "weight", "weight", "weight", "categorical", "code",
      "code", "code", "code", "code",
      "code", "code", "code", "code", "code"
    ),
    AppliesTo = "all",
    Notes = c(
      "Discharge/stay identifier; preserve exact formatting.",
      "Database-specific key identifier; preserve exact formatting.",
      "NIS hospital identifier; preserve exact formatting.",
      "Hospital identifier; preserve exact formatting.",
      "Database-specific hospital identifier; preserve exact formatting.",
      "NRD visit linkage identifier; preserve exact formatting.",
      "NRD visit linkage identifier; preserve exact formatting.",
      "Death indicator.",
      "Age in years; store as numeric across sources.",
      "Length of stay is analytic; numeric supports aggregation.",
      "Survey/discharge weight.",
      "Trend weight.",
      "Hospital weight.",
      "Primary payer category.",
      "Payer/code text field; preserve leading zeros and symbols.",
      "Diagnosis code; preserve leading zeros/decimals/alphanumeric values.",
      "ICD-10 diagnosis code; preserve alphanumeric values.",
      "Procedure code; preserve leading zeros/decimals/alphanumeric values.",
      "ICD-10 procedure code; preserve alphanumeric values.",
      "External cause code.",
      "CPT code; preserve leading zeros.",
      "CPT code; preserve leading zeros.",
      "Generic ICD/code field.",
      "Generic code field."
    )
  )
}

#' Apply non-negotiable built-in schema policies
#' @keywords internal
apply_builtin_schema_registry_policies <- function(reg, profile = c("generic", "hcup")) {
  profile <- match.arg(profile)
  reg <- data.table::as.data.table(reg)
  if ("Profile" %in% names(reg)) {
    declared <- unique(tolower(trimws(as.character(reg$Profile))))
    declared <- declared[!is.na(declared) & nzchar(declared)]
    if (length(declared) == 1L && declared %in% c("hcup", "generic")) profile <- declared
  }
  if (!"AppliesTo" %in% names(reg)) reg[, AppliesTo := "all"]
  if (!"Role" %in% names(reg)) reg[, Role := NA_character_]
  if (!"Notes" %in% names(reg)) reg[, Notes := NA_character_]
  if (profile == "generic") return(reg)
  idx <- which(trimws(as.character(reg$ColumnPattern)) == "^AGE$")
  if (length(idx) == 0L) {
    reg <- data.table::rbindlist(list(reg, data.table::data.table(
      Profile = "hcup",
      ColumnPattern = "^AGE$",
      CanonicalType = "numeric",
      Role = "analytic",
      AppliesTo = "all",
      Notes = "Age in years; store as numeric across sources."
    )), fill = TRUE)
  } else {
    reg[idx, `:=`(
      CanonicalType = "numeric",
      Role = ifelse(is.na(Role) | !nzchar(Role), "analytic", Role),
      Notes = ifelse(is.na(Notes) | !nzchar(Notes), "Age in years; store as numeric across sources.", Notes)
    )]
  }
  reg
}

#' Detect Excel workbook paths used by schema outputs
#' @keywords internal
is_excel_workbook_path <- function(path) {
  tolower(tools::file_ext(path)) %in% c("xlsx", "xlsm", "xls")
}

#' Write schema registry as an Excel workbook or CSV fallback
#' @keywords internal
write_schema_registry <- function(reg, SchemaRegistryPath) {
  if (is.null(SchemaRegistryPath) || !nzchar(SchemaRegistryPath)) return(invisible(NULL))
  dir.create(dirname(SchemaRegistryPath), recursive = TRUE, showWarnings = FALSE)
  reg <- data.table::as.data.table(reg)
  if (is_excel_workbook_path(SchemaRegistryPath)) {
    write_xlsx_safely(list(SchemaRegistry = reg), SchemaRegistryPath)
  } else {
    write_csv_safely(reg, SchemaRegistryPath)
  }
  invisible(SchemaRegistryPath)
}

#' Read or create a schema registry workbook/csv
#' @export
load_schema_registry <- function(SchemaRegistryPath = NULL, create_if_missing = TRUE,
                                 profile = c("generic", "hcup")) {
  profile <- match.arg(profile)
  if (is.null(SchemaRegistryPath) || !nzchar(SchemaRegistryPath)) {
    return(apply_builtin_schema_registry_policies(build_default_schema_registry(profile), profile = profile))
  }
  if (file.exists(SchemaRegistryPath)) {
    reg <- if (is_excel_workbook_path(SchemaRegistryPath)) {
      openxlsx::read.xlsx(SchemaRegistryPath, sheet = 1)
    } else {
      data.table::fread(SchemaRegistryPath)
    }
    reg <- data.table::as.data.table(reg)
  } else {
    reg <- build_default_schema_registry(profile)
    if (create_if_missing) {
      write_schema_registry(reg, SchemaRegistryPath)
      log_msg(sprintf("[SCHEMA REGISTRY] Created default '%s' registry: %s", profile, SchemaRegistryPath))
    }
  }
  req <- c("ColumnPattern", "CanonicalType")
  missing_req <- setdiff(req, names(reg))
  if (length(missing_req) > 0L) stop(sprintf("Schema registry missing required columns: %s", paste(missing_req, collapse = ", ")))
  reg[, ColumnPattern := trimws(as.character(ColumnPattern))]
  reg[, CanonicalType := vapply(CanonicalType, normalize_type_name, character(1))]
  if (!"AppliesTo" %in% names(reg)) reg[, AppliesTo := "all"]
  if (!"Profile" %in% names(reg)) reg[, Profile := profile]
  allowed <- allowed_canonical_types()
  bad_type <- which(!is_allowed_canonical_type(reg$CanonicalType))
  if (length(bad_type) > 0L) {
    details <- paste(sprintf("row %d: %s", bad_type, reg$CanonicalType[bad_type]), collapse = "; ")
    stop(sprintf("Schema registry contains invalid CanonicalType value(s): %s. Valid types: %s",
                 details, paste(allowed, collapse = ", ")))
  }
  reg <- apply_builtin_schema_registry_policies(reg, profile = profile)
  bad_regex <- which(vapply(reg$ColumnPattern, function(pattern) {
    inherits(tryCatch(regexpr(pattern, "repoquet_REGEX_TEST", perl = TRUE),
                      error = function(e) e), "error")
  }, logical(1)))
  if (length(bad_regex) > 0L) {
    details <- paste(sprintf("row %d: %s", bad_regex, reg$ColumnPattern[bad_regex]), collapse = "; ")
    stop(sprintf("Schema registry contains invalid ColumnPattern regex(es): %s", details))
  }
  reg
}

schema_registry_applies <- function(applies_to, database = NULL, table_name = NULL) {
  applies_to <- trimws(as.character(applies_to %||% "all"))
  if (length(applies_to) == 0L || is.na(applies_to[1]) || !nzchar(applies_to[1]) || identical(tolower(applies_to[1]), "all")) {
    return(TRUE)
  }
  db <- as.character(database %||% "")
  tbl <- as.character(table_name %||% "")
  tokens <- trimws(unlist(strsplit(applies_to[1], "[,;]")))
  any(tolower(tokens) %in% tolower(c(db, tbl, paste(db, tbl, sep = "_"), paste(db, tbl, sep = "/"))))
}

#' Apply repository-level schema registry overrides to an inferred type map
#' @export
apply_schema_registry <- function(col_classes, schema_registry = NULL, database = NULL, table_name = NULL) {
  if (is.null(col_classes)) return(col_classes)
  names(col_classes) <- canonical_colnames(names(col_classes))
  col_classes <- lapply(col_classes, normalize_type_name)
  if (is.null(schema_registry) || nrow(schema_registry) == 0L) return(col_classes)
  applies <- if ("AppliesTo" %in% names(schema_registry)) as.character(schema_registry$AppliesTo) else rep("all", nrow(schema_registry))
  for (col in names(col_classes)) {
    hit <- rep(FALSE, nrow(schema_registry))
    for (i in seq_len(nrow(schema_registry))) {
      hit[i] <- schema_registry_applies(applies[i], database = database, table_name = table_name) &&
        grepl(schema_registry$ColumnPattern[i], col, perl = TRUE, ignore.case = TRUE)
    }
    if (any(hit)) {
      col_classes[[col]] <- normalize_type_name(schema_registry$CanonicalType[which(hit)[1]])
    }
  }
  col_classes
}

#' Build table-specific column-class maps and apply the schema registry
#' @export
build_col_classes_by_table <- function(files, base_path, suffixes, n_workers = 1,
                                       reader = c("sav", "csv"), schema_registry = NULL,
                                       database = NULL) {
  suffixes <- as.character(suffixes)
  out <- list()
  for (suffix in unique(suffixes)) {
    idx <- which(suffixes == suffix)
    class_map <- build_col_classes(files = files[idx], base_path = base_path,
                                   n_workers = n_workers, reader = reader)
    names(class_map) <- canonical_colnames(names(class_map))
    class_map <- apply_schema_registry(class_map, schema_registry = schema_registry,
                                       database = database, table_name = suffix)
    out[[suffix]] <- class_map
  }
  out
}

################################################################################
#### Manifest and validation ####################################################
################################################################################
manifest_path_default <- function(ParquetBasePath) file.path(dirname(ParquetBasePath), "Manifest", "RepositoryMetadata.duckdb")

manifest_is_duckdb <- function(path) {
  !is.null(path) && nzchar(path) && tolower(tools::file_ext(path)) %in% c("duckdb", "ddb")
}

manifest_table_name <- function() "parquet_manifest"

#' Read the Parquet manifest from CSV or its transactional DuckDB store
#' @export
read_parquet_manifest <- function(ManifestPath) {
  if (is.null(ManifestPath) || !nzchar(ManifestPath) || !file.exists(ManifestPath)) {
    return(data.table::data.table())
  }
  if (!manifest_is_duckdb(ManifestPath)) return(data.table::fread(ManifestPath))
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ManifestPath, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  if (!manifest_table_name() %in% DBI::dbListTables(con)) return(data.table::data.table())
  data.table::as.data.table(DBI::dbReadTable(con, manifest_table_name()))
}

manifest_transaction <- function(ManifestPath, code) {
  dir.create(dirname(ManifestPath), recursive = TRUE, showWarnings = FALSE)
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ManifestPath, read_only = FALSE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbBegin(con)
  committed <- FALSE
  on.exit(if (!committed) try(DBI::dbRollback(con), silent = TRUE), add = TRUE)
  value <- force(code(con))
  DBI::dbCommit(con)
  committed <- TRUE
  value
}

manifest_add_missing_columns <- function(con, row) {
  table <- manifest_table_name()
  if (!table %in% DBI::dbListTables(con)) {
    DBI::dbWriteTable(con, table, row[0], overwrite = TRUE)
  }
  existing <- DBI::dbListFields(con, table)
  missing <- setdiff(names(row), existing)
  sql_type <- function(x) {
    if (inherits(x, "POSIXt")) return("TIMESTAMP")
    if (is.logical(x)) return("BOOLEAN")
    if (is.numeric(x)) return("DOUBLE")
    "VARCHAR"
  }
  for (nm in missing) {
    DBI::dbExecute(con, sprintf("ALTER TABLE %s ADD COLUMN %s %s",
                                quote_duckdb_ident(table), quote_duckdb_ident(nm), sql_type(row[[nm]])))
  }
  invisible(NULL)
}

schema_hash_from_classes <- function(col_classes) {
  if (is.null(col_classes) || length(col_classes) == 0L) return(NA_character_)
  nm <- sort(names(col_classes))
  txt <- paste(paste(nm, unlist(col_classes[nm], use.names = FALSE), sep = ":"), collapse = "|")
  if (requireNamespace("digest", quietly = TRUE)) {
    digest::digest(txt, algo = "sha256", serialize = FALSE)
  } else {
    as.character(abs(sum(utf8ToInt(txt) * seq_along(utf8ToInt(txt))) %% .Machine$integer.max))
  }
}

#' Write a manifest table through a temporary file replacement
#' @keywords internal
write_parquet_manifest_atomic <- function(manifest, ManifestPath) {
  if (is.null(ManifestPath) || !nzchar(ManifestPath)) return(invisible(NULL))
  if (manifest_is_duckdb(ManifestPath)) {
    manifest <- data.table::as.data.table(manifest)
    manifest_transaction(ManifestPath, function(con) {
      DBI::dbWriteTable(con, manifest_table_name(), manifest, overwrite = TRUE)
      invisible(ManifestPath)
    })
    return(invisible(ManifestPath))
  }
  dir.create(dirname(ManifestPath), recursive = TRUE, showWarnings = FALSE)
  tmp <- tempfile(pattern = paste0(basename(ManifestPath), ".tmp_"), tmpdir = dirname(ManifestPath))
  data.table::fwrite(data.table::as.data.table(manifest), tmp)
  replace_file_safely(tmp, ManifestPath)
  invisible(ManifestPath)
}

#' Remove manifest rows matching one or more fields
#' @keywords internal
remove_parquet_manifest_rows <- function(ManifestPath, Database = NULL, TableName = NULL,
                                         SourcePath = NULL, ParquetPath = NULL) {
  if (is.null(ManifestPath) || !nzchar(ManifestPath) || !file.exists(ManifestPath)) return(invisible(0L))
  old <- read_parquet_manifest(ManifestPath)
  if (nrow(old) == 0L) return(invisible(0L))
  has_filter <- length(c(Database, TableName, SourcePath, ParquetPath)) > 0L
  if (!has_filter) stop("remove_parquet_manifest_rows() requires at least one filter.")
  drop <- rep(TRUE, nrow(old))
  if (!is.null(Database)) {
    if ("Database" %in% names(old)) drop <- drop & old$Database %in% Database else drop <- rep(FALSE, nrow(old))
  }
  if (!is.null(TableName)) {
    if ("TableName" %in% names(old)) drop <- drop & old$TableName %in% TableName else drop <- rep(FALSE, nrow(old))
  }
  if (!is.null(SourcePath)) {
    if ("SourcePath" %in% names(old)) drop <- drop & old$SourcePath %in% SourcePath else drop <- rep(FALSE, nrow(old))
  }
  if (!is.null(ParquetPath)) {
    if ("ParquetPath" %in% names(old)) drop <- drop & old$ParquetPath %in% ParquetPath else drop <- rep(FALSE, nrow(old))
  }
  n_removed <- sum(drop, na.rm = TRUE)
  if (n_removed > 0L) write_parquet_manifest_atomic(old[!drop], ManifestPath)
  invisible(n_removed)
}

#' Append or replace one row in the Parquet manifest
#' @export
update_parquet_manifest <- function(ManifestPath, Database, TableName, DuckDBTable, Year,
                                    SourcePath, ParquetPath, NRows = NA_real_,
                                    SchemaHash = NA_character_, Status = "written",
                                    Notes = NA_character_,
                                    PartitionKey = NA_character_, PartitionValue = NA_character_,
                                    RunId = NULL, RepositoryKey = NA_character_,
                                    SourceSize = NA_real_, SourceMTimeUTC = NA_character_,
                                    SourceSHA256 = NA_character_, SourceFingerprint = NA_character_) {
  if (is.null(ManifestPath) || !nzchar(ManifestPath)) return(invisible(NULL))
  dir.create(dirname(ManifestPath), recursive = TRUE, showWarnings = FALSE)
  manifest_year <- suppressWarnings(as.integer(Year[1]))
  if (length(manifest_year) == 0L || is.na(manifest_year)) manifest_year <- NA_integer_
  #### General partition provenance: ";"-joined key names and (sanitized)   ####
  #### values, so site and nested partitions are first-class rather than    ####
  #### parsed back out of ParquetPath. Year stays as a derived convenience. ####
  pk_chr <- if (is.null(PartitionKey) || all(is.na(PartitionKey))) NA_character_ else paste(as.character(PartitionKey), collapse = ";")
  pv_chr <- if (is.null(PartitionValue) || all(is.na(PartitionValue))) NA_character_ else paste(as.character(PartitionValue), collapse = ";")
  row <- data.table::data.table(
    run_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    RunId = resolve_run_id(RunId),
    ManifestSchemaVersion = 2,
    Database = as.character(Database),
    TableName = as.character(TableName),
    DuckDBTable = as.character(DuckDBTable),
    Year = manifest_year,
    PartitionKey = pk_chr,
    PartitionValue = pv_chr,
    RepositoryKey = as.character(RepositoryKey),
    SourcePath = as.character(SourcePath),
    SourceSize = as.numeric(SourceSize),
    SourceMTimeUTC = as.character(SourceMTimeUTC),
    SourceSHA256 = as.character(SourceSHA256),
    SourceFingerprint = as.character(SourceFingerprint),
    ParquetPath = as.character(ParquetPath),
    NRows = suppressWarnings(as.numeric(NRows)),
    SchemaHash = as.character(SchemaHash),
    Status = as.character(Status),
    Notes = as.character(Notes)
  )
  if (manifest_is_duckdb(ManifestPath)) {
    manifest_transaction(ManifestPath, function(con) {
      manifest_add_missing_columns(con, row)
      table <- quote_duckdb_ident(manifest_table_name())
      slot <- if (!is.na(row$PartitionValue[1]) && nzchar(row$PartitionValue[1])) {
        row$PartitionValue[1]
      } else {
        as.character(row$Year[1])
      }
      DBI::dbExecute(con, paste0(
        "DELETE FROM ", table,
        " WHERE COALESCE(CAST(Database AS VARCHAR), '') = ?",
        " AND COALESCE(CAST(TableName AS VARCHAR), '') = ?",
        " AND COALESCE(NULLIF(CAST(PartitionValue AS VARCHAR), ''), CAST(Year AS VARCHAR), '') = ?",
        " AND COALESCE(CAST(SourcePath AS VARCHAR), '') = ?",
        " AND COALESCE(CAST(ParquetPath AS VARCHAR), '') = ?"),
        params = list(as.character(row$Database[1]), as.character(row$TableName[1]),
                      ifelse(is.na(slot), "", slot), as.character(row$SourcePath[1]),
                      as.character(row$ParquetPath[1])))
      existing <- DBI::dbListFields(con, manifest_table_name())
      append_row <- data.frame(row)[, existing, drop = FALSE]
      DBI::dbAppendTable(con, manifest_table_name(), append_row)
      invisible(row)
    })
    return(invisible(row))
  }
  old <- if (file.exists(ManifestPath)) read_parquet_manifest(ManifestPath) else data.table::data.table()
  if (nrow(old) > 0L) {
    #### Dedupe slot: PartitionValue when recorded, falling back to Year so ####
    #### rows written by older manifests (no PartitionValue column) still   ####
    #### dedupe correctly.                                                  ####
    slot_of <- function(dt) {
      pv <- if ("PartitionValue" %in% names(dt)) as.character(dt$PartitionValue) else rep(NA_character_, nrow(dt))
      ifelse(!is.na(pv) & nzchar(pv), pv, as.character(dt$Year))
    }
    key <- paste(old$Database, old$TableName, slot_of(old), old$SourcePath, old$ParquetPath, sep = "||")
    row_key <- paste(row$Database, row$TableName, slot_of(row), row$SourcePath, row$ParquetPath, sep = "||")
    old <- old[key != row_key]
  }
  write_parquet_manifest_atomic(data.table::rbindlist(list(old, row), fill = TRUE), ManifestPath)
  invisible(row)
}

quote_duckdb_ident <- function(x) paste0('"', gsub('"', '""', x), '"')
quote_duckdb_string <- function(x) paste0("'", gsub("'", "''", x), "'")

duckdb_sql_type_for_canonical <- function(type) {
  type <- normalize_type_name(type)
  if (grepl("^decimal\\([0-9]+,[0-9]+\\)$", type)) return(toupper(type))
  switch(type, character = "VARCHAR", integer = "INTEGER", int64 = "BIGINT",
         numeric = "DOUBLE", logical = "BOOLEAN", Date = "DATE",
         POSIXct = "TIMESTAMP", time = "TIME", duration = "INTERVAL",
         binary = "BLOB", list = "VARCHAR", "VARCHAR")
}

hive_partition_types <- function(table_name, table_schema = NULL, parquet_dir = NULL) {
  out <- character(0)
  if (!is.null(table_schema) && nrow(table_schema) > 0L) {
    ts <- data.table::as.data.table(table_schema)
    if ("DuckDBTable" %in% names(ts)) ts <- ts[tolower(as.character(DuckDBTable)) == tolower(table_name)]
    if (all(c("Role", "Column", "CanonicalType") %in% names(ts))) {
      ts <- ts[!is.na(Role) & tolower(as.character(Role)) == "partition"]
      if (nrow(ts) > 0L) {
        out <- vapply(ts$CanonicalType, duckdb_sql_type_for_canonical, character(1))
        names(out) <- tolower(as.character(ts$Column))
      }
    }
  }
  if (length(out) == 0L && !is.null(parquet_dir) && dir.exists(parquet_dir)) {
    dirs <- list.dirs(parquet_dir, recursive = TRUE, full.names = FALSE)
    parts <- unique(tolower(sub("=.*$", "", basename(dirs[grepl("=", basename(dirs), fixed = TRUE)]))))
    parts <- setdiff(parts, "batch_id")
    if (length(parts) > 0L) {
      out <- ifelse(parts == "year", "INTEGER", "VARCHAR")
      names(out) <- parts
    }
  }
  out
}

duckdb_type_pattern <- function(expected, compatible_numeric = FALSE) {
  expected <- normalize_type_name(expected)
  if (grepl("^decimal\\(", expected)) return("DECIMAL|NUMERIC")
  integer_types <- "TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT"
  switch(expected,
         "character" = "VARCHAR|CHAR|TEXT|STRING",
         "integer" = integer_types,
         "int64" = "BIGINT|HUGEINT|UBIGINT",
         "numeric" = if (isTRUE(compatible_numeric)) {
           paste("DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC", integer_types, sep = "|")
         } else {
           "DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC"
         },
         "logical" = "BOOLEAN",
         "Date" = "DATE",
         "POSIXct" = "TIMESTAMP",
         "time" = "TIME",
         "duration" = "INTERVAL",
         "binary" = "BLOB",
         "list" = "LIST|ARRAY",
         "a^")
}

duckdb_type_matches <- function(expected, actual, compatible_numeric = FALSE) {
  pattern <- duckdb_type_pattern(expected, compatible_numeric = compatible_numeric)
  allowed <- strsplit(pattern, "|", fixed = TRUE)[[1]]
  actual_base <- toupper(sub("[ (].*$", "", trimws(as.character(actual))))
  actual_base %in% toupper(allowed)
}

#' Validate a registered DuckDB view and compare important columns to registry
#' @export
validate_duckdb_table <- function(con, table_name, schema_registry = NULL, strict = FALSE,
                                  table_schema = NULL) {
  qtbl <- quote_duckdb_ident(table_name)
  desc <- tryCatch(DBI::dbGetQuery(con, paste("DESCRIBE", qtbl)), error = function(e) e)
  if (inherits(desc, "error")) {
    msg <- sprintf("[VALIDATION ERROR] %s DESCRIBE failed: %s", table_name, desc$message)
    log_msg(msg)
    if (strict) stop(msg)
    return(invisible(FALSE))
  }
  n <- tryCatch(DBI::dbGetQuery(con, paste("SELECT COUNT(*) AS n FROM", qtbl))$n, error = function(e) e)
  if (inherits(n, "error")) {
    msg <- sprintf("[VALIDATION ERROR] %s COUNT failed: %s", table_name, n$message)
    log_msg(msg)
    if (strict) stop(msg)
    return(invisible(FALSE))
  }
  desc$canonical_name <- canonical_colnames(desc$column_name)
  mismatch_messages <- character(0)
  logical_identity <- NULL
  if (!is.null(table_schema) && nrow(table_schema) > 0L && "DuckDBTable" %in% names(table_schema)) {
    logical_identity <- data.table::as.data.table(table_schema)[
      tolower(as.character(DuckDBTable)) == tolower(table_name)]
  }
  if (!is.null(schema_registry) && nrow(schema_registry) > 0L) {
    if (!is.null(logical_identity) && nrow(logical_identity) > 0L &&
        all(c("Database", "TableName") %in% names(logical_identity))) {
      validation_database <- as.character(logical_identity$Database[1])
      validation_table <- as.character(logical_identity$TableName[1])
    } else {
      table_parts <- strsplit(table_name, "_", fixed = TRUE)[[1]]
      validation_database <- table_parts[1]
      validation_table <- if (length(table_parts) > 1L) paste(table_parts[-1], collapse = "_") else table_name
    }
    applies <- if ("AppliesTo" %in% names(schema_registry)) as.character(schema_registry$AppliesTo) else rep("all", nrow(schema_registry))
    for (i in seq_len(nrow(schema_registry))) {
      if (!schema_registry_applies(applies[i], database = validation_database, table_name = validation_table)) next
      cols <- desc$canonical_name[grepl(schema_registry$ColumnPattern[i], desc$canonical_name, perl = TRUE, ignore.case = TRUE)]
      if (length(cols) == 0L) next
      expected <- normalize_type_name(schema_registry$CanonicalType[i])
      actual <- desc$column_type[match(cols, desc$canonical_name)]
      bad <- !duckdb_type_matches(expected, actual, compatible_numeric = TRUE)
      if (any(bad)) {
        msg <- sprintf("%s: registry expected %s for pattern %s, but %s",
                       table_name, expected, schema_registry$ColumnPattern[i],
                       paste(paste(cols[bad], actual[bad], sep = "="), collapse = ", "))
        mismatch_messages <- c(mismatch_messages, msg)
        log_msg(sprintf("[SCHEMA WARNING] %s", msg))
      }
    }
  }
  if (!is.null(table_schema)) {
    ts <- data.table::copy(data.table::as.data.table(table_schema))
    if ("DuckDBTable" %in% names(ts)) {
      ts <- ts[tolower(as.character(DuckDBTable)) == tolower(table_name)]
    } else if (all(c("Database", "TableName") %in% names(ts))) {
      ts[, DuckDBTable := paste(Database, TableName, sep = "_")]
      ts <- ts[tolower(as.character(DuckDBTable)) == tolower(table_name)]
    } else {
      ts <- ts[0]
    }
    if (nrow(ts) == 0L) {
      msg <- sprintf("%s: no rows found in the supplied table schema catalog", table_name)
      mismatch_messages <- c(mismatch_messages, msg)
      log_msg(sprintf("[SCHEMA WARNING] %s", msg))
    } else if (!all(c("Column", "CanonicalType") %in% names(ts))) {
      msg <- sprintf("%s: supplied table schema lacks Column and/or CanonicalType", table_name)
      mismatch_messages <- c(mismatch_messages, msg)
      log_msg(sprintf("[SCHEMA WARNING] %s", msg))
    } else {
      ts[, CanonicalColumn := canonical_colnames(Column)]
      ts[, CanonicalType := vapply(CanonicalType, normalize_type_name, character(1))]
      ts <- ts[!duplicated(CanonicalColumn)]
      missing_cols <- setdiff(ts$CanonicalColumn, desc$canonical_name)
      if (length(missing_cols) > 0L) {
        msg <- sprintf("%s: catalog column(s) missing from DuckDB: %s",
                       table_name, paste(missing_cols, collapse = ", "))
        mismatch_messages <- c(mismatch_messages, msg)
        log_msg(sprintf("[SCHEMA WARNING] %s", msg))
      }
      present <- ts[CanonicalColumn %in% desc$canonical_name]
      if (nrow(present) > 0L) {
        actual <- desc$column_type[match(present$CanonicalColumn, desc$canonical_name)]
        bad <- !mapply(duckdb_type_matches, present$CanonicalType, actual,
                       MoreArgs = list(compatible_numeric = FALSE), USE.NAMES = FALSE)
        if (any(bad)) {
          msg <- sprintf("%s: catalog expected exact resolved types, but %s",
                         table_name,
                         paste(paste0(present$CanonicalColumn[bad], " expected=", present$CanonicalType[bad],
                                      " actual=", actual[bad]), collapse = ", "))
          mismatch_messages <- c(mismatch_messages, msg)
          log_msg(sprintf("[SCHEMA WARNING] %s", msg))
        }
      }
      unexpected <- setdiff(desc$canonical_name, ts$CanonicalColumn)
      if (length(unexpected) > 0L) {
        log_msg(sprintf("[SCHEMA NOTICE] %s has DuckDB column(s) absent from the table catalog: %s",
                        table_name, paste(unexpected, collapse = ", ")))
      }
    }
  }
  if (length(mismatch_messages) > 0L) {
    if (strict) stop(sprintf("DuckDB schema validation failed: %s", paste(unique(mismatch_messages), collapse = " | ")))
    return(invisible(FALSE))
  }
  log_msg(sprintf("[VALIDATION OK] %s: %s rows", table_name, formatC(n, format = "d", big.mark = ",")))
  invisible(TRUE)
}

################################################################################
#### Per-file reader dispatch ##################################################
################################################################################
#' Dispatch a single source file to the appropriate reader and writer
#'
#' For each source file, \code{read_fn} determines the row count, applies the
#' \code{PartitionBy} strategy to decide whether the file can be loaded
#' directly into memory or must be processed in memory-bounded chunks, and
#' then delegates to \code{\link{safe_read_csv}}, \code{\link{safe_read_sav}},
#' or \code{\link{safe_read_sav_chunked}} accordingly.
#'
#' \code{read_fn} was originally defined as a nested closure inside
#' \code{\link{generic_db_loader}}, relying on lexical scoping to access
#' \code{MDTSelect}, \code{MasterDBPath}, \code{reader}, \code{PartitionBy},
#' \code{SAV_ROW_THRESHOLD}, \code{RAMThreshold}, and \code{SAV_CHUNK_SIZE}
#' from \code{generic_db_loader}'s frame. It is now a standalone top-level
#' function and all of these values must be passed explicitly as arguments.
#' @section Row-count determination:
#' The exact row count (\code{ncases}) is obtained via
#' \code{haven::read_sav(full_path, col_select = 1L)} -- reading a single
#' column keeps peak RAM proportional to one column's width times the row
#' count, regardless of how wide the file is.
#' @section PartitionBy strategies:
#' \describe{
#'   \item{\code{"NRows"}}{Chunked reading is used when \code{ncases} exceeds
#'     \code{SAV_ROW_THRESHOLD} (or when \code{ncases} cannot be determined).}
#'   \item{\code{"RAMEstimate"}}{A single-row sample (\code{n_max = 1}) is read
#'     to estimate per-row byte width from column types (8 bytes for numeric /
#'     \code{haven_labelled} columns, \code{nchar + 56} for character columns).
#'     The estimated total size (with a 4x safety multiplier) is compared to
#'     \code{RAMThreshold} (in GB) to decide whether to chunk.}
#'   \item{\code{"FAIL"}}{A full direct read, strip, sanitise, align, and
#'     year-assignment is attempted inside a \code{tryCatch}. If any step
#'     errors (most commonly an out-of-memory condition during
#'     \code{haven::read_sav()}), chunked reading is used instead. This
#'     strategy is the most accurate but also the most expensive when the
#'     direct read fails, since the full read is attempted and discarded.}
#' }
#' @param path Character. Relative file path as it appears in
#'   \code{MDTSelect$Path} (used to look up \code{MDTSelect$MDBDir}).
#' @param out_path Character (optional). Full output path for a
#'   single-file (non-chunked) Parquet write. Passed through to
#'   \code{\link{safe_read_sav_chunked}}.
#' @param all_cols Character vector (optional). Union of all
#'   columns for this table, passed to \code{\link{align_columns}}.
#' @param year_dir Character (optional). Hive-partitioned year
#'   directory (e.g. \code{".../NIS_Core/year=2019"}) where chunk files are
#'   written when chunking is used.
#' @param col_classes Named list (optional). Column class map from
#'   \code{\link{build_col_classes}}.
#' @param year_val Integer or character (optional). Year value
#'   passed to \code{\link{add_year_if_missing}}.
#' @param PrintStatus Logical. If \code{TRUE}, prints progress
#'   messages to the console in addition to the log file. Default \code{FALSE}.
#' @param TerminalHivePartition Logical. If \code{TRUE}, chunk files are
#'   written to source-specific \code{batch_id=<stem>_<NNNNN>/data.parquet}
#'   subdirectories instead of flat \code{<stem>_<NNNNN>.parquet} files. Passed to
#'   \code{\link{safe_read_sav_chunked}}.
#' @param MDTSelect Data frame. Subset of the master database table
#'   for the current database, used to resolve \code{path} to a full
#'   filesystem path via \code{MDTSelect$MDBDir}.
#' @param MasterDBPath Character. Root directory containing the
#'   source SAV/CSV files.
#' @param reader Character. One of \code{"sav"} or
#'   \code{"csv"}. CSV files are passed directly to
#'   \code{\link{safe_read_csv}} without row-count dispatch.
#' @param PartitionBy Character. One of \code{"NRows"},
#'   \code{"RAMEstimate"}, or \code{"FAIL"}. See Details.
#' @param SAV_ROW_THRESHOLD Integer. Row count above which the
#'   \code{"NRows"} strategy chunks a file.
#' @param RAMThreshold Numeric. Estimated size in GB above which the
#'   \code{"RAMEstimate"} strategy chunks a file.
#' @param SAV_CHUNK_SIZE Integer. Rows per chunk, passed to
#'   \code{\link{safe_read_sav_chunked}}.
#' @param chunk_size_decrement Integer (optional). Passed through to
#'   \code{\link{safe_read_sav_chunked}}. \code{NULL} (the default) lets
#'   \code{safe_read_sav_chunked()} compute its own default (10\% of
#'   \code{SAV_CHUNK_SIZE}).
#' @param min_chunk_size Integer (optional). Passed through to
#'   \code{\link{safe_read_sav_chunked}}. \code{NULL} (the default) lets
#'   \code{safe_read_sav_chunked()} compute its own default (equal to
#'   \code{chunk_size_decrement}).
#' @return Either:
#' \itemize{
#'   \item A \code{data.table} containing the full file contents (direct read
#'     succeeded), or
#'   \item a list with \code{written = TRUE} and \code{n_rows} (chunked read
#'     succeeded and wrote Parquet files directly), or
#'   \item \code{data.frame()} (file not found or \code{reader = "csv"}
#'     read failed).
#' }
#' @seealso \code{\link{generic_db_loader}}, \code{\link{safe_read_sav}},
#'   \code{\link{safe_read_sav_chunked}}, \code{\link{safe_read_csv}}
#' @examples
#' \donttest{
#' set.seed(1)
#' MasterDBPath <- tempfile("masterdb_")
#' dir.create(file.path(MasterDBPath, "DEMO"), recursive = TRUE)
#' df <- data.frame(AGE = sample(18:90, 200, replace = TRUE),
#'                  SEX = haven::labelled(sample(1:2, 200, replace = TRUE), c(Male = 1, Female = 2)) )
#' haven::write_sav(df, file.path(MasterDBPath, "DEMO", "DEMO_2020_Core.sav"))
#' #### MDTSelect maps the relative Path to its MDBDir subdirectory ####
#' MDTSelect <- data.frame(Database = "DEMO",
#'                         MDBDir = "DEMO",
#'                         Path = "DEMO_2020_Core.sav",
#'                         TableName = "Core",
#'                         Year = 2020,
#'                         FileType = "sav",
#'                         stringsAsFactors = FALSE)
#' LogPath <- tempfile(fileext = ".txt")
#' year_dir <- tempfile("year_2020_")
#' dir.create(year_dir, recursive = TRUE)
#' #### 200 rows < SAV_ROW_THRESHOLD -> direct read, returns a data.table ####
#' result <- read_fn(path = "DEMO_2020_Core.sav",
#'                   year_dir = year_dir,
#'                   out_path = file.path(year_dir, "DEMO_2020_Core_sav.parquet"),
#'                   all_cols = c("AGE", "SEX"),
#'                   year_val = 2020,
#'                   MDTSelect = MDTSelect,
#'                   MasterDBPath = MasterDBPath,
#'                   reader = "sav",
#'                   PartitionBy = "NRows",
#'                   SAV_ROW_THRESHOLD = 1000000L,
#'                   RAMThreshold = 40,
#'                   SAV_CHUNK_SIZE = 5000000L)
#' class(result) # "data.table" "data.frame"
#' nrow(result) # 200
#' unlink(c(MasterDBPath, year_dir, LogPath), recursive = TRUE)
#' }
#' @export
read_fn <- function(path, out_path = NULL, all_cols = NULL, year_dir = NULL,
                    col_classes = NULL, year_val = NULL, PrintStatus = FALSE, TerminalHivePartition = FALSE,
                    MDTSelect, MasterDBPath, reader, PartitionBy, SAV_ROW_THRESHOLD, RAMThreshold, SAV_CHUNK_SIZE,
                    chunk_size_decrement = NULL, min_chunk_size = NULL, partition_keys = "YEAR",
                    partition_values = NULL, max_coerce_na_pct = NULL,
                     ManifestPath = NULL, Database = NULL, TableName = NULL,
                    DuckDBTable = NULL, SourcePath = NULL, SchemaHash = NA_character_,
                    MaxFileStemTruncate = FALSE, accept_partial = FALSE,
                    RepositoryLock = NULL) {
  #### An unrecognized strategy string would otherwise silently behave as   ####
  #### "always direct read" -- an OOM crash hours into a run instead of an  ####
  #### immediate error here.                                                ####
  PartitionBy <- match.arg(PartitionBy, c("NRows", "RAMEstimate", "FAIL"))
  file_mdbdir <- MDTSelect[MDTSelect$Path == path, ]$MDBDir[1]
  row_options <- MDTSelect[MDTSelect$Path == path, , drop = FALSE]
  reader_options <- if (nrow(row_options) > 0L) reader_options_for_row(row_options[1, , drop = FALSE]) else list()
  full_path <- file.path(MasterDBPath, file_mdbdir, path)
  if(!file.exists(full_path)){
    log_msg(sprintf("[ERROR] Check file path. File not found: %s", full_path))
    return(data.frame())
  }
  rd <- get_file_reader(reader)
  if (tolower(reader) %in% c("csv", "tsv", "txt", "gz")) {
    reader_options <- .resolve_delimited_reader_options(full_path, reader_options)
  }
  #### Formats without a chunked implementation are a single full read.     ####
  if (!isTRUE(rd$chunkable)) {
    df <- tryCatch(call_reader(rd, "read_full", full_path, reader_options = reader_options,
                               col_classes = col_classes), error = function(e) {
      log_msg(sprintf("[ERROR] %s read failed for %s: %s", reader, basename(full_path), conditionMessage(e)))
      data.frame()
    })
    if (!is.data.frame(df) || nrow(df) == 0L) return(df)
    if ("YEAR" %in% canonical_colnames(partition_keys)) df <- add_year_if_missing(df, year_val)
    df <- align_columns(df, all_cols, col_classes, max_coerce_na_pct = max_coerce_na_pct)
    return(list(data = df, pre_aligned = TRUE, written = FALSE))
  }
  ncases <- tryCatch(as.numeric(call_reader(rd, "count_rows", full_path,
                                            reader_options = reader_options)), error = function(e) NA_real_)
  DFTemp <- NULL; use_chunked <- FALSE
  if(PartitionBy == "NRows"){
    use_chunked <- is.na(ncases) || ncases > SAV_ROW_THRESHOLD
    log_msg(sprintf("[NRows] %s: %s rows -> %s reader", basename(full_path),
                    ifelse(is.na(ncases), "unknown", formatC(ncases, format="d", big.mark=",")),
                    ifelse(use_chunked, "chunked", "direct")))
  }
  if(PartitionBy == "RAMEstimate"){
    sample_row <- tryCatch(
      utils::head(call_reader(rd, "read_sample", full_path, reader_options = reader_options), 1L),
      error = function(e) NULL
    )
    if(is.null(sample_row) || is.na(ncases)){
      use_chunked <- TRUE
      log_msg(sprintf("[RAMEstimate] %s: sample read unsuccessful -- defaulting to chunked", basename(full_path)))
    } else {
      row_size_bytes <- sum(sapply(seq_len(ncol(sample_row)), function(i) {
        col <- sample_row[[i]]
        if (is.numeric(col) || inherits(col, "haven_labelled")) { 8L
        } else if (is.character(col)) {
          nc <- nchar(as.character(col), allowNA = TRUE)
          nc <- nc[!is.na(nc)]
          if (length(nc) == 0L){ 56L } else {max(nc) + 56L}
        } else { 8L }
      }))
      rm(sample_row)
      estimated_size_gb <- (row_size_bytes * as.numeric(ncases)) / 2^30
      RAMLimit <- estimated_size_gb * 4
      use_chunked <- RAMLimit > RAMThreshold
      log_msg(sprintf("[RAMEstimate] %s: %s rows x %d bytes/row = %.1f GB raw, %.1f GB est (3x) -> %s reader",
                      basename(full_path), formatC(ncases, format="d", big.mark=","),
                      row_size_bytes, estimated_size_gb, RAMLimit,
                      ifelse(use_chunked, "chunked", "direct")))
      rm(RAMLimit); rm(estimated_size_gb); rm(row_size_bytes)
    }
  }
  if(PartitionBy == "FAIL"){
    if(PrintStatus){ print("Performing test read") }
    DFTemp <- tryCatch({
      #### read_full strips label classes and sanitizes encodings, and      ####
      #### errors on failure -- the error is the chunk-dispatch signal.     ####
      df <- call_reader(rd, "read_full", full_path, reader_options = reader_options,
                        col_classes = col_classes)
      if ("YEAR" %in% canonical_colnames(partition_keys)) df <- add_year_if_missing(df, year_val)
      df <- align_columns(df, all_cols, col_classes, max_coerce_na_pct = max_coerce_na_pct)
      list(data = df, error = FALSE, pre_aligned = TRUE)
    }, error = function(e){
      log_msg(sprintf("[LOAD CHECK COMPLETE] Direct read unsuccessful for %s: %s -- using chunked reader",
                      basename(full_path), e$message))
      list(data = NULL, error = TRUE)
    })
    use_chunked <- DFTemp$error
    log_msg(sprintf("[LOAD CHECK COMPLETE] %s -> %s reader",
                    basename(full_path), ifelse(use_chunked, "chunked", "direct")))
  }
  log_msg(sprintf("[READ] %s: %s rows -> %s reader (PartitionBy=%s)", basename(full_path),
                  ifelse(is.na(ncases), "unknown", formatC(ncases, format="d", big.mark=",")),
                  ifelse(use_chunked, "chunked", "direct"), PartitionBy))
  gc(verbose = FALSE)
  if(use_chunked){
    if(!is.null(DFTemp)){ rm(DFTemp); gc(verbose = FALSE) }
    if(PrintStatus){ print("Writing Chunked Table") }
    if (identical(reader, "sav")) {
      safe_read_sav_chunked(path = full_path, year_dir = year_dir, chunk_size = SAV_CHUNK_SIZE, out_path = out_path,
                            all_cols = all_cols, col_classes = col_classes, year_val = year_val, TerminalHivePartition = TerminalHivePartition,
                            chunk_size_decrement = chunk_size_decrement, min_chunk_size = min_chunk_size,
                            partition_keys = partition_keys, partition_values = partition_values,
                            max_coerce_na_pct = max_coerce_na_pct,
                            accept_partial = accept_partial,
                            ManifestPath = ManifestPath, Database = Database, TableName = TableName,
                            DuckDBTable = DuckDBTable, SourcePath = SourcePath %||% path,
                            SchemaHash = SchemaHash, MaxFileStemTruncate = MaxFileStemTruncate,
                            reader_options = reader_options, RepositoryLock = RepositoryLock)
    } else {
      read_delimited_chunked(path = full_path, chunk_size = SAV_CHUNK_SIZE, year_dir = year_dir,
                             all_cols = all_cols, col_classes = col_classes, year_val = year_val,
                             TerminalHivePartition = TerminalHivePartition,
                             partition_keys = partition_keys, partition_values = partition_values,
                             max_coerce_na_pct = max_coerce_na_pct,
                             ManifestPath = ManifestPath, Database = Database, TableName = TableName,
                             DuckDBTable = DuckDBTable, SourcePath = SourcePath %||% path,
                             SchemaHash = SchemaHash, MaxFileStemTruncate = MaxFileStemTruncate,
                             reader = reader, reader_options = reader_options,
                             RepositoryLock = RepositoryLock)
    }
  } else {
    if(PrintStatus){ print("Reading Complete Table") }
    if (!is.null(DFTemp) && !is.null(DFTemp$data)) {
      df_out      <- DFTemp$data
      pre_aligned <- isTRUE(DFTemp$pre_aligned)
      rm(DFTemp); gc(verbose = FALSE)
      list(data = df_out, pre_aligned = pre_aligned, written = FALSE)
    } else {
      tryCatch(call_reader(rd, "read_full", full_path, reader_options = reader_options,
                           col_classes = col_classes), error = function(e) {
        log_msg(sprintf("[ERROR] %s read failed for %s: %s", reader, basename(full_path), conditionMessage(e)))
        data.frame()
      })
    }
  }
}

##################################
#### Create DuckDB Connection ####
##################################
#' Open a DuckDB connection with standard configuration
#'
#' Connects to the DuckDB database file a
#' \code{file.path(FormattedDBPath, DBName)}, then sets \code{threads},
#' \code{memory_limit}, \code{enable_progress_bar}, and
#' \code{temp_directory} for the session.
#' @param FormattedDBPath Character. Directory containing the DuckDB file.
#' @param DBName Character. Name of the DuckDB file.  Defaults to
#'   \code{"DuckDBRelationalDatabase.duckdb"}.
#' @param TempDirPath Character. Path DuckDB will use to spill intermediate
#'   results when \code{memory_limit} is exceeded.
#' @param GB Character. Memory limit string passed to DuckDB's
#'   \code{memory_limit} setting (e.g. \code{"48GB"}).  Defaults to
#'   \code{"48GB"}.
#' @param ReadOnly Logical. If \code{TRUE} (default), the connection is
#'   opened read-only, which allows multiple concurrent readers and is safer
#'   for analyst sessions.
#' @param ProgressBar Logical. If \code{TRUE} (default), DuckDB's built-in
#'   progress bar is enabled for long-running queries.
#' @return An open DBI connection to the DuckDB database.
#' @seealso \code{\link{register_parquet_view}},
#'   \code{\link{DBViewSummary}}
#' @examples
#' \dontrun{
#' tmp_dir  <- tempfile("duckdb_demo_")
#' tmp_tmp  <- tempfile("duckdb_tmp_")
#' dir.create(tmp_dir); dir.create(tmp_tmp)
#' con <- open_duckdb(FormattedDBPath = tmp_dir,
#'                    TempDirPath     = tmp_tmp,
#'                    GB              = "4GB",
#'                    ReadOnly        = FALSE)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' unlink(c(tmp_dir, tmp_tmp), recursive = TRUE)
#' }
#' @export
open_duckdb <- function(FormattedDBPath, DBName = "DuckDBRelationalDatabase.duckdb", TempDirPath, GB = "48GB", ReadOnly = TRUE, ProgressBar = TRUE) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file.path(FormattedDBPath, DBName), read_only = ReadOnly)
  n_threads <- parallel::detectCores()
  DBI::dbExecute(con, glue::glue("SET threads = {n_threads}"))
  DBI::dbExecute(con, glue::glue("SET memory_limit = '{GB}'"))
  bar_status <- ifelse(ProgressBar, "true", "false")
  DBI::dbExecute(con, glue::glue("SET enable_progress_bar = {bar_status}"))
  DBI::dbExecute(con, paste0("SET temp_directory = ", quote_duckdb_string(TempDirPath)))
  return(con)
}

################################################################################
#### Survey-weighted estimate helpers ##########################################
################################################################################

#' Survey-weighted counts and means over a DuckDB view
#'
#' HCUP discharge records are a sample: national estimates require the survey
#' weight (\code{DISCWT} for NIS, \code{TRENDWT} for trend files, etc.). These
#' helpers compute weighted point estimates with the weight handling that is
#' easy to get subtly wrong by hand -- rows with a missing weight are excluded
#' from weighted figures (and counted separately), and for means the weight
#' sum is restricted to rows where the value is non-missing so the denominator
#' matches the numerator.
#'
#' IMPORTANT: these are point estimates only. Standard errors for HCUP data
#' require the full survey design (strata such as \code{NIS_STRATUM}, clusters
#' such as \code{HOSP_NIS}); use the \pkg{survey} package when variance
#' estimates are needed.
#' @param con Live DuckDB connection.
#' @param table Character. Registered view/table name (validated against the
#'   database).
#' @param value_col Character (means only). Numeric column to average.
#' @param weight_col Character. Survey weight column, default \code{"DISCWT"}.
#' @param by Character vector (optional). Grouping columns; validated as
#'   identifiers against the table.
#' @param where Character (optional). Raw SQL predicate appended as
#'   \code{WHERE ...}. Trusted input only -- it is interpolated verbatim.
#' @return data.table. Counts: by-groups, \code{n_unweighted},
#'   \code{n_weighted}, \code{n_missing_weight}. Means additionally:
#'   \code{mean_weighted}, \code{mean_unweighted}, \code{n_value_missing}.
#' @examples
#' \dontrun{
#' hcup_weighted_count(con, "NIS_Core", by = "YEAR")
#' hcup_weighted_mean(con, "NIS_Core", value_col = "LOS", by = "YEAR",
#'                    where = "AGE >= 65")
#' }
#' @export
hcup_weighted_count <- function(con, table, weight_col = "DISCWT", by = NULL, where = NULL) {
  weighted_summary(con, table, value_col = NULL, weight_col = weight_col, by = by, where = where)
}

#### Domain-neutral aliases: identical machinery, but the weight column is  ####
#### required rather than defaulting to HCUP's DISCWT.                      ####
#' @rdname hcup_weighted_count
#' @export
weighted_count <- function(con, table, weight_col, by = NULL, where = NULL) {
  if (missing(weight_col)) stop("weight_col is required for weighted_count(); for HCUP data use hcup_weighted_count() which defaults to DISCWT.")
  weighted_summary(con, table, value_col = NULL, weight_col = weight_col, by = by, where = where)
}

#' @rdname hcup_weighted_count
#' @export
weighted_mean <- function(con, table, value_col, weight_col, by = NULL, where = NULL) {
  if (missing(value_col) || is.null(value_col)) stop("value_col is required for weighted_mean().")
  if (missing(weight_col)) stop("weight_col is required for weighted_mean(); for HCUP data use hcup_weighted_mean() which defaults to DISCWT.")
  weighted_summary(con, table, value_col = value_col, weight_col = weight_col, by = by, where = where)
}

#' @rdname hcup_weighted_count
#' @export
hcup_weighted_mean <- function(con, table, value_col, weight_col = "DISCWT", by = NULL, where = NULL) {
  if (missing(value_col) || is.null(value_col)) stop("value_col is required for hcup_weighted_mean().")
  hcup_weighted_summary(con, table, value_col = value_col, weight_col = weight_col, by = by, where = where)
}

#' @rdname hcup_weighted_count
#' @export
hcup_weighted_summary <- function(con, table, value_col = NULL, weight_col = "DISCWT",
                                  by = NULL, where = NULL) {
  weighted_summary(con, table, value_col = value_col, weight_col = weight_col, by = by, where = where)
}

#' @rdname hcup_weighted_count
#' @export
weighted_summary <- function(con, table, value_col = NULL, weight_col,
                                  by = NULL, where = NULL) {
  if (missing(weight_col)) stop("weight_col is required for weighted_summary(); for HCUP data use hcup_weighted_summary() which defaults to DISCWT.")
  qtbl <- quote_duckdb_ident(table)
  desc <- DBI::dbGetQuery(con, paste("DESCRIBE", qtbl))
  resolve <- function(x, what) {
    hit <- desc$column_name[match(toupper(x), toupper(desc$column_name))]
    if (any(is.na(hit))) {
      stop(sprintf("%s not found in %s: %s", what, table, paste(x[is.na(hit)], collapse = ", ")))
    }
    hit
  }
  w <- quote_duckdb_ident(resolve(weight_col, "weight_col"))
  by_res <- if (!is.null(by)) resolve(by, "by column(s)") else character(0)
  qby <- vapply(by_res, quote_duckdb_ident, character(1))
  sel_by <- if (length(qby) > 0L) paste0(paste(qby, collapse = ", "), ",") else ""
  grp <- if (length(qby) > 0L) paste("GROUP BY", paste(qby, collapse = ", "),
                                     "ORDER BY", paste(qby, collapse = ", ")) else ""
  where_clause <- if (!is.null(where) && nzchar(where)) paste("WHERE", where) else ""
  if (is.null(value_col)) {
    q <- glue::glue("
      SELECT {sel_by}
             COUNT(*) AS n_unweighted,
             SUM(CASE WHEN {w} IS NOT NULL THEN {w} ELSE 0 END) AS n_weighted,
             SUM(CASE WHEN {w} IS NULL THEN 1 ELSE 0 END) AS n_missing_weight
      FROM {qtbl} {where_clause} {grp}")
  } else {
    x <- quote_duckdb_ident(resolve(value_col, "value_col"))
    #### The weighted mean's denominator only counts weights of rows whose  ####
    #### value is present, so numerator and denominator cover the same rows. ####
    q <- glue::glue("
      SELECT {sel_by}
             COUNT(*) AS n_unweighted,
             SUM(CASE WHEN {w} IS NOT NULL THEN {w} ELSE 0 END) AS n_weighted,
             SUM(CASE WHEN {w} IS NULL THEN 1 ELSE 0 END) AS n_missing_weight,
             SUM(CASE WHEN {x} IS NULL THEN 1 ELSE 0 END) AS n_value_missing,
             CASE WHEN SUM(CASE WHEN {x} IS NOT NULL AND {w} IS NOT NULL THEN {w} ELSE 0 END) > 0
                  THEN SUM(CASE WHEN {x} IS NOT NULL AND {w} IS NOT NULL THEN {w} * {x} ELSE 0 END)
                       / SUM(CASE WHEN {x} IS NOT NULL AND {w} IS NOT NULL THEN {w} ELSE 0 END)
                  ELSE NULL END AS mean_weighted,
             AVG({x}) AS mean_unweighted
      FROM {qtbl} {where_clause} {grp}")
  }
  data.table::as.data.table(DBI::dbGetQuery(con, q))
}

###################################
#### Search Diagnosis function ####
###################################
#' Search DuckDB tables for diagnosis / procedure codes
#'
#' Scans every qualifying \code{VARCHAR} column in each of the specified
#' tables for a vector of codes using a single \code{UNPIVOT}-based query
#' per table (falling back to a \code{UNION ALL} if \code{UNPIVOT} is not
#' supported for a given table).  Returns a detailed \code{data.table} plus
#' a rolled-up summary in a named list.
#'
#' Using \code{UNPIVOT} means DuckDB scans each table once regardless of how
#' many codes or columns are searched, rather than issuing one query per code
#' per column.
#' @param con A DBI connection to an open DuckDB database.
#' @param codes Character vector. Diagnosis or procedure codes to search for.
#' @param tables Character vector (optional). Table names to restrict the
#'   search to.  \code{NULL} (default) searches all tables in \code{con}.
#'   Names not found in the database trigger a \code{warning()}, and the
#'   search proceeds with whatever names were found.
#' @param match_type Character scalar.  One of \code{"exact"} (default;
#'   fastest, uses an index when available), \code{"prefix"} (matches all
#'   sub-codes; cannot use a standard index), or \code{"any"} (broadest;
#'   always a full scan).
#' @param col_filter Character scalar. Regular expression applied to column
#'   names to restrict which \code{VARCHAR} columns are searched.  Pass
#'   \code{NULL} to search every \code{VARCHAR} column.
#' @param min_rows Integer. Minimum matching row count for a result to be
#'   included.  Default \code{1L}.
#' @param verbose Logical. If \code{TRUE} (default), prints per-table
#'   progress messages.
#' @return A named list with two elements:
#' \describe{
#'   \item{\code{DiagnosisSearch}}{A \code{data.table} with columns
#'     \code{database_table}, \code{column_name}, \code{code},
#'     \code{n_rows}, \code{total_rows}, \code{pct_of_table},
#'     \code{match_type}.}
#'   \item{\code{DiagnosisSearchSummary}}{A rolled-up \code{data.table}
#'     with one row per code showing \code{n_tables}, \code{n_columns},
#'     \code{total_rows}, \code{tables_found}, and \code{columns_found}.}
#' }
#' @seealso \code{\link{column_availability}}
#' @examples
#' \dontrun{
#' con     <- open_duckdb(FormattedDBPath, TempDirPath = TempDirPath, ReadOnly = TRUE)
#' results <- search_diagnosis_codes(con, codes = c("K35.2", "K35.3"),
#'                                   match_type = "exact")
#' View(results$DiagnosisSearch)
#' View(results$DiagnosisSearchSummary)
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
#' @export
search_diagnosis_codes <- function(con, codes, tables = NULL, match_type = c("exact", "prefix", "any"),
    col_filter = "dx|diag|icd|ecode|dcode|pcode|ais|code|ccs|drg|cpt|proc", min_rows = 1L, verbose = TRUE ){
  match_type <- match.arg(match_type)
  all_tables <- DBI::dbListTables(con)
  if (is.null(tables)) {
    search_tables <- all_tables
  } else {
    missing_tbls <- setdiff(tables, all_tables)
    if (length(missing_tbls) > 0) {
      warning(sprintf("Tables not found in database: %s", paste(missing_tbls, collapse = ", ")))
    }
    search_tables <- intersect(tables, all_tables)
  }
  if (length(search_tables) == 0) stop("No valid tables to search.")
  if (verbose) message(sprintf("Searching %d tables for %d codes...", length(search_tables), length(codes)))
  safe_codes <- gsub("'", "''", codes)
  match_predicate <- switch(match_type,
                            "exact"  = glue::glue("UPPER(diagnosis_value) IN ({glue::glue_collapse(glue::glue(\"UPPER('{safe_codes}')\"), sep = ', ')})"),
                            "prefix" = glue::glue("({glue::glue_collapse(glue::glue(\"UPPER(diagnosis_value) LIKE UPPER('{safe_codes}%')\"), sep = ' OR ')})"),
                            "any"    = glue::glue("({glue::glue_collapse(glue::glue(\"UPPER(diagnosis_value) LIKE UPPER('%{safe_codes}%')\"), sep = ' OR ')})") )
  assign_codes <- function(values, mtype){
    if(mtype == "exact") {
      idx <- match(toupper(values), toupper(codes))
      return(codes[idx])
    }
    lapply(values, function(v){ vu <- toupper(v)
      if (mtype == "prefix") { codes[vapply(toupper(codes), function(cd) startsWith(vu, cd), logical(1))]
      } else { codes[vapply(toupper(codes), function(cd) grepl(cd, vu, fixed = TRUE), logical(1))] }
    })
    }
  results <- vector("list", length(search_tables))
  for (ti in seq_along(search_tables)) {
    tbl <- search_tables[ti]
    tbl_q <- quote_duckdb_ident(tbl)
    if (verbose) message(sprintf("  [%d/%d] %s", ti, length(search_tables), tbl))
    col_info <- tryCatch({ DBI::dbGetQuery(con, glue::glue("DESCRIBE {tbl_q}")) },
      error = function(e) {
        if (verbose) message(sprintf("    [SKIP] Could not describe %s: %s", tbl, e$message))
        return(NULL)
      })
    if (is.null(col_info) || nrow(col_info) == 0) next
    varchar_cols <- col_info$column_name[grepl("VARCHAR|TEXT|STRING|CHAR", col_info$column_type, ignore.case = TRUE) ]
    if (length(varchar_cols) == 0) {
      if (verbose) message("    [SKIP] No VARCHAR columns")
      next
    }
    if(!is.null(col_filter) && nchar(col_filter) > 0){
      varchar_cols <- varchar_cols[grepl(col_filter, varchar_cols, ignore.case = TRUE)]
    }
    if(length(varchar_cols) == 0){
      if (verbose) message("[SKIP] No columns match col_filter")
      next
    }
    total_rows <- tryCatch({ DBI::dbGetQuery(con, glue::glue("SELECT COUNT(*) AS n FROM {tbl_q}"))$n},
      error = function(e) NA_integer_ )
    varchar_cols_q <- quote_duckdb_ident(varchar_cols)
    col_list <- paste(varchar_cols_q, collapse = ", ")
    full_sql <- glue::glue("
      WITH unpivoted AS (
        UNPIVOT {tbl_q}
        ON {col_list}
        INTO NAME diagnosis_column VALUE diagnosis_value
      )
      SELECT
        diagnosis_column,
        diagnosis_value,
        COUNT(*) AS n_rows
      FROM unpivoted
      WHERE diagnosis_value IS NOT NULL
        AND {match_predicate}
      GROUP BY diagnosis_column, diagnosis_value
    ")
    tbl_result <- tryCatch({DBI::dbGetQuery(con, full_sql)},
      error = function(e) {
        if (verbose) message(sprintf("    [UNPIVOT failed, falling back] %s: %s", tbl, e$message))
        union_sql <- paste(
          vapply(varchar_cols, function(col) {
            col_q <- quote_duckdb_ident(col)
            col_s <- quote_duckdb_string(col)
            glue::glue("SELECT {col_s} AS diagnosis_column, {col_q} AS diagnosis_value FROM {tbl_q}")
          }, character(1)),
          collapse = "\nUNION ALL\n"
        )
        fallback_sql <- glue::glue("
          SELECT diagnosis_column, diagnosis_value, COUNT(*) AS n_rows
          FROM ({union_sql})
          WHERE diagnosis_value IS NOT NULL
            AND {match_predicate}
          GROUP BY diagnosis_column, diagnosis_value
        ")
        tryCatch(DBI::dbGetQuery(con, fallback_sql),
                 error = function(e2) {
                   if (verbose) message(sprintf("    [ERR] %s: %s", tbl, e2$message))
                   NULL
                 })
      })
    if (is.null(tbl_result) || nrow(tbl_result) == 0) next
    tbl_result$diagnosis_column <- gsub('^"|"$', "", tbl_result$diagnosis_column)
    matched_codes <- assign_codes(tbl_result$diagnosis_value, match_type)
    expanded <- if (match_type == "exact") {
      tbl_result$code <- matched_codes
      as.data.table(tbl_result)
    } else {
      dt <- as.data.table(tbl_result)
      n_match <- lengths(matched_codes)
      dt_rep  <- dt[rep(seq_len(.N), n_match)]
      dt_rep$code <- unlist(matched_codes)
      dt_rep
    }
    expanded <- expanded[, .(n_rows = sum(n_rows)), by = .(diagnosis_column, code)]
    expanded[, database_table := tbl]
    expanded[, total_rows     := total_rows]
    expanded[, pct_of_table   := round(100 * n_rows / total_rows, 4)]
    expanded[, match_type     := match_type]
    setnames(expanded, "diagnosis_column", "column_name")
    expanded <- expanded[n_rows >= min_rows]
    results[[ti]] <- expanded
  }
  non_null_results <- Filter(Negate(is.null), results)
  out <- if (length(non_null_results) > 0L) rbindlist(non_null_results, fill = TRUE) else data.table()
  if (nrow(out) == 0) {
    if (verbose) message("No matches found.")
    empty_search <- data.table(
      database_table = character(),
      column_name    = character(),
      code           = character(),
      n_rows         = integer(),
      total_rows     = integer(),
      pct_of_table   = numeric(),
      match_type     = character()
    )
    empty_summary <- data.table(code = character(),
                                n_tables = integer(),
                                n_columns = integer(),
                                total_rows = integer(),
                                tables_found = character(),
                                columns_found = character())
    return(list(DiagnosisSearch = empty_search, DiagnosisSearchSummary = empty_summary))
  }
  setorder(out, -n_rows, database_table, column_name, code)
  setcolorder(out, c("database_table", "column_name", "code",
                     "n_rows", "total_rows", "pct_of_table", "match_type"))
  if (verbose) {
    message(sprintf(
      "\nSearch complete. %d matches across %d table-column combinations.",
      sum(out$n_rows > 0), nrow(unique(out[, .(database_table, column_name)])) ))
  }
  DiagnosisSearchSummary <- out[n_rows > 0][, .(n_tables = uniqueN(database_table),
                                                                 n_columns = uniqueN(paste(database_table, column_name, sep = ".")),
                                                                 total_rows = sum(n_rows),
                                                                 tables_found = paste(sort(unique(database_table)), collapse = ", "),
                                                                 columns_found = paste(sort(unique(paste(database_table, column_name, sep = "."))), collapse = ", ") ),
                                                             by = code ][order(-total_rows)]
  return(list(DiagnosisSearch=out, DiagnosisSearchSummary=DiagnosisSearchSummary))
}

#############################################################################################
#### Determine the columns in a table across each partition that contain relevant information ####
#############################################################################################
#' Report non-NA percentage per column per partition from a Parquet table
#'
#' For each Hive partition directory of \code{table_name} under
#' \code{ParquetBasePath}, runs \code{SUMMARIZE} in DuckDB to obtain the
#' \code{null_percentage} for every column without pulling any column data
#' into R.  Returns the results as a wide matrix (column \eqn{\times} partition)
#' alongside a boolean presence matrix and pre-computed lists of mostly-empty
#' and partition-inconsistent columns.
#' @param con A DBI connection to an open DuckDB database.
#' @param table_name Character. Name of the table (must correspond to a
#'   subdirectory of \code{ParquetBasePath}).
#' @param ParquetBasePath Character. Root directory of the Parquet store.
#' @return A named list with four elements:
#' \describe{
#'   \item{\code{PercentageNonNA}}{Wide \code{data.frame}: columns are partitions,
#'     rows are column names, values are \code{pct_non_na} (0--100).}
#'   \item{\code{ContainsValues}}{Wide logical \code{data.frame}: \code{TRUE}
#'     where \code{pct_non_na > 0}.}
#'   \item{\code{mostly_empty}}{Rows of \code{PercentageNonNA} where the
#'     maximum across years is \eqn{\leq 1\%}.}
#'   \item{\code{inconsistent}}{Rows where some partitions have \code{> 1\%}
#'     non-NA and others have \eqn{\leq 1\%} -- i.e., the column is present
#'     in some partitions but effectively absent in others.}
#' }
#' @seealso \code{\link{ColumnAvailabilityCompile}},
#'   \code{\link{ColumnAvailabilityView}}
#' @examples
#' \dontrun{
#' con  <- open_duckdb(FormattedDBPath, TempDirPath = TempDirPath, ReadOnly = TRUE)
#' avail <- column_availability(con, "NIS_Core", ParquetBasePath)
#' head(avail$PercentageNonNA)
#' avail$mostly_empty$column
#' DBI::dbDisconnect(con, shutdown = TRUE)
#' }
#' @export
column_availability <- function(con, table_name, ParquetBasePath) {
  table_dir <- file.path(ParquetBasePath, table_name)
  empty_result <- list(
    PercentageNonNA = data.frame(column = character()),
    ContainsValues = data.frame(column = character()),
    mostly_empty = data.frame(column = character()),
    inconsistent = data.frame(column = character())
  )
  if (!dir.exists(table_dir)) {
    log_msg(sprintf("[AVAILABILITY WARNING] Table directory does not exist: %s", table_dir))
    return(empty_result)
  }
  all_dirs <- list.dirs(table_dir, recursive = TRUE, full.names = TRUE)
  hive_dirs <- all_dirs[grepl("=", basename(all_dirs), fixed = TRUE)]
  hive_dirs <- hive_dirs[!grepl("^batch_id=", basename(hive_dirs), ignore.case = TRUE)]
  has_parquet <- vapply(hive_dirs, function(d) {
    length(list.files(d, pattern = "\\.parquet$", recursive = TRUE,
                      full.names = TRUE, ignore.case = TRUE)) > 0L
  }, logical(1))
  hive_dirs <- hive_dirs[has_parquet]
  if (length(hive_dirs) > 0L) {
    hive_dirs_fwd <- gsub("\\\\", "/", hive_dirs)
    deepest <- !vapply(seq_along(hive_dirs_fwd), function(i) {
      any(startsWith(hive_dirs_fwd[-i], paste0(hive_dirs_fwd[i], "/")))
    }, logical(1))
    partition_dirs <- hive_dirs[deepest]
  } else {
    direct_files <- list.files(table_dir, pattern = "\\.parquet$", recursive = TRUE,
                               full.names = TRUE, ignore.case = TRUE)
    partition_dirs <- if (length(direct_files) > 0L) table_dir else character(0)
  }
  if (length(partition_dirs) == 0L) {
    log_msg(sprintf("[AVAILABILITY WARNING] No parquet-backed partition directories found under %s", table_dir))
    return(empty_result)
  }
  table_dir_fwd <- paste0(gsub("\\\\", "/", normalizePath(table_dir, winslash = "/", mustWork = FALSE)), "/")
  reports <- lapply(partition_dirs, function(part_dir) {
    part_fwd <- gsub("\\\\", "/", normalizePath(part_dir, winslash = "/", mustWork = FALSE))
    rel <- sub(paste0("^", gsub("([\\W])", "\\\\\\1", table_dir_fwd)), "", paste0(part_fwd, "/"))
    rel <- sub("/$", "", rel)
    partition_label <- if (grepl("^(YEAR|year)=", rel)) {
      sub("^(YEAR|year)=", "", rel)
    } else if (nzchar(rel)) {
      rel
    } else {
      "all"
    }
    summary_tbl <- tryCatch(
      DBI::dbGetQuery(con, sprintf("SUMMARIZE SELECT * FROM read_parquet(%s, hive_partitioning = true, union_by_name = true)",
                                   quote_duckdb_string(paste0(part_fwd, "/**/*.parquet")))),
      error = function(e) NULL )
    if (is.null(summary_tbl)) return(NULL)
    data.frame(Partition = partition_label, column = summary_tbl$column_name, pct_non_na = round(100 - summary_tbl$null_percentage, 2) )
  })
  long <- do.call(rbind, Filter(Negate(is.null), reports))
  if (is.null(long) || nrow(long) == 0L) return(empty_result)
  wide <- stats::reshape(long, idvar = "column", timevar = "Partition", direction = "wide", v.names = "pct_non_na")
  names(wide) <- gsub("^pct_non_na\\.", "", names(wide))
  wide <- wide[!is.na(wide$column),]
  wideLogic <- wide[,!(colnames(wide) %in% "column")] > 0
  wideLogic <- cbind(data.frame(column = wide$column), wideLogic)
  partition_cols <- setdiff(names(wide), "column")
  max_pct <- apply(wide[partition_cols], 1, function(x){max(x, na.rm = TRUE)})
  mostly_empty <- wide[max_pct <= 1, ]
  inconsistent <- wide[max_pct > 1 & apply(wide[partition_cols], 1, function(x){min(x, na.rm = TRUE)}) <= 1, ]
  return(list(PercentageNonNA = wide, ContainsValues = wideLogic, mostly_empty = mostly_empty, inconsistent = inconsistent))
}

excel_sheet_name <- function(name, existing = character(), prefer_existing = FALSE) {
  out <- gsub("[\\[\\]\\:\\*\\?\\/\\\\]", "_", as.character(name)[1])
  out <- trimws(out)
  if (is.na(out) || !nzchar(out)) out <- "Sheet"
  out <- substr(out, 1L, 31L)
  if (isTRUE(prefer_existing) && out %in% existing) return(out)
  if (!out %in% existing) return(out)
  for (i in seq_len(999L)) {
    suffix <- paste0("_", i)
    candidate <- paste0(substr(out, 1L, 31L - nchar(suffix)), suffix)
    if (!candidate %in% existing) return(candidate)
  }
  stop(sprintf("Could not create a unique Excel sheet name for '%s'", name))
}

availability_sheet_map_name <- function() "_SheetMap"

read_availability_sheet_map <- function(WBPath) {
  empty <- data.frame(OriginalName = character(), SheetName = character(), stringsAsFactors = FALSE)
  if (is.null(WBPath) || !nzchar(WBPath) || !file.exists(WBPath)) return(empty)
  sheets <- tryCatch(openxlsx::getSheetNames(WBPath), error = function(e) character())
  map_name <- availability_sheet_map_name()
  if (!map_name %in% sheets) return(empty)
  out <- tryCatch(openxlsx::read.xlsx(WBPath, sheet = map_name), error = function(e) empty)
  if (!all(c("OriginalName", "SheetName") %in% names(out))) return(empty)
  out[, c("OriginalName", "SheetName"), drop = FALSE]
}

availability_sheet_name_for <- function(WBPath, table_name) {
  map <- read_availability_sheet_map(WBPath)
  label <- as.character(table_name)[1]
  hit <- map[as.character(map$OriginalName) == label, , drop = FALSE]
  if (nrow(hit) > 0L) return(as.character(hit$SheetName[1]))
  sheets <- if (!is.null(WBPath) && nzchar(WBPath) && file.exists(WBPath)) {
    openxlsx::getSheetNames(WBPath)
  } else {
    character()
  }
  excel_sheet_name(label, existing = setdiff(sheets, availability_sheet_map_name()), prefer_existing = TRUE)
}

#' Run \code{column_availability()} for a list of tables and write to Excel
#'
#' Iterates over \code{tables}, calls \code{\link{column_availability}} for
#' each, and writes the \code{PercentageNonNA} matrix to a sheet in an Excel
#' workbook at \code{SupportingInfoPath}.  Creates the workbook on the first
#' table if it does not yet exist; appends a new sheet for each subsequent
#' table via \code{\link{WorkbookUpdateopenxlsx}}.
#' @param con A DBI connection to an open DuckDB database.
#' @param tables Character vector of table names to process.
#' @param ParquetBasePath Character. Root directory of the Parquet store.
#' @param SupportingInfoPath Character. Path to the output \code{.xlsx}
#'   workbook.
#' @param verbose Logical. If \code{TRUE} (default), logs or prints a
#'   per-table summary line.
#' @param logStatus Logical. If \code{TRUE} (default), messages are written
#'   via \code{\link{log_msg}}; otherwise they are printed to the console.
#' @param StartAt Integer. Index into \code{tables} at which to begin (for
#'   resuming an interrupted run).  Default \code{1}.
#' @return \code{invisible(NULL)}.  Called for its side effects.
#' @seealso \code{\link{column_availability}},
#'   \code{\link{ColumnAvailabilityView}},
#'   \code{\link{WorkbookUpdateopenxlsx}}
#' @export
ColumnAvailabilityCompile <- function(con, tables, ParquetBasePath, SupportingInfoPath, verbose = TRUE, logStatus = TRUE, StartAt = 1){
  if (length(tables) == 0L || StartAt > length(tables)) return(invisible(NULL))
  for(tbl in StartAt:length(tables)){
    avail <- column_availability(con, tables[tbl], ParquetBasePath)
    avail <- avail[["PercentageNonNA"]]
    partition_cols <- setdiff(names(avail), "column")
    if (length(partition_cols) == 0L || nrow(avail) == 0L) {
      n_empty <- 0L
      n_inconsistent <- 0L
    } else {
      #### Without na.rm, a column absent from any one partition yields NA   ####
      #### here and is silently dropped from BOTH counters -- yet columns    ####
      #### missing from some partitions are exactly the "inconsistent" ones. ####
      max_pct <- suppressWarnings(apply(avail[partition_cols], 1, max, na.rm = TRUE))
      min_pct <- suppressWarnings(apply(avail[partition_cols], 1, min, na.rm = TRUE))
      max_pct[!is.finite(max_pct)] <- 0   # all-NA row: treat as empty everywhere
      min_pct[!is.finite(min_pct)] <- 0
      n_empty <- sum(max_pct <= 1, na.rm = TRUE)
      n_inconsistent <- sum(max_pct > 1 & min_pct <= 1, na.rm = TRUE)
    }
    WorkbookUpdateopenxlsx(WBPath = SupportingInfoPath, DTAdd = avail, SheetName = tables[tbl])
    if(verbose){
      if(logStatus){
      log_msg(sprintf("%-20s %3d columns total, %3d effectively empty, %3d inconsistent across partitions", tables[tbl], nrow(avail), n_empty, n_inconsistent))
      } else {
        print(sprintf("%-20s %3d columns total, %3d effectively empty, %3d inconsistent across partitions", tables[tbl], nrow(avail), n_empty, n_inconsistent))
      }
      print(paste(tbl, "of", length(tables), "Complete:", tables[tbl]))
      }
  }
}

#' Visualise column availability as a heatmap
#'
#' Reads the \code{PercentageNonNA} sheet for \code{table_name} from the
#' Excel workbook written by \code{\link{ColumnAvailabilityCompile}} and
#' renders a \pkg{ComplexHeatmap} heatmap where each cell is coloured by
#' the percentage of non-\code{NA} values (0--100), with grey cells
#' indicating columns that are fully absent in a given partition.
#' @param SupportingInfoPath Character. Path to the \code{.xlsx} workbook
#'   produced by \code{\link{ColumnAvailabilityCompile}}.
#' @param table_name Character. Sheet name (i.e. table name) to read and
#'   plot.
#' @return A named list with two elements:
#' \describe{
#'   \item{\code{table}}{The raw \code{data.frame} read from the workbook.}
#'   \item{\code{heatmap}}{A \code{ComplexHeatmap::Heatmap} object ready
#'     to print or save.}
#' }
#' @seealso \code{\link{ColumnAvailabilityCompile}}
#' @export
ColumnAvailabilityView <- function(SupportingInfoPath, table_name){
  sheet_name <- availability_sheet_name_for(SupportingInfoPath, table_name)
  temp <- openxlsx::read.xlsx(SupportingInfoPath, sheet = sheet_name)
  raw_table <- temp
  if (!"column" %in% names(temp)) stop(sprintf("Availability sheet '%s' does not contain a column field.", sheet_name))
  rownames(temp) <- temp$column
  temp$column <- NULL
  mat <- as.matrix(temp)
  mat[mat ==0] <- NA
  col_fun <- circlize::colorRamp2(c(0, 50, 100), RColorBrewer::brewer.pal(9, "Blues")[c(2,5,8)] )
  P <- ComplexHeatmap::Heatmap(matrix = mat,
               cluster_rows = FALSE,
               cluster_columns = FALSE,
               col = col_fun,
               na_col = "grey90",
               rect_gp = grid::gpar(col = "white", lwd = 1.5),
               name = "Percentage of Usable data",
               row_title = paste("Columns in:", table_name),
               column_title = "Partition")
  return(list(table = raw_table, heatmap = P, sheet = sheet_name))
}

############################################
#### Function that updates the workbook ####
############################################
#' Add a sheet to an existing \pkg{openxlsx} workbook
#'
#' Loads the workbook at \code{WBPath}, adds a new sheet named
#' \code{SheetName}, writes \code{DTAdd} to it, and saves the workbook back
#' to the same path.
#' @param WBPath Character. Path to an existing \code{.xlsx} workbook.
#' @param DTAdd A \code{data.frame} or \code{data.table} to write into the
#'   new sheet.
#' @param SheetName Character. Name of the new worksheet.
#' @return \code{invisible(NULL)}.  Called for its side effects.
#' @seealso \code{\link{ColumnAvailabilityCompile}}
#' @keywords internal
WorkbookUpdateopenxlsx <- function(WBPath, DTAdd, SheetName){
  dir.create(dirname(WBPath), recursive = TRUE, showWarnings = FALSE)
  wb <- if (file.exists(WBPath)) openxlsx::loadWorkbook(WBPath) else openxlsx::createWorkbook()
  map_name <- availability_sheet_map_name()
  sheet_map <- read_availability_sheet_map(WBPath)
  label <- as.character(SheetName)[1]
  hit <- sheet_map[as.character(sheet_map$OriginalName) == label, , drop = FALSE]
  if (nrow(hit) > 0L) {
    resolved_sheet <- as.character(hit$SheetName[1])
  } else {
    existing <- unique(c(names(wb), as.character(sheet_map$SheetName)))
    natural_sheet <- excel_sheet_name(label, existing = character(), prefer_existing = FALSE)
    if (natural_sheet %in% names(wb) && !natural_sheet %in% as.character(sheet_map$SheetName)) {
      resolved_sheet <- natural_sheet
    } else {
      resolved_sheet <- excel_sheet_name(label, existing = setdiff(existing, map_name), prefer_existing = FALSE)
    }
    sheet_map <- rbind(sheet_map,
                       data.frame(OriginalName = label, SheetName = resolved_sheet, stringsAsFactors = FALSE))
  }
  if (resolved_sheet %in% names(wb)) {
    openxlsx::removeWorksheet(wb, sheet = resolved_sheet)
  }
  openxlsx::addWorksheet(wb, sheetName = resolved_sheet)
  openxlsx::writeData(wb, sheet = resolved_sheet, x = DTAdd)
  if (map_name %in% names(wb)) openxlsx::removeWorksheet(wb, sheet = map_name)
  openxlsx::addWorksheet(wb, sheetName = map_name)
  openxlsx::writeData(wb, sheet = map_name, x = sheet_map)
  tmp <- tempfile(pattern = paste0(basename(WBPath), ".tmp_"), tmpdir = dirname(WBPath), fileext = ".xlsx")
  openxlsx::saveWorkbook(wb, tmp, overwrite = TRUE)
  replace_file_safely(tmp, WBPath)
  invisible(resolved_sheet)
}

#############################################################################
#### Repository architecture and schema engine overrides ####################
#############################################################################
#' Create a domain-neutral data-contract template
#' @export
build_data_contract_template <- function() {
  data.frame(
    ContractName = character(), DuckDBTable = character(), Column = character(),
    Rule = character(), Value = character(), ReferenceTable = character(),
    ReferenceColumn = character(), Where = character(), Severity = character(),
    Enabled = logical(), Notes = character(), stringsAsFactors = FALSE)
}

#' Read or create a repository data-contract file
#' @export
load_data_contracts <- function(DataContractPath, create_if_missing = FALSE) {
  if (is.null(DataContractPath) || !nzchar(DataContractPath)) return(data.table::data.table())
  if (!file.exists(DataContractPath)) {
    if (!isTRUE(create_if_missing)) return(data.table::data.table())
    template <- build_data_contract_template()
    if (is_excel_workbook_path(DataContractPath)) {
      write_xlsx_safely(list(DataContracts = template), DataContractPath)
    } else {
      write_csv_safely(template, DataContractPath)
    }
    return(data.table::as.data.table(template))
  }
  out <- if (is_excel_workbook_path(DataContractPath)) {
    data.table::as.data.table(openxlsx::read.xlsx(DataContractPath, sheet = "DataContracts"))
  } else {
    data.table::fread(DataContractPath)
  }
  required <- c("DuckDBTable", "Column", "Rule")
  missing <- setdiff(required, names(out))
  if (length(missing) > 0L) stop(sprintf("Data contract file is missing: %s", paste(missing, collapse = ", ")))
  defaults <- list(ContractName = NA_character_, Value = NA_character_, ReferenceTable = NA_character_,
                   ReferenceColumn = NA_character_, Where = NA_character_, Severity = "error",
                   Enabled = TRUE, Notes = NA_character_)
  for (nm in names(defaults)) if (!nm %in% names(out)) out[, (nm) := defaults[[nm]]]
  out[, Rule := tolower(trimws(as.character(Rule)))]
  out[, Severity := tolower(trimws(as.character(Severity)))]
  out[is.na(Severity) | !nzchar(Severity), Severity := "error"]
  out[, Enabled := ifelse(is.na(Enabled), TRUE, as.logical(Enabled))]
  allowed_rules <- c("not_null", "unique", "range", "allowed", "regex", "foreign_key")
  bad_rules <- out[Enabled %in% TRUE & !Rule %in% allowed_rules]
  if (nrow(bad_rules) > 0L) stop(sprintf("Unsupported data-contract rule(s): %s", paste(unique(bad_rules$Rule), collapse = ", ")))
  bad_severity <- out[Enabled %in% TRUE & !Severity %in% c("error", "warning")]
  if (nrow(bad_severity) > 0L) stop("Data-contract Severity must be 'error' or 'warning'.")
  out
}

contract_literal <- function(x) {
  x <- trimws(as.character(x))
  if (grepl("^-?[0-9]+(?:\\.[0-9]+)?$", x)) x else quote_duckdb_string(x)
}

#' Validate repository tables against declarative data contracts
#' @export
validate_data_contracts <- function(con, DataContractPath, strict = TRUE, logStatus = TRUE,
                                    LogPath = NULL, RunId = NULL) {
  previous_run <- if (!is.null(LogPath) || !is.null(RunId)) begin_repository_run(LogPath, RunId) else NULL
  if (!is.null(previous_run)) on.exit(restore_repository_run(previous_run), add = TRUE)
  contracts <- load_data_contracts(DataContractPath, create_if_missing = FALSE)
  contracts <- contracts[Enabled %in% TRUE]
  empty <- data.table::data.table(ContractName = character(), DuckDBTable = character(),
                                  Column = character(), Rule = character(), Severity = character(),
                                  Violations = numeric(), Status = character(), Message = character())
  if (nrow(contracts) == 0L) return(invisible(empty))
  tables <- DBI::dbListTables(con)
  results <- vector("list", nrow(contracts))
  for (i in seq_len(nrow(contracts))) {
    rule <- contracts[i]
    table <- as.character(rule$DuckDBTable)
    column <- as.character(rule$Column)
    message <- NA_character_
    violations <- NA_real_
    try_result <- tryCatch({
      if (!table %in% tables) stop(sprintf("Table not found: %s", table))
      desc <- DBI::dbGetQuery(con, paste("DESCRIBE", quote_duckdb_ident(table)))
      if (!column %in% desc$column_name) stop(sprintf("Column not found: %s.%s", table, column))
      qt <- quote_duckdb_ident(table); qc <- quote_duckdb_ident(column)
      where <- if (!is.na(rule$Where) && nzchar(trimws(rule$Where))) paste0(" AND (", rule$Where, ")") else ""
      sql <- switch(rule$Rule,
        not_null = sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s IS NULL%s", qt, qc, where),
        unique = sprintf("SELECT COALESCE(SUM(n - 1), 0) AS n FROM (SELECT %s, COUNT(*) n FROM %s WHERE %s IS NOT NULL%s GROUP BY %s HAVING COUNT(*) > 1)", qc, qt, qc, where, qc),
        range = {
          bounds <- trimws(strsplit(as.character(rule$Value), ";", fixed = TRUE)[[1]])
          if (length(bounds) != 2L) stop("range Value must be 'minimum;maximum'.")
          sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL AND (%s < %s OR %s > %s)%s",
                  qt, qc, qc, contract_literal(bounds[1]), qc, contract_literal(bounds[2]), where)
        },
        allowed = {
          values <- trimws(strsplit(as.character(rule$Value), ";", fixed = TRUE)[[1]])
          if (length(values) == 0L || any(!nzchar(values))) stop("allowed Value must contain semicolon-separated values.")
          sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL AND %s NOT IN (%s)%s",
                  qt, qc, qc, paste(vapply(values, contract_literal, character(1)), collapse = ", "), where)
        },
        regex = sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s IS NOT NULL AND NOT regexp_matches(CAST(%s AS VARCHAR), %s)%s",
                        qt, qc, qc, quote_duckdb_string(as.character(rule$Value)), where),
        foreign_key = {
          rt <- as.character(rule$ReferenceTable); rc <- as.character(rule$ReferenceColumn)
          if (!rt %in% tables) stop(sprintf("Reference table not found: %s", rt))
          if (is.na(rc) || !nzchar(rc)) stop("foreign_key requires ReferenceColumn.")
          sprintf("SELECT COUNT(*) AS n FROM %s child WHERE child.%s IS NOT NULL%s AND NOT EXISTS (SELECT 1 FROM %s parent WHERE parent.%s = child.%s)",
                  qt, qc, where, quote_duckdb_ident(rt), quote_duckdb_ident(rc), qc)
        })
      as.numeric(DBI::dbGetQuery(con, sql)$n[1])
    }, error = function(e) e)
    if (inherits(try_result, "error")) {
      message <- conditionMessage(try_result)
      status <- "error"
    } else {
      violations <- try_result
      status <- if (violations == 0) "pass" else "fail"
    }
    results[[i]] <- data.table::data.table(
      ContractName = as.character(rule$ContractName), DuckDBTable = table, Column = column,
      Rule = as.character(rule$Rule), Severity = as.character(rule$Severity),
      Violations = violations, Status = status, Message = message)
    if (isTRUE(logStatus)) log_msg(sprintf("[CONTRACT %s] %s %s.%s (%s): %s",
                                           toupper(status), rule$ContractName, table, column,
                                           rule$Rule, ifelse(is.na(violations), message, paste(violations, "violation(s)"))))
  }
  out <- data.table::rbindlist(results, fill = TRUE)
  fatal <- out$Severity == "error" & out$Status %in% c("fail", "error")
  if (isTRUE(strict) && any(fatal)) {
    stop(sprintf("Data-contract validation failed for %d rule(s).", sum(fatal)))
  }
  invisible(out)
}

#' Discover candidate table relationships from compatible schema columns
#' @export
discover_schema_relationships <- function(table_schema, include_candidates = TRUE) {
  ts <- data.table::copy(data.table::as.data.table(table_schema))
  required <- c("DuckDBTable", "Column", "CanonicalType")
  if (!all(required %in% names(ts)) || nrow(ts) == 0L) return(data.table::data.table())
  if (!"Role" %in% names(ts)) ts[, Role := NA_character_]
  if (!"MergeGroup" %in% names(ts)) ts[, MergeGroup := NA_character_]
  if (!"MergeReviewed" %in% names(ts)) ts[, MergeReviewed := FALSE]
  ts[, Column := canonical_colnames(Column)]
  ts[, CanonicalType := vapply(CanonicalType, normalize_type_name, character(1))]
  ts[, MergeGroup := toupper(trimws(as.character(MergeGroup)))]
  ts[, MergeReviewed := as.logical(MergeReviewed)]
  ts[is.na(MergeReviewed), MergeReviewed := FALSE]
  ts[, ExplicitGroup := !is.na(MergeGroup) & nzchar(MergeGroup)]
  ts[, Candidate := ExplicitGroup | (!MergeReviewed &
       (tolower(as.character(Role)) %in% c("join_key", "partition") |
        (isTRUE(include_candidates) & vapply(Column, merge_key_name_candidate, logical(1)))))]
  ts[, RelationGroup := ifelse(ExplicitGroup, MergeGroup, paste0("CANDIDATE::", Column))]
  keys <- ts[Candidate %in% TRUE]
  pairs <- merge(keys, keys, by = c("RelationGroup", "CanonicalType"), allow.cartesian = TRUE,
                 suffixes = c("_Left", "_Right"))
  pairs <- pairs[DuckDBTable_Left < DuckDBTable_Right]
  unique(pairs[, .(LeftTable = DuckDBTable_Left, RightTable = DuckDBTable_Right,
                   Column = Column_Left, CanonicalType,
                   Detection = ifelse(!grepl("^CANDIDATE::", RelationGroup), "approved_group",
                                      ifelse(tolower(as.character(Role_Left)) == "join_key" |
                                         tolower(as.character(Role_Right)) == "join_key", "declared", "candidate")))])
}

#' Initialize a versioned repository directory layout
#'
#' Creates the first two priority infrastructure layers for the data warehouse:
#' schema metadata, manifests, logs, and checkpoints.  The function returns a
#' named list of paths that should be passed into the loader rather than relying
#' on scattered path literals.
#' @export
################################################################################
#### Project scaffolding #######################################################
################################################################################

#' Load a repository configuration file
#'
#' Reads the \code{repository_config.R} written by
#' \code{\link{create_repository_project}}: an R file that defines a list
#' named \code{repository_config} with the paths and thresholds the loader
#' needs. Keeping configuration in one reviewable file removes every
#' hard-coded machine path from the run script.
#' @export
load_repository_config <- function(path) {
  if (!file.exists(path)) stop(sprintf("Configuration file not found: %s", path))
  env <- new.env(parent = baseenv())
  sys.source(path, envir = env)
  cfg <- get0("repository_config", envir = env, inherits = FALSE)
  if (!is.list(cfg)) stop(sprintf("%s must define a list named 'repository_config'.", path))
  required <- c("MasterDBPath", "FormattedDBPath", "MDTPath")
  missing_req <- setdiff(required, names(cfg))
  if (length(missing_req) > 0L) stop(sprintf("repository_config is missing: %s", paste(missing_req, collapse = ", ")))
  defaults <- list(SAV_CHUNK_SIZE = 1000000L, SAV_ROW_THRESHOLD = 1000000L,
                   RAMThreshold = 30, PartitionBy = "NRows",
                   n_workers = max(1L, parallel::detectCores() - 1L),
                   MaxCoerceNAPct = 25, SourceFingerprintMode = "metadata",
                   DBName = "Repository.duckdb", DuckDB_GB = "8GB")
  for (nm in names(defaults)) if (is.null(cfg[[nm]])) cfg[[nm]] <- defaults[[nm]]
  cfg
}

#' Scaffold a new repository project in a directory
#'
#' Generates everything a new user needs to run the workflow against their own
#' data, with no hard-coded paths: a \code{repository_config.R}, an empty MDT
#' workbook with the correct headers, a schema registry (the \code{"generic"}
#' profile by default; \code{"hcup"} for HCUP data), and a commented
#' \code{run_repository.R} script that executes the full pipeline
#' (preflight -> catalog -> load -> views -> reconciliation).
#' @param dir Project directory (created if needed).
#' @param MasterDBPath Where source files live; defaults to
#'   \code{<dir>/source_data}.
#' @param profile Schema-registry profile: "generic" (default) or "hcup".
#' @param overwrite Logical. Refuse to clobber existing scaffold files unless TRUE.
#' @return Invisibly, a named list of the created paths.
#' @seealso \code{\link{generate_example_repository}} for a runnable synthetic example.
#' @export
create_repository_project <- function(dir, MasterDBPath = file.path(dir, "source_data"),
                                      profile = c("generic", "hcup"), overwrite = FALSE) {
  profile <- match.arg(profile)
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(MasterDBPath, recursive = TRUE, showWarnings = FALSE)
  formatted <- file.path(dir, "formatted")
  paths <- list(dir = dir, MasterDBPath = MasterDBPath, FormattedDBPath = formatted,
                ConfigPath = file.path(dir, "repository_config.R"),
                MDTPath = file.path(dir, "DBSetup.xlsx"),
                RunnerPath = file.path(dir, "run_repository.R"))
  for (p in c(paths$ConfigPath, paths$MDTPath, paths$RunnerPath)) {
    if (file.exists(p) && !isTRUE(overwrite)) {
      stop(sprintf("%s already exists; pass overwrite = TRUE to replace scaffold files.", p))
    }
  }
  norm <- function(p) gsub("\\\\", "/", p)
  writeLines(c(
    "#### Repository configuration -- the only file with machine-specific paths. ####",
    "repository_config <- list(",
    sprintf("  MasterDBPath    = \"%s\",   # root of the source data files", norm(MasterDBPath)),
    sprintf("  FormattedDBPath = \"%s\",   # parquet store, checkpoints, logs, catalog", norm(formatted)),
    sprintf("  MDTPath         = \"%s\",   # the Master Database Table workbook", norm(paths$MDTPath)),
    "  PartitionBy       = \"NRows\",     # NRows | RAMEstimate | FAIL",
    "  SAV_ROW_THRESHOLD = 1000000L,    # rows above which files stream in chunks",
    "  SAV_CHUNK_SIZE    = 1000000L,    # rows per chunk",
    "  RAMThreshold      = 30,          # GB, for PartitionBy = \"RAMEstimate\"",
    "  MaxCoerceNAPct    = 25,          # fail a file when coercion destroys more than this % of a column",
    "  SourceFingerprintMode = \"metadata\", # metadata | sha256 | none",
    "  n_workers         = max(1L, parallel::detectCores() - 1L),",
    "  DBName            = \"Repository.duckdb\",",
    "  DuckDB_GB         = \"8GB\"        # DuckDB memory limit (~75% of available RAM)",
    ")"), paths$ConfigPath)
  #### Empty MDT with the exact headers the preflight requires.             ####
  mdt_template <- data.frame(Database = character(), MDBDir = character(), Path = character(),
                             TableName = character(), FileType = character(),
                             PartitionKey = character(), PartitionValue = character(),
                             PartitionType = character(), PhysicalTableName = character(),
                             Encoding = character(), Delimiter = character(), Quote = character(),
                             NAStrings = character(), DecimalMark = character(),
                              DateFormat = character(), DateTimeFormat = character(), Timezone = character(),
                              MalformedRowPolicy = character(), ContinuationColumn = character(),
                              ContinuationJoin = character(), ReaderOptions = character(),
                              ReadMode = character(), AcceptPartial = logical())
  wb <- openxlsx::createWorkbook(); openxlsx::addWorksheet(wb, "Sheet1")
  openxlsx::writeData(wb, "Sheet1", mdt_template)
  openxlsx::saveWorkbook(wb, paths$MDTPath, overwrite = TRUE)
  rp <- RepositoryInitialize(FormattedDBPath = formatted, profile = profile)
  writeLines(c(
    "#### Repository loader -- run top to bottom, or step through interactively. ####",
    "#### All machine-specific settings live in repository_config.R.             ####",
    "library(data.table); library(openxlsx); library(DBI); library(duckdb)",
    "library(haven); library(arrow); library(glue); library(future); library(future.apply)",
    "",
    "source(\"<PATH TO DBFunctions>.R\")   # <- point at the workflow source file",
    sprintf("cfg <- load_repository_config(\"%s\")", norm(paths$ConfigPath)),
    "paths <- RepositoryInitialize(FormattedDBPath = cfg$FormattedDBPath)",
    "RunId <- new_repository_run_id()",
    "",
    "#### 1. Master Database Table: one row per source file ####",
    "MDT <- openxlsx::read.xlsx(cfg$MDTPath, sheet = \"Sheet1\")",
    "# scan_for_new_source_files(cfg$MasterDBPath, MDT)   # propose rows for new files",
    "ValidateMDTPreflight(MDT, strict = TRUE, ParquetBasePath = paths$ParquetBasePath,",
    "                     LogPath = paths$LogPath, RunId = RunId)",
    "",
    "#### 2. Survey sources and write the compact review workbook ####",
    "PrepareSchemaRegistry(MDT, MasterDBPath = cfg$MasterDBPath,",
    "                      ObservationPath = paths$SchemaObservationPath,",
    "                      SchemaReviewPath = paths$SchemaReviewPath,",
    "                      n_workers = cfg$n_workers,",
    "                      SchemaRegistryPath = paths$SchemaRegistryPath,",
    "                      LogPath = paths$LogPath, RunId = RunId)",
    "# Open StartHere. Complete ColumnDecisions and CompatibilityDecisions; PolicyReport is informational.",
    "FinalizeSchemaRegistry(SchemaReviewPath = paths$SchemaReviewPath,",
    "                       TableSchemaPath = paths$TableSchemaPath, strict = TRUE)",
    "",
    "#### 3. Load to hive-partitioned Parquet (checkpointed and resumable) ####",
    "run_result <- ParquetBackEndCreate(MDT = MDT, DBLoad = sort(unique(MDT$Database)),",
    "                                  MasterDBPath = cfg$MasterDBPath,",
    "                                  completed_checkpoint = load_checkpoint(paths$CheckpointPath),",
    "                                  CheckpointPath = paths$CheckpointPath,",
    "                                  ParquetBasePath = paths$ParquetBasePath,",
    "                                  LogPath = paths$LogPath, n_workers = cfg$n_workers,",
    "                                  PartitionBy = cfg$PartitionBy, RAMThreshold = cfg$RAMThreshold,",
    "                                  SAV_ROW_THRESHOLD = cfg$SAV_ROW_THRESHOLD,",
    "                                  SAV_CHUNK_SIZE = cfg$SAV_CHUNK_SIZE,",
    "                                  SchemaRegistryPath = paths$SchemaRegistryPath,",
    "                                  TableSchemaPath = paths$TableSchemaPath,",
    "                                  ManifestPath = paths$ManifestPath,",
    "                                  MaxCoerceNAPct = cfg$MaxCoerceNAPct,",
    "                                  SourceFingerprintMode = cfg$SourceFingerprintMode,",
    "                                  StopOnFileError = TRUE, ReturnRunResult = TRUE,",
    "                                  UseSchemaCatalog = TRUE,",
    "                                  RunId = RunId,",
    "                                  RunPreflight = FALSE)",
    "print(run_result)",
    "",
    "#### 4. Register DuckDB views and verify ####",
    "con <- open_duckdb(FormattedDBPath = cfg$FormattedDBPath, DBName = cfg$DBName,",
    "                   TempDirPath = file.path(cfg$FormattedDBPath, \"duckdb_temp\"),",
    "                   GB = cfg$DuckDB_GB, ReadOnly = FALSE)",
    "done <- MDT[checkpoint_completed_mask(MDT, load_checkpoint(paths$CheckpointPath)), ]",
    "register_parquet_view_compile(con, ParquetBasePath = paths$ParquetBasePath,",
    "                              tables_written = unique(repository_table_names(done)),",
    "                              SchemaRegistryPath = paths$SchemaRegistryPath,",
    "                              TableSchemaPath = paths$TableSchemaPath,",
    "                              LogPath = paths$LogPath, RunId = RunId)",
    "contract_results <- validate_data_contracts(con, paths$DataContractPath, strict = TRUE,",
    "                                           LogPath = paths$LogPath, RunId = RunId)",
    "",
    "#### 5. Reconcile the four sources of truth ####",
    "repo_audit <- audit_repository(MDT, paths$ParquetBasePath, paths$CheckpointPath,",
    "                               paths$ManifestPath, con = con,",
    "                               LogPath = paths$LogPath, RunId = RunId)",
    "repo_audit$issues",
    "# search_labels(\"<term>\", TableSchemaPath = paths$TableSchemaPath)   # data dictionary",
    "DBI::dbDisconnect(con, shutdown = TRUE)"), paths$RunnerPath)
  log_msg(sprintf("[SCAFFOLD] Repository project created in %s (profile: %s). Edit repository_config.R and DBSetup.xlsx, then run run_repository.R.", dir, profile))
  invisible(c(paths, rp))
}

#' Generate a runnable synthetic example repository
#'
#' Builds a complete toy project on top of \code{\link{create_repository_project}}:
#' a year-partitioned SALES database (csv, three years, with an identifier,
#' a code column, and a sampling weight), a site-partitioned SENSORS database
#' (csv, two sites), and a labelled Stata file to exercise the data dictionary
#' -- plus a filled-in DBSetup.xlsx. Everything the quickstart in the README
#' does runs against this.
#' @param dir Project directory.
#' @param seed RNG seed for reproducible fake data.
#' @return Invisibly, the scaffold paths (as from create_repository_project).
#' @export
generate_example_repository <- function(dir, seed = 1) {
  set.seed(seed)
  paths <- create_repository_project(dir, profile = "generic", overwrite = TRUE)
  src <- paths$MasterDBPath
  dir.create(file.path(src, "SALES"), showWarnings = FALSE)
  dir.create(file.path(src, "SENSORS"), showWarnings = FALSE)
  dir.create(file.path(src, "STUDY"), showWarnings = FALSE)
  rows <- list()
  for (yr in 2021:2023) {
    n <- 40
    data.table::fwrite(data.table::data.table(
      ORDER_ID = sprintf("O%04d%d", seq_len(n), yr - 2020),
      AMOUNT = round(stats::rlnorm(n, 4, 0.6), 2),
      REGION_CODE = sample(c("N01", "S02", "E03", "W04"), n, replace = TRUE),
      WEIGHT = round(stats::runif(n, 0.5, 3), 3)),
      file.path(src, "SALES", sprintf("sales_%d.csv", yr)))
    rows[[length(rows) + 1L]] <- data.frame(Database = "SALES", MDBDir = "SALES",
      Path = sprintf("sales_%d.csv", yr), TableName = "Orders", FileType = "csv",
      PartitionKey = "year", PartitionValue = as.character(yr), AcceptPartial = NA)
  }
  for (site in c("Alpha", "Beta")) {
    n <- 30
    data.table::fwrite(data.table::data.table(
      SENSOR_ID = sprintf("%s-%03d", toupper(substr(site, 1, 1)), seq_len(n)),
      READING = round(stats::rnorm(n, 20, 4), 2),
      STATUS_CODE = sample(c("OK", "WARN", "FAIL"), n, replace = TRUE, prob = c(.8, .15, .05))),
      file.path(src, "SENSORS", sprintf("sensors_%s.csv", tolower(site))))
    rows[[length(rows) + 1L]] <- data.frame(Database = "SENSORS", MDBDir = "SENSORS",
      Path = sprintf("sensors_%s.csv", tolower(site)), TableName = "Readings", FileType = "csv",
      PartitionKey = "SITE", PartitionValue = site, AcceptPartial = NA)
  }
  haven::write_dta(data.frame(
    SUBJECT_ID = haven::labelled(sprintf("S%03d", 1:20), label = "Study subject identifier"),
    ARM = haven::labelled(sample(1:2, 20, replace = TRUE),
                          labels = c(Treatment = 1, Control = 2), label = "Randomization arm"),
    OUTCOME = haven::labelled(round(stats::rnorm(20, 50, 10), 1), label = "Primary outcome score")),
    file.path(src, "STUDY", "study_2022.dta"))
  rows[[length(rows) + 1L]] <- data.frame(Database = "STUDY", MDBDir = "STUDY",
    Path = "study_2022.dta", TableName = "Subjects", FileType = "dta",
    PartitionKey = "year", PartitionValue = "2022", AcceptPartial = NA)
  mdt <- do.call(rbind, rows)
  wb <- openxlsx::createWorkbook(); openxlsx::addWorksheet(wb, "Sheet1")
  openxlsx::writeData(wb, "Sheet1", mdt)
  openxlsx::saveWorkbook(wb, paths$MDTPath, overwrite = TRUE)
  log_msg(sprintf("[EXAMPLE] Synthetic repository written to %s: %d source files across 3 databases (year, site, and labelled Stata).",
                  dir, nrow(mdt)))
  invisible(paths)
}

RepositoryInitialize <- function(FormattedDBPath, ParquetBasePath = file.path(FormattedDBPath, "parquet"),
                                 CheckpointPath = file.path(FormattedDBPath, "Checkpoints", "load_checkpoint.rds"),
                                 LogPath = file.path(FormattedDBPath, "Logs", "load_log.txt"),
                                 SchemaRegistryPath = file.path(FormattedDBPath, "Schema", "SchemaRegistry.xlsx"),
                                 TableSchemaPath = file.path(FormattedDBPath, "Schema", "TableSchemas.xlsx"),
                                 SchemaObservationPath = file.path(FormattedDBPath, "Schema", "SchemaObservations.parquet"),
                                 SchemaReviewPath = file.path(FormattedDBPath, "Schema", "SchemaReview.xlsx"),
                                 ManifestPath = NULL,
                                 DataContractPath = file.path(FormattedDBPath, "Schema", "DataContracts.xlsx"),
                                  create = TRUE, profile = c("generic", "hcup")) {
  profile <- match.arg(profile)
  if (is.null(ManifestPath)) {
    legacy_manifest <- file.path(FormattedDBPath, "Manifest", "ParquetManifest.csv")
    ManifestPath <- if (file.exists(legacy_manifest)) legacy_manifest else
      file.path(FormattedDBPath, "Manifest", "RepositoryMetadata.duckdb")
  }
  paths <- list(
    FormattedDBPath = FormattedDBPath,
    ParquetBasePath = ParquetBasePath,
    CheckpointPath = CheckpointPath,
    LogPath = LogPath,
    SchemaDir = dirname(SchemaRegistryPath),
    SchemaRegistryPath = SchemaRegistryPath,
    TableSchemaPath = TableSchemaPath,
    SchemaObservationPath = SchemaObservationPath,
    SchemaReviewPath = SchemaReviewPath,
    DataContractPath = DataContractPath,
    ManifestDir = dirname(ManifestPath),
    ManifestPath = ManifestPath,
    CheckpointDir = dirname(CheckpointPath),
    LogDir = dirname(LogPath)
  )
  if (isTRUE(create)) {
    dirs <- unique(unname(c(paths[c("FormattedDBPath", "ParquetBasePath", "SchemaDir", "ManifestDir", "CheckpointDir", "LogDir")])) )
    for (d in dirs) dir.create(d, recursive = TRUE, showWarnings = FALSE)
    if (!file.exists(SchemaRegistryPath)) {
      load_schema_registry(SchemaRegistryPath = SchemaRegistryPath, create_if_missing = TRUE, profile = profile)
    }
    if (!file.exists(DataContractPath)) load_data_contracts(DataContractPath, create_if_missing = TRUE)
  }
  paths
}

#' Validate MDT structure and output safety before schema discovery or loading
#'
#' This preflight deliberately does not open source files or execute reader
#' options. Those checks belong to \code{\link{SurveyRepositorySchema}}, which
#' records the detailed evidence and errors in the schema artifacts. Keeping
#' this function structural avoids reading every large/network source twice.
#' \code{MasterDBPath} remains accepted for backward compatibility but is not
#' inspected.
#' @export
ValidateMDTPreflight <- function(MDT, strict = TRUE, logStatus = TRUE,
                                 ParquetBasePath = NULL, MaxFileStemTruncate = TRUE,
                                 TerminalHivePartition = FALSE, MasterDBPath = NULL,
                                 LogPath = NULL, RunId = NULL) {
  previous_run <- if (!is.null(LogPath) || !is.null(RunId)) begin_repository_run(LogPath, RunId) else NULL
  if (!is.null(previous_run)) on.exit(restore_repository_run(previous_run), add = TRUE)
  #### Year is no longer required: identity and partitioning derive from   ####
  #### the partition spec (PartitionKey/PartitionValue, with Year as the   ####
  #### legacy fallback for blank YEAR-keyed rows).                         ####
  required <- c("Database", "MDBDir", "Path", "TableName", "FileType")
  missing_required <- setdiff(required, names(MDT))
  issues <- data.table::data.table(Check = character(), Severity = character(), Message = character(), N = integer())
  add_issue <- function(check, severity, message, n = 1L) {
    issues <<- data.table::rbindlist(list(issues, data.table::data.table(Check = check, Severity = severity, Message = message, N = as.integer(n))), fill = TRUE)
  }
  if (length(missing_required) > 0L) {
    add_issue("required_columns", "error", sprintf("MDT is missing required columns: %s", paste(missing_required, collapse = ", ")), length(missing_required))
  } else {
    MDTdt <- data.table::as.data.table(MDT)
    #### Required columns must also be non-blank per row: a blank Path or   ####
    #### MDBDir builds a garbage file path that fails much later and less   ####
    #### legibly than an error here.                                        ####
    for (rc in required) {
      vals <- MDTdt[[rc]]
      blank <- is.na(vals) | !nzchar(trimws(as.character(vals)))
      if (any(blank)) add_issue("blank_required_values", "error",
                                sprintf("Column %s has %d blank/NA value(s) (first at row %d).", rc, sum(blank), which(blank)[1]),
                                sum(blank))
    }
    physical_names <- repository_table_names(MDTdt)
    table_names <- unique(data.table::data.table(
      Database = as.character(MDTdt$Database), TableName = as.character(MDTdt$TableName),
      Raw = physical_names, Normalized = tolower(physical_names)))
    table_name_collisions <- table_names[, .(N = .N, Names = paste(sort(unique(Raw)), collapse = " | ")), by = Normalized][N > 1L]
    if (nrow(table_name_collisions) > 0L) {
      for (i in seq_len(nrow(table_name_collisions))) {
        add_issue("table_name_case_collision", "error",
                  sprintf("Database/TableName combinations differ only by case and resolve to the same repository/DuckDB table: %s",
                          table_name_collisions$Names[i]), table_name_collisions$N[i])
      }
    }
    unsafe_physical <- !grepl("^[A-Za-z][A-Za-z0-9_]*$", physical_names)
    if (any(unsafe_physical)) {
      add_issue("unsafe_physical_table_name", "error",
                sprintf("Physical table names must start with a letter and contain only letters, digits, and underscores. First invalid value: %s",
                        physical_names[which(unsafe_physical)[1]]), sum(unsafe_physical))
    }
    ambiguous_physical <- table_names[, .(LogicalTables = paste(sort(unique(paste(Database, TableName, sep = "/"))), collapse = " | "),
                                          NLogical = data.table::uniqueN(paste(Database, TableName, sep = "||"))),
                                      by = Normalized][NLogical > 1L]
    if (nrow(ambiguous_physical) > 0L) {
      for (i in seq_len(nrow(ambiguous_physical))) {
        add_issue("ambiguous_physical_table", "error",
                  sprintf("Different logical tables resolve to the same physical table: %s. Set distinct PhysicalTableName values in the MDT.",
                          ambiguous_physical$LogicalTables[i]), ambiguous_physical$NLogical[i])
      }
    }
    logical_physical <- table_names[, .(NPhysical = data.table::uniqueN(Normalized),
                                        Physical = paste(sort(unique(Raw)), collapse = " | ")),
                                    by = .(Database, TableName)][NPhysical > 1L]
    if (nrow(logical_physical) > 0L) {
      add_issue("mixed_physical_table_name", "error",
                "Rows for one Database/TableName must use one PhysicalTableName.", nrow(logical_physical))
    }
    bad_filetype <- !tolower(MDTdt$FileType) %in% supported_file_types()
    if (any(bad_filetype)) add_issue("bad_filetype", "error",
                                     sprintf("One or more MDT FileType values have no registered reader. Supported: %s.",
                                             paste(supported_file_types(), collapse = ", ")), sum(bad_filetype))
    #### AcceptPartial (optional): per-row acknowledgment that a file's     ####
    #### verified truncation is accepted; such rows checkpoint with a       ####
    #### warning instead of failing on row-count mismatch.                  ####
    if ("AcceptPartial" %in% names(MDTdt)) {
      ap_raw <- MDTdt$AcceptPartial
      ap_set <- !is.na(ap_raw) & nzchar(trimws(as.character(ap_raw)))
      ap <- suppressWarnings(as.logical(ap_raw))
      bad_ap <- ap_set & is.na(ap)
      if (any(bad_ap)) add_issue("bad_accept_partial", "error",
                                 "One or more AcceptPartial values are not blank/TRUE/FALSE.", sum(bad_ap))
      n_ap <- sum(ap %in% TRUE)
      if (n_ap > 0L) add_issue("accept_partial_rows", "warning",
                               sprintf("%d row(s) set AcceptPartial=TRUE; truncated loads for these files checkpoint with a [WARN] instead of failing.", n_ap), n_ap)
    }
    #### Hive partition spec checks. Every row must resolve to a valid      ####
    #### spec; blank PartitionKey/PartitionValue fall back to YEAR + the    ####
    #### legacy Year column when present.                                   ####
    specs <- vector("list", nrow(MDTdt))
    partition_types <- vector("list", nrow(MDTdt))
    for (i in seq_len(nrow(MDTdt))) {
      specs[[i]] <- tryCatch(partition_spec_for_row(MDTdt[i, ]), error = function(e) e)
      if (inherits(specs[[i]], "error")) {
        add_issue("bad_partition_spec", "error", sprintf("Row %d (%s): %s", i, MDTdt$Path[i], conditionMessage(specs[[i]])))
      } else {
        partition_types[[i]] <- tryCatch(partition_types_for_row(MDTdt[i, ], specs[[i]]), error = function(e) e)
        if (inherits(partition_types[[i]], "error")) {
          add_issue("bad_partition_type", "error", sprintf("Row %d (%s): %s", i, MDTdt$Path[i], conditionMessage(partition_types[[i]])))
        }
      }
    }
    ok <- !vapply(specs, inherits, logical(1), what = "error") &
      !vapply(partition_types, inherits, logical(1), what = "error")
    if (all(ok)) {
      #### Identity check uses the same key the checkpoint uses.            ####
      rk <- repository_checkpoint_key(MDTdt)
      dup_key <- sum(duplicated(rk) | duplicated(rk, fromLast = TRUE))
      if (dup_key > 0L) add_issue("duplicate_repository_key", "error", "Duplicate repository checkpoint identities found.", dup_key)
    }
    #### YEAR-keyed partition values must be whole years, or hive dirs and  ####
    #### YEAR-typed queries break. Note as.integer("2019.5") silently       ####
    #### truncates, so require an all-digits string, not mere coercibility. ####
    bad_year <- sum(vapply(specs[ok], function(s) {
      "YEAR" %in% s$keys && !grepl("^[0-9]+$", s$values[match("YEAR", s$keys)])
    }, logical(1)))
    if (bad_year > 0L) add_issue("bad_year", "error", "One or more YEAR partition values are not whole numbers.", bad_year)
    if (any(ok)) {
      spec_dt <- data.table::data.table(Database = MDTdt$Database[ok], TableName = MDTdt$TableName[ok],
                                        PhysicalTable = physical_names[ok],
                                        KeySet = vapply(specs[ok], function(s) paste(s$keys, collapse = ";"), character(1)),
                                        TypeSet = vapply(partition_types[ok], paste, collapse = ";", character(1)),
                                        Dir = vapply(specs[ok], function(s) s$dir, character(1)),
                                        RawValue = vapply(specs[ok], function(s) paste(s$values, collapse = ";"), character(1)),
                                        Path = MDTdt$Path[ok],
                                        FileType = tolower(MDTdt$FileType[ok]))
      spec_dt[, DirKey := tolower(Dir)]
      if (isTRUE(TerminalHivePartition) && any(vapply(specs[ok], function(s) "BATCH_ID" %in% s$keys, logical(1)))) {
        add_issue("reserved_partition_key", "error",
                  "PartitionKey BATCH_ID is reserved when TerminalHivePartition=TRUE because the chunk writer creates batch_id= directories.",
                  sum(vapply(specs[ok], function(s) "BATCH_ID" %in% s$keys, logical(1))))
      }
      mixed_keys <- spec_dt[, .(n_keysets = data.table::uniqueN(KeySet), keysets = paste(sort(unique(KeySet)), collapse = " vs ")), by = .(Database, TableName)][n_keysets > 1L]
      if (nrow(mixed_keys) > 0L) {
        for (i in seq_len(nrow(mixed_keys))) add_issue("mixed_partition_keys", "error",
                                                       sprintf("%s/%s declares multiple partition key sets (%s); a table's directory tree must use one.",
                                                               mixed_keys$Database[i], mixed_keys$TableName[i], mixed_keys$keysets[i]))
      }
      mixed_types <- spec_dt[, .(n_typesets = data.table::uniqueN(TypeSet),
                                 typesets = paste(sort(unique(TypeSet)), collapse = " vs ")),
                             by = .(Database, TableName)][n_typesets > 1L]
      if (nrow(mixed_types) > 0L) {
        for (i in seq_len(nrow(mixed_types))) add_issue("mixed_partition_types", "error",
          sprintf("%s/%s declares multiple partition type sets (%s); use one type per partition key across a table.",
                  mixed_types$Database[i], mixed_types$TableName[i], mixed_types$typesets[i]))
      }
      spec_dt[, OutputStem := mapply(function(db, tbl, dir, path) {
        partition_dir <- if (!is.null(ParquetBasePath) && nzchar(ParquetBasePath)) {
          file.path(ParquetBasePath, spec_dt[Database == db & TableName == tbl & Dir == dir]$PhysicalTable[1], dir)
        } else {
          NULL
        }
        tolower(parquet_output_stem(path, partition_dir = partition_dir,
                                    MaxFileStemTruncate = MaxFileStemTruncate))
      }, Database, TableName, Dir, Path, USE.NAMES = FALSE)]
      stem_collisions <- spec_dt[, .(N = .N, Dirs = paste(sort(unique(Dir)), collapse = " | "),
                                             Paths = paste(sort(unique(Path)), collapse = " | ")),
                                 by = .(PhysicalTable, DirKey, OutputStem)][N > 1L]
      if (nrow(stem_collisions) > 0L) {
        for (i in seq_len(nrow(stem_collisions))) add_issue("output_filename_collision", "error",
                                                            sprintf("%s partition '%s' has %d source files that resolve to output stem '%s': %s",
                                                                    stem_collisions$PhysicalTable[i],
                                                                    stem_collisions$Dirs[i], stem_collisions$N[i],
                                                                    stem_collisions$OutputStem[i], stem_collisions$Paths[i]))
      }
      spec_dt[, ChunkStem := mapply(function(db, tbl, dir, path) {
        partition_dir <- if (!is.null(ParquetBasePath) && nzchar(ParquetBasePath)) {
          file.path(ParquetBasePath, spec_dt[Database == db & TableName == tbl & Dir == dir]$PhysicalTable[1], dir)
        } else {
          NULL
        }
        tolower(parquet_chunk_stem(path, partition_dir = partition_dir,
                                   TerminalHivePartition = TerminalHivePartition,
                                   MaxFileStemTruncate = MaxFileStemTruncate))
      }, Database, TableName, Dir, Path, USE.NAMES = FALSE)]
      chunkable_types <- supported_file_types()[vapply(supported_file_types(), function(tp) isTRUE(get_file_reader(tp)$chunkable), logical(1))]
      chunk_collisions <- spec_dt[FileType %in% chunkable_types,
                                  .(N = .N, Dirs = paste(sort(unique(Dir)), collapse = " | "),
                                    Paths = paste(sort(unique(Path)), collapse = " | ")),
                                  by = .(PhysicalTable, DirKey, ChunkStem)][N > 1L]
      if (nrow(chunk_collisions) > 0L) {
        for (i in seq_len(nrow(chunk_collisions))) add_issue("chunk_filename_collision", "error",
                                                             sprintf("%s partition '%s' has %d chunkable files that resolve to chunk stem '%s': %s",
                                                                     chunk_collisions$PhysicalTable[i],
                                                                     chunk_collisions$Dirs[i], chunk_collisions$N[i],
                                                                     chunk_collisions$ChunkStem[i], chunk_collisions$Paths[i]))
      }
      #### Distinct raw PartitionValues that sanitize to the same directory ####
      #### would silently merge partitions.                                 ####
      raw_vals <- if ("PartitionValue" %in% names(MDTdt)) as.character(MDTdt$PartitionValue)[ok] else rep(NA_character_, sum(ok))
      collide <- data.table::data.table(Database = spec_dt$Database, TableName = spec_dt$TableName,
                                        Dir = spec_dt$Dir, DirKey = spec_dt$DirKey,
                                        Raw = ifelse(is.na(raw_vals) | !nzchar(trimws(raw_vals)), spec_dt$RawValue, trimws(raw_vals)))
      coll <- collide[, .(n_raw = data.table::uniqueN(Raw), Dirs = paste(sort(unique(Dir)), collapse = " | ")),
                      by = .(Database, TableName, DirKey)][n_raw > 1L]
      if (nrow(coll) > 0L) {
        for (i in seq_len(nrow(coll))) add_issue("partition_value_collision", "error",
                                                 sprintf("%s/%s: %d distinct PartitionValue(s) sanitize to the same directory '%s'.",
                                                         coll$Database[i], coll$TableName[i], coll$n_raw[i], coll$Dirs[i]))
      }
    }
  }
  if (nrow(issues) > 0L && isTRUE(logStatus)) {
    for (i in seq_len(nrow(issues))) log_msg(sprintf("[PREFLIGHT %s] %s: %s (n=%s)", toupper(issues$Severity[i]), issues$Check[i], issues$Message[i], issues$N[i]))
  }
  if (isTRUE(strict) && any(issues$Severity == "error")) stop("MDT preflight validation failed. Review logged [PREFLIGHT ERROR] messages.")
  invisible(issues)
}

################################################################################
#### User-guided schema discovery workflow ####################################
################################################################################

.classify_schema_reader_warnings <- function(warnings, rows_sampled = NA_integer_) {
  warnings <- unique(as.character(warnings))
  warnings <- warnings[!is.na(warnings) & nzchar(trimws(warnings))]
  if (length(warnings) == 0L) {
    return(list(Text = NA_character_, Class = NA_character_, Severity = NA_character_))
  }
  if (all(startsWith(warnings, "[READER REPAIR]"))) {
    return(list(Text = paste(warnings, collapse = " | "),
                Class = "continuation_repaired", Severity = "info"))
  }
  if (any(grepl("Stopped early on line [0-9]+\\. Expected [0-9]+ fields", warnings))) {
    return(list(Text = paste(warnings, collapse = " | "),
                Class = "structural_mismatch", Severity = "warning"))
  }
  list(Text = paste(warnings, collapse = " | "),
       Class = "reader_warning", Severity = "warning")
}

.schema_observation_issue_row <- function(row_meta, full_path, message, encoding_info = NULL) {
  pspec <- tryCatch(partition_spec_for_row(row_meta), error = function(e) list(keys = NA_character_, values = NA_character_))
  data.table::data.table(
    Database = as.character(row_meta$Database[1]),
    TableName = as.character(row_meta$TableName[1]),
    DuckDBTable = repository_table_name_for_row(row_meta),
    SourcePath = normalizePath(full_path, winslash = "/", mustWork = FALSE),
    FileType = tolower(as.character(row_meta$FileType[1])),
    PartitionKey = paste(pspec$keys, collapse = ";"),
    PartitionValue = paste(pspec$values, collapse = ";"),
    ObservationKind = "source_error",
    Column = NA_character_, OriginalColumn = NA_character_,
    ObservedType = NA_character_, IsPartitionColumn = FALSE,
    RowsSampled = NA_real_, NonMissingCount = NA_real_, MissingPercent = NA_real_,
    IntegerLike = NA, FractionalCount = NA_real_, LeadingZeroCount = NA_real_,
    NumericParseFailureCount = NA_real_, Minimum = NA_character_, Maximum = NA_character_,
    MaximumTextLength = NA_real_, PrecisionRisk = NA,
    InferenceConfidence = "unavailable", ReaderWarning = NA_character_,
    ReaderWarningClass = NA_character_, ReaderWarningSeverity = NA_character_,
    ReaderRepairCount = NA_real_, ReaderRepairLines = NA_character_,
    ReaderRepairPolicy = NA_character_,
    SurveyStatus = "error", SurveyMessage = as.character(message),
    SourceSize = suppressWarnings(as.numeric(file.info(full_path)$size[1])),
    SourceModifiedUTC = NA_character_, SourceFingerprint = NA_character_,
    DeclaredEncoding = encoding_info$DeclaredEncoding %||% NA_character_,
    DetectedEncoding = encoding_info$DetectedEncoding %||% NA_character_,
    EncodingConfidence = encoding_info$EncodingConfidence %||% NA_real_,
    EncodingUsed = encoding_info$EncodingUsed %||% NA_character_,
    EncodingDetectionMethod = encoding_info$DetectionMethod %||% NA_character_,
    EncodingValidationStatus = "error"
  )
}

.schema_observation_stats <- function(x) {
  type <- if (inherits(x, "integer64")) "int64" else normalize_type_name(class(x)[1])
  present <- !is.na(x)
  n_present <- sum(present)
  n_total <- length(x)
  fractional <- NA_real_
  integer_like <- NA
  precision_risk <- FALSE
  minimum <- maximum <- NA_character_
  leading_zero <- numeric_parse_failures <- 0
  max_text <- NA_real_

  if (n_present > 0L && type %in% c("logical", "integer", "int64", "numeric")) {
    values <- suppressWarnings(as.numeric(x[present]))
    finite <- values[is.finite(values)]
    if (length(finite) > 0L) {
      fractional <- if (type == "numeric") sum(abs(finite - round(finite)) > 1e-9) else 0
      integer_like <- isTRUE(fractional == 0)
      precision_risk <- any(abs(finite) > 2^53)
      minimum <- format(min(finite), scientific = FALSE, trim = TRUE, digits = 22)
      maximum <- format(max(finite), scientific = FALSE, trim = TRUE, digits = 22)
    }
  } else if (n_present > 0L && type %in% c("Date", "POSIXct", "time", "duration")) {
    minimum <- as.character(min(x[present], na.rm = TRUE))[1]
    maximum <- as.character(max(x[present], na.rm = TRUE))[1]
  } else if (n_present > 0L && type == "character") {
    values <- trimws(as.character(x[present]))
    values <- values[nzchar(values)]
    if (length(values) > 0L) {
      leading_zero <- sum(grepl("^[+-]?0[0-9]+$", values))
      parsed <- suppressWarnings(as.numeric(values))
      numeric_parse_failures <- sum(is.na(parsed))
      max_text <- max(nchar(values, type = "chars"))
    }
  }

  list(
    ObservedType = type,
    RowsSampled = as.numeric(n_total),
    NonMissingCount = as.numeric(n_present),
    MissingPercent = if (n_total == 0L) NA_real_ else 100 * (n_total - n_present) / n_total,
    IntegerLike = integer_like,
    FractionalCount = fractional,
    LeadingZeroCount = as.numeric(leading_zero),
    NumericParseFailureCount = as.numeric(numeric_parse_failures),
    Minimum = minimum,
    Maximum = maximum,
    MaximumTextLength = max_text,
    PrecisionRisk = precision_risk
  )
}

.survey_schema_source <- function(row_meta, MasterDBPath, SourceFingerprintMode = "metadata") {
  full_path <- source_path_for_row(row_meta, MasterDBPath)
  encoding_info <- NULL
  warnings_seen <- character(0)
  capture_warnings <- function(expr) {
    withCallingHandlers(expr, warning = function(w) {
      warnings_seen <<- unique(c(warnings_seen, conditionMessage(w)))
      invokeRestart("muffleWarning")
    })
  }
  tryCatch({
    reader_type <- tolower(as.character(row_meta$FileType[1]))
    rd <- get_file_reader(reader_type)
    options <- reader_options_for_row(row_meta)
    if (reader_type %in% c("csv", "tsv", "txt", "gz")) {
      options <- .resolve_delimited_reader_options(full_path, options)
      encoding_info <- options$.EncodingInfo
    }
    original_header <- capture_warnings(call_reader(rd, "read_header", full_path,
                                                     reader_options = options))
    header_encoding_info <- attr(original_header, "repoquet_encoding_info")
    if (!is.null(header_encoding_info)) encoding_info <- header_encoding_info
    sample <- capture_warnings(call_reader(rd, "read_sample", full_path,
                                           reader_options = options))
    sample_encoding_info <- attr(sample, "repoquet_encoding_info")
    if (!is.null(sample_encoding_info)) encoding_info <- sample_encoding_info
    repair_info <- attr(sample, "repoquet_delimited_diagnostics")
    if (!is.null(repair_info) && isTRUE(repair_info$RepairCount > 0L)) {
      warnings_seen <- unique(c(
        warnings_seen,
        sprintf("[READER REPAIR] Appended %d continuation line(s) to %s at physical line(s): %s",
                as.integer(repair_info$RepairCount), repair_info$ContinuationColumn,
                paste(repair_info$RepairLines, collapse = ","))
      ))
    }
    sample <- strip_haven(sample)
    data.table::setDT(sample)
    warning_info <- .classify_schema_reader_warnings(warnings_seen, nrow(sample))
    pspec <- partition_spec_for_row(row_meta)
    ptypes <- partition_types_for_row(row_meta)
    fingerprint <- source_fingerprint(full_path, mode = SourceFingerprintMode)
    original_header <- as.character(original_header)
    header_map <- split(original_header, canonical_colnames(original_header))
    confidence <- if (reader_type %in% c("csv", "tsv", "txt", "gz")) "sampled" else "declared_or_stored"
    warning_text <- warning_info$Text
    base <- list(
      Database = as.character(row_meta$Database[1]),
      TableName = as.character(row_meta$TableName[1]),
      DuckDBTable = repository_table_name_for_row(row_meta),
      SourcePath = normalizePath(full_path, winslash = "/", mustWork = FALSE),
      FileType = reader_type,
      PartitionKey = paste(pspec$keys, collapse = ";"),
      PartitionValue = paste(pspec$values, collapse = ";"),
      SourceSize = fingerprint$size,
      SourceModifiedUTC = fingerprint$mtime_utc,
      SourceFingerprint = fingerprint$fingerprint,
      DeclaredEncoding = encoding_info$DeclaredEncoding %||% NA_character_,
      DetectedEncoding = encoding_info$DetectedEncoding %||% NA_character_,
      EncodingConfidence = encoding_info$EncodingConfidence %||% NA_real_,
      EncodingUsed = encoding_info$EncodingUsed %||% NA_character_,
      EncodingDetectionMethod = encoding_info$DetectionMethod %||% NA_character_,
      EncodingValidationStatus = if (is.null(encoding_info)) NA_character_ else "sample_valid_utf8",
      ReaderRepairCount = repair_info$RepairCount %||% 0,
      ReaderRepairLines = if (is.null(repair_info) || length(repair_info$RepairLines) == 0L) NA_character_ else
        paste(repair_info$RepairLines, collapse = ","),
      ReaderRepairPolicy = repair_info$RepairPolicy %||% "error"
    )
    rows <- lapply(names(sample), function(column) {
      stats <- .schema_observation_stats(sample[[column]])
      original <- header_map[[column]]
      data.table::as.data.table(c(base, list(
        ObservationKind = "source_column",
        Column = column,
        OriginalColumn = if (is.null(original)) column else paste(unique(original), collapse = " | "),
        IsPartitionColumn = column %in% canonical_colnames(pspec$keys),
        InferenceConfidence = confidence,
        ReaderWarning = warning_text,
        ReaderWarningClass = warning_info$Class,
        ReaderWarningSeverity = warning_info$Severity,
        SurveyStatus = "ok",
        SurveyMessage = NA_character_
      ), stats))
    })
    #### Hive partition columns are virtual schema members even when the     ####
    #### physical source column is absent or intentionally removed.          ####
    for (j in seq_along(pspec$keys)) {
      rows[[length(rows) + 1L]] <- data.table::as.data.table(c(base, list(
        ObservationKind = "hive_partition",
        Column = canonical_colnames(pspec$keys[j]), OriginalColumn = NA_character_,
        ObservedType = normalize_type_name(ptypes[j]), IsPartitionColumn = TRUE,
        RowsSampled = as.numeric(nrow(sample)), NonMissingCount = as.numeric(nrow(sample)),
        MissingPercent = 0, IntegerLike = normalize_type_name(ptypes[j]) %in% c("integer", "int64"),
        FractionalCount = 0, LeadingZeroCount = 0, NumericParseFailureCount = 0,
        Minimum = NA_character_, Maximum = NA_character_, MaximumTextLength = NA_real_,
        PrecisionRisk = FALSE, InferenceConfidence = "configured_partition",
        ReaderWarning = warning_text, ReaderWarningClass = warning_info$Class,
        ReaderWarningSeverity = warning_info$Severity,
        SurveyStatus = "ok", SurveyMessage = NA_character_
      )))
    }
    list(ok = TRUE, path = full_path,
         data = data.table::rbindlist(rows, fill = TRUE),
         error = NA_character_)
  }, error = function(e) {
    list(ok = FALSE, path = full_path,
         data = .schema_observation_issue_row(row_meta, full_path, conditionMessage(e), encoding_info),
         error = conditionMessage(e))
  })
}

#' Survey source schemas without applying domain policies
#'
#' Reads every selected source through its registered reader, records one
#' metadata row per source column and virtual partition column, and stores the
#' detailed evidence as Parquet. No registry override or cross-table name rule
#' is applied at this stage.
#' @export
SurveyRepositorySchema <- function(MDT, MasterDBPath, ObservationPath,
                                   DBLoad = NULL, n_workers = 1,
                                   SourceFingerprintMode = c("metadata", "sha256", "none"),
                                   StrictReaders = FALSE, LogPath = NULL, RunId = NULL) {
  SourceFingerprintMode <- match.arg(SourceFingerprintMode)
  if (length(ObservationPath) != 1L || is.na(ObservationPath) || !nzchar(trimws(ObservationPath))) {
    stop("ObservationPath must be one non-empty Parquet file path.")
  }
  if (tolower(tools::file_ext(ObservationPath)) != "parquet") {
    stop("ObservationPath must end in .parquet.")
  }
  previous_run <- if (!is.null(LogPath) || !is.null(RunId)) begin_repository_run(LogPath, RunId) else NULL
  if (!is.null(previous_run)) on.exit(restore_repository_run(previous_run), add = TRUE)
  required <- c("Database", "MDBDir", "Path", "TableName", "FileType")
  missing <- setdiff(required, names(MDT))
  if (length(missing) > 0L) stop("Schema survey requires MDT columns: ", paste(missing, collapse = ", "))
  MDTdt <- data.table::as.data.table(MDT)
  if (!is.null(DBLoad)) MDTdt <- MDTdt[as.character(Database) %in% as.character(DBLoad)]
  if (nrow(MDTdt) == 0L) stop("Schema survey has no MDT rows to inspect.")
  scan_one <- function(i) {
    #### Multisession workers do not inherit callbacks stored inside the    ####
    #### main process's reader-registry environment when repoquet.R was     ####
    #### sourced directly. Re-registering is cheap and makes both installed ####
    #### package and development/source execution deterministic.            ####
    register_builtin_file_readers()
    .survey_schema_source(MDTdt[i, ], MasterDBPath, SourceFingerprintMode)
  }
  results <- .parallel_scan_with_serial_retry(
    seq_len(nrow(MDTdt)), scan_one, n_workers = n_workers,
    future_packages = c("data.table", "haven", "arrow", "bit64", "openxlsx", "stringi"),
    is_failure = function(x) !is.list(x) || !isTRUE(x$ok),
    context = "repository schema survey"
  )
  observations <- data.table::rbindlist(lapply(results, `[[`, "data"), fill = TRUE)
  dir.create(dirname(ObservationPath), recursive = TRUE, showWarnings = FALSE)
  write_arrow_table_safely(arrow::as_arrow_table(observations), ObservationPath)
  failed <- vapply(results, function(x) !isTRUE(x$ok), logical(1))
  n_warnings <- sum(!is.na(observations$ReaderWarning) & nzchar(observations$ReaderWarning))
  summary <- data.table::data.table(
    Sources = nrow(MDTdt), ObservationRows = nrow(observations),
    Columns = data.table::uniqueN(observations[SurveyStatus == "ok"]$Column, na.rm = TRUE),
    FailedSources = sum(failed), WarningRows = n_warnings,
    ObservationPath = normalizePath(ObservationPath, winslash = "/", mustWork = FALSE)
  )
  log_msg(sprintf("[SCHEMA SURVEY] %d source(s), %d observation row(s), %d failed source(s); wrote %s",
                  summary$Sources, summary$ObservationRows, summary$FailedSources, ObservationPath))
  out <- structure(list(observations = observations, summary = summary,
                        ObservationPath = ObservationPath), class = "RepositorySchemaSurvey")
  if (isTRUE(StrictReaders) && any(failed)) {
    stop(sprintf("Schema survey failed for %d source file(s). Detailed error rows were written to %s.",
                 sum(failed), ObservationPath))
  }
  out
}

#' Retrieve detailed schema observations from the internal Parquet store
#' @export
GetSchemaObservations <- function(ObservationPath, Database = NULL, TableName = NULL,
                                  Column = NULL, IssuesOnly = FALSE, Limit = NULL) {
  if (!file.exists(ObservationPath)) stop("Schema observation Parquet file not found: ", ObservationPath)
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  sql <- sprintf("SELECT * FROM read_parquet(%s)",
                 quote_duckdb_string(normalizePath(ObservationPath, winslash = "/", mustWork = TRUE)))
  where <- character(0)
  if (!is.null(Database)) where <- c(where, sprintf("%s = %s", quote_duckdb_ident("Database"), quote_duckdb_string(Database[1])))
  if (!is.null(TableName)) where <- c(where, sprintf("%s = %s", quote_duckdb_ident("TableName"), quote_duckdb_string(TableName[1])))
  if (!is.null(Column)) where <- c(where, sprintf("%s = %s", quote_duckdb_ident("Column"), quote_duckdb_string(canonical_colnames(Column[1]))))
  if (isTRUE(IssuesOnly)) {
    where <- c(where, sprintf("(%s <> 'ok' OR COALESCE(%s, '') <> '')",
                              quote_duckdb_ident("SurveyStatus"), quote_duckdb_ident("ReaderWarning")))
  }
  if (length(where) > 0L) sql <- paste(sql, "WHERE", paste(where, collapse = " AND "))
  order_columns <- vapply(c("Database", "TableName", "Column", "PartitionValue", "SourcePath"),
                          quote_duckdb_ident, character(1))
  sql <- paste(sql, "ORDER BY", paste(order_columns, collapse = ", "))
  if (!is.null(Limit)) {
    limit <- suppressWarnings(as.integer(Limit[1]))
    if (is.na(limit) || limit < 1L) stop("Limit must be a positive integer.")
    sql <- paste(sql, "LIMIT", limit)
  }
  data.table::as.data.table(DBI::dbGetQuery(con, sql))
}

.schema_type_history <- function(rows) {
  h <- unique(rows[, .(PartitionKey, PartitionValue, ObservedType, InferenceConfidence)])
  numeric_partition <- suppressWarnings(as.numeric(h$PartitionValue))
  if (all(!is.na(numeric_partition))) h <- h[order(numeric_partition, ObservedType)] else h <- h[order(PartitionValue, ObservedType)]
  paste(sprintf("%s=%s: %s", h$PartitionKey, h$PartitionValue, h$ObservedType), collapse = "; ")
}

.schema_recommendation_for_group <- function(rows) {
  types <- sort(unique(rows$ObservedType[!is.na(rows$ObservedType) & rows$ObservedType != "unknown"]))
  warning_severity <- if ("ReaderWarningSeverity" %in% names(rows)) {
    tolower(trimws(as.character(rows$ReaderWarningSeverity)))
  } else {
    ifelse(!is.na(rows$ReaderWarning) & nzchar(rows$ReaderWarning), "warning", NA_character_)
  }
  has_warning <- any(warning_severity %in% c("warning", "error"), na.rm = TRUE)
  risk <- "Lossless"
  if (length(types) == 0L) {
    recommended <- "character"; risk <- "Review"
    reason <- "No non-missing type evidence was available."
  } else if (length(types) == 1L) {
    recommended <- types[1]
    reason <- sprintf("Observed consistently as %s.", recommended)
  } else if (all(types %in% c("integer", "int64"))) {
    recommended <- "int64"
    reason <- "Integer widths differ; int64 preserves every observed integer."
  } else if (all(types %in% c("logical", "integer"))) {
    recommended <- "integer"
    reason <- "Logical and integer values can be represented without value loss as integer."
  } else if (all(types %in% c("logical", "integer", "int64", "numeric"))) {
    precision_risk <- any(rows$PrecisionRisk %in% TRUE, na.rm = TRUE)
    fractional <- sum(rows$FractionalCount, na.rm = TRUE)
    if (fractional == 0 && "int64" %in% types && !precision_risk) {
      recommended <- "int64"
      reason <- "All sampled numeric values are integral; int64 avoids floating-point identifiers."
    } else if (!precision_risk) {
      recommended <- "numeric"
      reason <- "Numeric storage safely accommodates the observed integer and fractional values."
    } else {
      recommended <- "character"
      reason <- "Observed magnitudes can exceed exact floating-point integer precision."
      risk <- "Review"
    }
  } else if ("character" %in% types) {
    recommended <- "character"
    reason <- "Character storage preserves mixed textual and numeric representations."
  } else if (all(types %in% c("Date", "POSIXct"))) {
    recommended <- "POSIXct"
    reason <- "Datetime storage preserves both dates and timestamps."
  } else {
    recommended <- "character"
    reason <- sprintf("No automatic lossless promotion exists for: %s.", paste(types, collapse = ", "))
    risk <- "Potential loss"
  }
  if (has_warning && risk == "Lossless") risk <- "Review"
  if (has_warning) reason <- paste(reason, "Reader warnings require confirmation.")
  list(RecommendedType = normalize_type_name(recommended), Risk = risk,
       Reason = reason, RequiresReview = risk != "Lossless")
}

.schema_policy_resolution <- function(rows, recommendation, policy_hit = NULL) {
  data_type <- normalize_type_name(recommendation$RecommendedType)
  if (is.null(policy_hit) || nrow(policy_hit) == 0L) {
    return(list(
      DataRecommendedType = data_type, RecommendedType = data_type,
      Risk = recommendation$Risk, Reason = recommendation$Reason,
      RequiresReview = recommendation$RequiresReview, Role = NULL,
      PolicyPattern = NA_character_, PolicyType = NA_character_,
      PolicyRole = NA_character_, PolicyStatus = "not_configured",
      PolicyConflict = FALSE
    ))
  }
  policy_type <- normalize_type_name(policy_hit$CanonicalType[1])
  observed <- unique(rows$ObservedType[!is.na(rows$ObservedType) & rows$ObservedType != "unknown"])
  safe_policy <- identical(promote_types(c(observed, policy_type)), policy_type)
  conflict <- !identical(policy_type, data_type)
  status <- if (!conflict) {
    "matched"
  } else if (safe_policy) {
    "lossless_policy_promotion"
  } else {
    "explicit_override_required"
  }
  final_type <- if (safe_policy) policy_type else data_type
  risk <- recommendation$Risk
  reason <- recommendation$Reason
  if (conflict) {
    risk <- recommendation$Risk
    reason <- paste0(reason, " Policy ", as.character(policy_hit$ColumnPattern[1]),
                     " proposes ", policy_type, "; ",
                     if (safe_policy) "the lossless policy promotion was applied automatically and is recorded in PolicyReport."
                     else "the observed evidence does not support that coercion, so the data-derived type was retained and the conflict is recorded in PolicyReport.")
  }
  list(
    DataRecommendedType = data_type, RecommendedType = final_type,
    Risk = risk, Reason = reason,
    RequiresReview = isTRUE(recommendation$RequiresReview),
    Role = if ("Role" %in% names(policy_hit)) as.character(policy_hit$Role[1]) else NULL,
    PolicyPattern = as.character(policy_hit$ColumnPattern[1]),
    PolicyType = policy_type,
    PolicyRole = if ("Role" %in% names(policy_hit)) as.character(policy_hit$Role[1]) else NA_character_,
    PolicyStatus = status, PolicyConflict = conflict
  )
}

.schema_compatibility_review <- function(registry) {
  registry <- data.table::as.data.table(registry)
  empty <- data.table::data.table(
    Scope = character(), Database = character(), Column = character(),
    MergeGroup = character(), Tables = character(), Databases = character(),
    CurrentTypes = character(), RecommendedCommonType = character(),
    ApprovedCommonType = character(), SuggestedRole = character(),
    RecommendationReason = character(), Decision = character(),
    UserNotes = character(), CompatibilitySignature = character())
  build_row <- function(rows, scope, database) {
    types <- sort(unique(vapply(rows$RecommendedType, normalize_type_name, character(1))))
    if (length(types) <= 1L || data.table::uniqueN(rows$DuckDBTable) <= 1L) return(NULL)
    column <- rows$Column[1]
    common <- promote_types(types, column)
    suggested_role <- if (any(tolower(rows$Role) == "join_key", na.rm = TRUE) ||
                          isTRUE(merge_key_name_candidate(column))) "join_key" else "compatible_column"
    group <- if (scope == "cross_database") paste("ALL", column, sep = "::") else
      paste(database, column, sep = "::")
    signature <- paste("compatibility_v1", scope, database, column,
                       paste(sort(paste(rows$DuckDBTable, rows$RecommendedType, sep = "=")), collapse = "|"),
                       sep = "||")
    data.table::data.table(
      Scope = scope, Database = database, Column = column, MergeGroup = group,
      Tables = paste(sort(unique(rows$DuckDBTable)), collapse = "; "),
      Databases = paste(sort(unique(rows$Database)), collapse = "; "),
      CurrentTypes = paste(types, collapse = ","),
      RecommendedCommonType = common, ApprovedCommonType = common,
      SuggestedRole = suggested_role,
      RecommendationReason = sprintf("%s preserves the compatible values represented by: %s.",
                                     common, paste(types, collapse = ", ")),
      Decision = "", UserNotes = "",
      CompatibilitySignature = digest::digest(signature, algo = "sha256", serialize = FALSE)
    )
  }
  within <- lapply(split(registry, paste(registry$Database, registry$Column, sep = "\r")),
                   function(rows) build_row(rows, "within_database", rows$Database[1]))
  cross <- lapply(split(registry, registry$Column), function(rows) {
    if (data.table::uniqueN(rows$Database) <= 1L) return(NULL)
    build_row(rows, "cross_database", "ALL")
  })
  out <- data.table::rbindlist(c(within, cross), fill = TRUE)
  if (nrow(out) == 0L) return(empty)
  data.table::setorder(out, Scope, Database, Column)
  out
}

#' Recommend canonical table schemas from observed evidence
#' @export
RecommendRepositorySchema <- function(survey = NULL, ObservationPath = NULL,
                                      SchemaRegistryPath = NULL, schema_registry = NULL,
                                      SchemaProfile = c("none", "generic", "hcup")) {
  SchemaProfile <- match.arg(SchemaProfile)
  if (is.null(schema_registry) && !is.null(SchemaRegistryPath) && nzchar(SchemaRegistryPath)) {
    if (!file.exists(SchemaRegistryPath)) stop("Schema policy workbook not found: ", SchemaRegistryPath)
    schema_registry <- load_schema_registry(SchemaRegistryPath, create_if_missing = FALSE,
                                            profile = if (SchemaProfile == "none") "generic" else SchemaProfile)
  } else if (is.null(schema_registry) && SchemaProfile != "none") {
    schema_registry <- load_schema_registry(NULL, create_if_missing = FALSE, profile = SchemaProfile)
  }
  observations <- if (inherits(survey, "RepositorySchemaSurvey")) {
    data.table::copy(survey$observations)
  } else {
    if (is.null(ObservationPath)) stop("Provide survey or ObservationPath.")
    GetSchemaObservations(ObservationPath)
  }
  observations <- data.table::as.data.table(observations)
  encoding_defaults <- list(DeclaredEncoding = NA_character_, DetectedEncoding = NA_character_,
                            EncodingConfidence = NA_real_, EncodingUsed = NA_character_,
                            EncodingDetectionMethod = NA_character_,
                            EncodingValidationStatus = NA_character_)
  for (field in names(encoding_defaults)) {
    if (!field %in% names(observations)) observations[, (field) := encoding_defaults[[field]]]
  }
  if (!"ReaderWarningClass" %in% names(observations)) observations[, ReaderWarningClass := NA_character_]
  if (!"ReaderWarningSeverity" %in% names(observations)) {
    observations[, ReaderWarningSeverity := ifelse(!is.na(ReaderWarning) & nzchar(ReaderWarning),
                                                    "warning", NA_character_)]
  }
  repair_defaults <- list(ReaderRepairCount = 0, ReaderRepairLines = NA_character_,
                          ReaderRepairPolicy = "error")
  for (field in names(repair_defaults)) {
    if (!field %in% names(observations)) observations[, (field) := repair_defaults[[field]]]
  }
  source_issues <- unique(observations[
    SurveyStatus != "ok" | (!is.na(ReaderWarning) & nzchar(ReaderWarning)),
    .(Database, TableName, SourcePath, FileType, PartitionKey, PartitionValue,
      SurveyStatus, ReaderWarning, ReaderWarningClass, ReaderWarningSeverity,
      ReaderRepairCount, ReaderRepairLines, ReaderRepairPolicy,
      SurveyMessage, DeclaredEncoding,
      DetectedEncoding, EncodingConfidence, EncodingUsed,
      EncodingDetectionMethod, EncodingValidationStatus)
  ])
  usable <- observations[SurveyStatus == "ok" &
    (ObservationKind == "hive_partition" | !IsPartitionColumn)]
  if (nrow(usable) == 0L) stop("Schema survey contains no usable column observations.")
  split_key <- paste(usable$Database, usable$TableName, usable$DuckDBTable,
                     usable$Column, sep = "\r")
  groups <- split(usable, split_key)
  registry <- data.table::rbindlist(lapply(groups, function(rows) {
    recommendation <- .schema_recommendation_for_group(rows)
    policy_hit <- schema_registry_match(rows$Column[1], schema_registry,
                                        database = rows$Database[1], table_name = rows$TableName[1])
    resolved <- .schema_policy_resolution(rows, recommendation, policy_hit)
    observed_types <- paste(sort(unique(rows$ObservedType)), collapse = ",")
    role <- if (all(rows$ObservationKind == "hive_partition")) "partition" else
      resolved$Role %||% "data"
    signature_text <- paste(
      "schema_proposal_v2",
      paste(sort(unique(paste(rows$SourceFingerprint, rows$PartitionValue,
                              rows$ObservedType, rows$ReaderWarningClass,
                               rows$ReaderWarningSeverity, rows$EncodingUsed,
                               rows$ReaderRepairCount, rows$ReaderRepairLines,
                               rows$ReaderRepairPolicy,
                               rows$LeadingZeroCount, rows$NumericParseFailureCount,
                              rows$PrecisionRisk, sep = "|"))), collapse = "||"),
      resolved$PolicyPattern, resolved$PolicyType, resolved$PolicyStatus, sep = "||")
    data.table::data.table(
      Database = rows$Database[1], TableName = rows$TableName[1],
      DuckDBTable = rows$DuckDBTable[1], Column = rows$Column[1],
      ObservedTypes = observed_types, TypeHistory = .schema_type_history(rows),
      DataRecommendedType = resolved$DataRecommendedType,
      RecommendedType = resolved$RecommendedType,
      ApprovedType = resolved$RecommendedType,
      Risk = resolved$Risk, RecommendationReason = resolved$Reason,
      Confidence = paste(sort(unique(rows$InferenceConfidence)), collapse = ","),
      PolicyPattern = resolved$PolicyPattern, PolicyType = resolved$PolicyType,
      PolicyRole = resolved$PolicyRole, PolicyStatus = resolved$PolicyStatus,
      PolicyConflict = resolved$PolicyConflict,
      Role = role, MergeGroup = "", RequiresReview = resolved$RequiresReview,
      Decision = if (resolved$RequiresReview) "" else "Auto-approved",
      DecisionOrigin = if (resolved$RequiresReview) "new" else "automatic",
      UserNotes = "", ObservationSignature = digest::digest(signature_text, algo = "sha256", serialize = FALSE)
    )
  }), fill = TRUE)
  data.table::setorder(registry, Database, TableName, Column)
  compatibility <- .schema_compatibility_review(registry)
  history <- unique(usable[, .(Database, TableName, DuckDBTable, Column,
                               PartitionKey, PartitionValue, ObservedType,
                               InferenceConfidence, ReaderWarning,
                               ReaderWarningClass, ReaderWarningSeverity,
                               ReaderRepairCount, ReaderRepairLines, ReaderRepairPolicy)])
  type_counts <- history[, .(NTypes = data.table::uniqueN(ObservedType)),
                         by = .(Database, TableName, Column)]
  history <- merge(history, type_counts, by = c("Database", "TableName", "Column"), all.x = TRUE)
  history <- history[NTypes > 1L | (!is.na(ReaderWarning) & nzchar(ReaderWarning))]
  summary <- data.table::data.table(
    Columns = nrow(registry), AutoApproved = sum(!registry$RequiresReview),
    NeedsReview = sum(registry$RequiresReview), SourceIssues = nrow(source_issues),
    CompatibilityConflicts = nrow(compatibility)
  )
  structure(list(registry = registry, compatibility = compatibility,
                 history = history, source_issues = source_issues,
                 summary = summary,
                 ObservationPath = ObservationPath %||% survey$ObservationPath),
            class = "RepositorySchemaProposal")
}

.preserve_schema_review_decisions <- function(registry, SchemaReviewPath) {
  if (!file.exists(SchemaReviewPath) || !is_excel_workbook_path(SchemaReviewPath)) return(registry)
  sheets <- tryCatch(openxlsx::getSheetNames(SchemaReviewPath), error = function(e) character(0))
  old_parts <- lapply(intersect(c("ColumnDecisions", "Review", "Registry"), sheets), function(sheet) {
    tryCatch(data.table::as.data.table(openxlsx::read.xlsx(SchemaReviewPath, sheet = sheet)), error = function(e) NULL)
  })
  old <- data.table::rbindlist(Filter(Negate(is.null), old_parts), fill = TRUE)
  if (nrow(old) == 0L || !all(c("Database", "TableName", "Column", "ObservationSignature") %in% names(old))) return(registry)
  #### Preserve only deliberate user decisions. Blank rows from an older, ####
  #### more conservative proposal must not replace a newly auto-approved   ####
  #### decision when recommendation rules improve.                          ####
  if (!"Decision" %in% names(old)) return(registry)
  old_decision <- tolower(trimws(as.character(old$Decision)))
  old <- old[old_decision %in% c("accept", "override")]
  if (nrow(old) == 0L) return(registry)
  old <- old[!duplicated(paste(Database, TableName, Column, sep = "\r"))]
  current_key <- paste(registry$Database, registry$TableName, registry$Column, sep = "\r")
  old_key <- paste(old$Database, old$TableName, old$Column, sep = "\r")
  idx <- match(current_key, old_key)
  same <- !is.na(idx) & registry$ObservationSignature == old$ObservationSignature[idx]
  fields <- intersect(c("ApprovedType", "Decision", "Role", "MergeGroup", "UserNotes"), names(old))
  registry <- .copy_review_character_fields(
    registry, old, which(same), idx[which(same)], fields)
  registry[same & RequiresReview == TRUE & nzchar(trimws(as.character(Decision))),
           DecisionOrigin := "preserved"]
  registry
}

.preserve_compatibility_decisions <- function(compatibility, SchemaReviewPath) {
  if (nrow(compatibility) == 0L || !file.exists(SchemaReviewPath) ||
      !is_excel_workbook_path(SchemaReviewPath)) return(compatibility)
  sheets <- tryCatch(openxlsx::getSheetNames(SchemaReviewPath), error = function(e) character(0))
  old_parts <- lapply(intersect(c("CompatibilityDecisions", "CompatibilityReview",
                                  "CompatibilityRegistry"), sheets), function(sheet) {
    tryCatch(data.table::as.data.table(openxlsx::read.xlsx(
      SchemaReviewPath, sheet = sheet)), error = function(e) NULL)
  })
  old <- data.table::rbindlist(Filter(Negate(is.null), old_parts), fill = TRUE)
  required <- c("Scope", "Database", "Column", "CompatibilitySignature")
  if (nrow(old) == 0L || !all(required %in% names(old))) return(compatibility)
  if (!"Decision" %in% names(old)) return(compatibility)
  old_decision <- tolower(trimws(as.character(old$Decision)))
  old <- old[old_decision %in% c("accept", "override", "ignore")]
  if (nrow(old) == 0L) return(compatibility)
  old <- old[!duplicated(paste(Scope, Database, Column, sep = "\r"))]
  key <- function(x) paste(x$Scope, x$Database, x$Column, sep = "\r")
  idx <- match(key(compatibility), key(old))
  same <- !is.na(idx) & compatibility$CompatibilitySignature == old$CompatibilitySignature[idx]
  fields <- intersect(c("ApprovedCommonType", "Decision", "SuggestedRole", "UserNotes"),
                      names(old))
  compatibility <- .copy_review_character_fields(
    compatibility, old, which(same), idx[which(same)], fields)
  compatibility
}

.copy_review_character_fields <- function(target, source, target_rows,
                                           source_rows, fields) {
  target <- data.table::as.data.table(target)
  if (length(target_rows) == 0L || length(fields) == 0L) return(target)
  for (field in fields) {
    #### Blank Excel columns are often read as numeric or logical. Review ####
    #### fields are textual contracts, so normalize both sides before a  ####
    #### data.table assignment can coerce valid decisions to NA.          ####
    if (!field %in% names(target)) {
      target[, (field) := NA_character_]
    } else {
      data.table::set(target, j = field, value = as.character(target[[field]]))
    }
    data.table::set(target, i = target_rows, j = field,
                    value = as.character(source[[field]][source_rows]))
  }
  target
}

#' Write a compact, user-reviewable schema proposal workbook
#' @export
WriteSchemaProposal <- function(proposal, SchemaReviewPath, PreserveDecisions = TRUE) {
  if (!inherits(proposal, "RepositorySchemaProposal")) stop("proposal must come from RecommendRepositorySchema().")
  registry <- data.table::copy(proposal$registry)
  policy_defaults <- list(
    DataRecommendedType = if ("RecommendedType" %in% names(registry)) registry$RecommendedType else NA_character_,
    PolicyPattern = NA_character_, PolicyType = NA_character_, PolicyRole = NA_character_,
    PolicyStatus = "not_configured", PolicyConflict = FALSE
  )
  for (field in names(policy_defaults)) {
    if (!field %in% names(registry)) registry[, (field) := policy_defaults[[field]]]
  }
  if (isTRUE(PreserveDecisions)) registry <- .preserve_schema_review_decisions(registry, SchemaReviewPath)
  compatibility <- data.table::copy(proposal$compatibility %||% data.table::data.table())
  if (isTRUE(PreserveDecisions)) {
    compatibility <- .preserve_compatibility_decisions(compatibility, SchemaReviewPath)
  }
  if (ncol(compatibility) == 0L) {
    #### Keep the hidden registry structurally valid even when no groups  ####
    #### exist. Header-only sheets read cleanly and do not masquerade as  ####
    #### user decisions during finalization.                              ####
    compatibility <- data.table::data.table(
      Scope = character(), Database = character(), Column = character(),
      MergeGroup = character(), CurrentTypes = character(), Tables = character(),
      RecommendedCommonType = character(), ApprovedCommonType = character(),
      RecommendationReason = character(), SuggestedRole = character(),
      Decision = character(), CompatibilitySignature = character(),
      UserNotes = character())
  }
  normalize_decision <- function(x) {
    out <- tolower(trimws(as.character(x)))
    out[is.na(out)] <- ""
    out
  }
  column_unresolved <- registry$RequiresReview %in% TRUE &
    !normalize_decision(registry$Decision) %in% c("accept", "override")
  column_decisions <- data.table::copy(registry[column_unresolved])
  if (nrow(column_decisions) > 0L) {
    column_decisions[, RequiredAction :=
      "Select Accept, or choose ApprovedType and select Override."]
    first <- c("Decision", "ApprovedType", "UserNotes", "RequiredAction",
               "Database", "TableName", "Column", "RecommendedType", "Risk",
               "RecommendationReason", "ObservedTypes", "TypeHistory")
    data.table::setcolorder(column_decisions,
      c(intersect(first, names(column_decisions)), setdiff(names(column_decisions), first)))
  }
  compatibility_unresolved <- if (nrow(compatibility) > 0L) {
    !normalize_decision(compatibility$Decision) %in% c("accept", "override", "ignore")
  } else logical(0)
  compatibility_decisions <- data.table::copy(compatibility[compatibility_unresolved])
  if (nrow(compatibility_decisions) > 0L) {
    compatibility_decisions[, RequiredAction := paste0(
      "Select Accept, select Override after changing ApprovedCommonType, ",
      "or select Ignore to keep the fields separate.")]
    first <- c("Decision", "ApprovedCommonType", "UserNotes", "RequiredAction",
               "Scope", "Database", "Column", "CurrentTypes",
               "RecommendedCommonType", "RecommendationReason", "Tables")
    data.table::setcolorder(compatibility_decisions,
      c(intersect(first, names(compatibility_decisions)),
        setdiff(names(compatibility_decisions), first)))
  }
  policy_report <- registry[PolicyConflict %in% TRUE, .(
    ActionRequired = "No - informational only",
    Database, TableName, DuckDBTable, Column, ObservedTypes,
    DataRecommendedType, PolicyType, RecommendedType,
    PolicyPattern, PolicyRole, PolicyStatus,
    PolicyApplied = PolicyStatus == "lossless_policy_promotion",
    Outcome = ifelse(PolicyStatus == "lossless_policy_promotion",
                     "Lossless policy type applied automatically",
                     "Policy not applied; data-derived type retained"),
    RecommendationReason
  )]
  source_issues <- data.table::copy(data.table::as.data.table(
    proposal$source_issues %||% data.table::data.table()))
  source_errors <- if ("SurveyStatus" %in% names(source_issues)) {
    tolower(trimws(as.character(source_issues$SurveyStatus))) == "error"
  } else rep(FALSE, nrow(source_issues))
  source_errors[is.na(source_errors)] <- FALSE
  if (nrow(source_issues) > 0L) {
    source_issues[, Blocking := ifelse(source_errors, "Yes", "No")]
    source_issues[, RequiredAction := ifelse(
      source_errors,
      "Correct the MDT or reader configuration, then rerun PrepareSchemaRegistry().",
      "Review the warning; any required column decision appears in ColumnDecisions.")]
    first <- c("Blocking", "RequiredAction", "Database", "TableName", "SourcePath",
               "SurveyStatus", "SurveyMessage", "ReaderWarning")
    data.table::setcolorder(source_issues,
      c(intersect(first, names(source_issues)), setdiff(names(source_issues), first)))
  }
  n_source_errors <- sum(source_errors)
  n_column_decisions <- nrow(column_decisions)
  n_compatibility_decisions <- nrow(compatibility_decisions)
  ready <- n_source_errors == 0L && n_column_decisions == 0L &&
    n_compatibility_decisions == 0L
  column_overview_fields <- c(
    "Database", "TableName", "DuckDBTable", "Column", "ObservedTypes",
    "TypeHistory", "DataRecommendedType", "PolicyType", "RecommendedType",
    "ApprovedType", "Risk", "RecommendationReason", "RequiresReview",
    "Decision", "Role", "MergeGroup", "Confidence", "UserNotes")
  column_overview <- registry[, intersect(column_overview_fields, names(registry)),
                              with = FALSE]
  compatibility_overview_fields <- c(
    "Scope", "Database", "Column", "CurrentTypes", "Tables",
    "RecommendedCommonType", "ApprovedCommonType", "RecommendationReason",
    "SuggestedRole", "Decision", "MergeGroup", "UserNotes")
  compatibility_overview <- compatibility[,
    intersect(compatibility_overview_fields, names(compatibility)), with = FALSE]
  type_history <- data.table::copy(data.table::as.data.table(
    proposal$history %||% data.table::data.table()))
  start_here <- data.table::data.table(
    Step = c("Source validation", "Column schema decisions",
             "Cross-table compatibility", "Policy report", "Finalization"),
    Status = c(
      if (n_source_errors > 0L) "BLOCKED" else if (nrow(source_issues) > 0L) "COMPLETE - WARNINGS LOGGED" else "COMPLETE",
      if (n_column_decisions > 0L) "ACTION REQUIRED" else "COMPLETE",
      if (n_compatibility_decisions > 0L) "ACTION REQUIRED" else "COMPLETE",
      "INFORMATIONAL",
      if (ready) "READY" else "BLOCKED"),
    Remaining = c(n_source_errors, n_column_decisions, n_compatibility_decisions,
                  nrow(policy_report), n_source_errors + n_column_decisions + n_compatibility_decisions),
    RequiredAction = c(
      if (n_source_errors > 0L) "Resolve blocking rows and rerun PrepareSchemaRegistry()." else "None.",
      if (n_column_decisions > 0L) "Complete every visible row." else "None.",
      if (n_compatibility_decisions > 0L) "Complete every visible row." else "None.",
      "No decision required; review only when useful.",
      if (ready) "Run FinalizeSchemaRegistry()." else "Complete the blocking steps above."),
    Worksheet = c("SourceIssues", "ColumnDecisions", "CompatibilityDecisions",
                  "PolicyReport", "StartHere")
  )
  settings <- data.table::data.table(
    Setting = c("ObservationPath", "Columns", "AutoApproved", "ColumnDecisions",
                "CompatibilityDecisions", "CompatibilityGroups", "PolicyItems",
                "SourceIssues", "BlockingSourceErrors", "GeneratedUTC"),
    Value = c(proposal$ObservationPath %||% "", proposal$summary$Columns,
              proposal$summary$AutoApproved, n_column_decisions,
              n_compatibility_decisions,
              proposal$summary$CompatibilityConflicts %||% nrow(compatibility),
              nrow(policy_report), nrow(source_issues), n_source_errors,
              format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
  )
  workbook_guide <- data.table::data.table(
    Worksheet = c(
      "StartHere", "ColumnDecisions", "CompatibilityDecisions", "SourceIssues",
      "ColumnOverview", "CompatibilityOverview", "TypeHistory", "PolicyReport",
      "Registry", "CompatibilityRegistry", "Settings"),
    Category = c(
      "Guide", "Action", "Action", "Validation", "Reference", "Reference",
      "Reference", "Reference", "System", "System", "System"),
    Contains = c(
      "Current workflow status, required actions, and this workbook guide.",
      "Only columns whose recommended type needs an explicit decision.",
      "Only cross-table or cross-database compatibility groups needing a decision.",
      "Source read errors and warnings found during schema survey.",
      "Every column, its observed type history, recommendation, and approved output type.",
      "Every compatibility candidate and its proposed common type and role.",
      "Detailed evidence for columns whose types changed or whose readers raised warnings.",
      "Informational differences between data-derived recommendations and optional policies.",
      "Complete machine-readable column registry used during finalization.",
      "Complete machine-readable compatibility registry used during finalization.",
      "Generation paths, counts, and timestamp."),
    UserAction = c(
      "Begin here and follow rows marked ACTION REQUIRED or BLOCKED.",
      if (n_column_decisions > 0L) "Complete every row." else "None.",
      if (n_compatibility_decisions > 0L) "Complete every row." else "None.",
      if (n_source_errors > 0L) "Resolve blocking errors and rerun schema preparation." else
        if (nrow(source_issues) > 0L) "Review warnings when useful." else "None.",
      "Review how source types will be normalized; make decisions only in ColumnDecisions.",
      "Review proposed shared types; make decisions only in CompatibilityDecisions.",
      "Use for investigation; no edits are required.",
      "Review when useful; no decisions are required here.",
      "Do not edit.", "Do not edit.", "Do not edit."),
    Status = c(
      if (ready) "READY" else "ACTION REQUIRED",
      if (n_column_decisions > 0L) "ACTION REQUIRED" else "COMPLETE",
      if (n_compatibility_decisions > 0L) "ACTION REQUIRED" else "COMPLETE",
      if (n_source_errors > 0L) "BLOCKED" else if (nrow(source_issues) > 0L) "WARNINGS" else "COMPLETE",
      rep("REFERENCE", 4L), rep("HIDDEN", 3L)),
    Rows = c(
      nrow(start_here), n_column_decisions, n_compatibility_decisions,
      nrow(source_issues), nrow(column_overview), nrow(compatibility_overview),
      nrow(type_history), nrow(policy_report), nrow(registry),
      nrow(compatibility), nrow(settings))
  )
  dir.create(dirname(SchemaReviewPath), recursive = TRUE, showWarnings = FALSE)
  tmp <- tempfile(pattern = paste0(basename(SchemaReviewPath), ".tmp_"),
                  tmpdir = dirname(SchemaReviewPath), fileext = ".xlsx")
  wb <- openxlsx::createWorkbook()
  openxlsx::addWorksheet(wb, "StartHere", gridLines = FALSE, tabColour = "#70AD47")
  openxlsx::mergeCells(wb, "StartHere", cols = 1:6, rows = 1)
  openxlsx::writeData(wb, "StartHere", "Schema Review", startRow = 1, startCol = 1)
  openxlsx::mergeCells(wb, "StartHere", cols = 1:6, rows = 2)
  openxlsx::writeData(wb, "StartHere",
    paste0("Only rows marked ACTION REQUIRED or BLOCKED need attention. ",
           "Action tabs come first; overview tabs explain how columns will be formatted."),
    startRow = 2, startCol = 1)
  openxlsx::mergeCells(wb, "StartHere", cols = 1:6, rows = 3)
  openxlsx::writeData(wb, "StartHere",
    paste0("Counts are a snapshot from workbook generation. After completing the visible ",
           "decision rows, run FinalizeSchemaRegistry(); another survey is not required."),
    startRow = 3, startCol = 1)
  openxlsx::writeDataTable(wb, "StartHere", start_here, startRow = 5,
                           tableStyle = "TableStyleMedium2")
  guide_title_row <- 7L + nrow(start_here)
  guide_table_row <- guide_title_row + 1L
  openxlsx::mergeCells(wb, "StartHere", cols = 1:6, rows = guide_title_row)
  openxlsx::writeData(wb, "StartHere", "Workbook Guide",
                      startRow = guide_title_row, startCol = 1)
  openxlsx::writeDataTable(wb, "StartHere", workbook_guide,
                           startRow = guide_table_row,
                           tableStyle = "TableStyleMedium2")
  openxlsx::freezePane(wb, "StartHere", firstActiveRow = 6)
  openxlsx::setColWidths(wb, "StartHere", cols = 1:6,
                         widths = c(28, 24, 62, 58, 28, 12))
  title_style <- openxlsx::createStyle(fontSize = 18, fontColour = "#FFFFFF",
                                        fgFill = "#1F4E78", textDecoration = "bold",
                                        halign = "left", valign = "center")
  note_style <- openxlsx::createStyle(fontColour = "#404040", fgFill = "#D9EAF7",
                                       wrapText = TRUE, valign = "center")
  openxlsx::addStyle(wb, "StartHere", title_style, rows = 1, cols = 1:6, gridExpand = TRUE)
  openxlsx::addStyle(wb, "StartHere", note_style, rows = 2:3, cols = 1:6, gridExpand = TRUE)
  openxlsx::addStyle(wb, "StartHere", title_style, rows = guide_title_row,
                     cols = 1:6, gridExpand = TRUE)
  openxlsx::setRowHeights(wb, "StartHere", rows = c(1, 2, 3), heights = c(28, 34, 34))
  openxlsx::setRowHeights(wb, "StartHere", rows = guide_title_row, heights = 24)
  status_styles <- list(
    green = openxlsx::createStyle(fgFill = "#E2F0D9", fontColour = "#385723", textDecoration = "bold"),
    yellow = openxlsx::createStyle(fgFill = "#FFF2CC", fontColour = "#7F6000", textDecoration = "bold"),
    red = openxlsx::createStyle(fgFill = "#F4CCCC", fontColour = "#9C0006", textDecoration = "bold"),
    gray = openxlsx::createStyle(fgFill = "#E7E6E6", fontColour = "#404040", textDecoration = "bold")
  )
  for (i in seq_len(nrow(start_here))) {
    style <- if (start_here$Status[i] %in% c("COMPLETE", "COMPLETE - WARNINGS LOGGED", "READY")) {
      status_styles$green
    } else if (start_here$Status[i] == "ACTION REQUIRED") {
      status_styles$yellow
    } else if (start_here$Status[i] == "BLOCKED") {
      status_styles$red
    } else status_styles$gray
    openxlsx::addStyle(wb, "StartHere", style, rows = 5L + i, cols = 2, stack = TRUE)
  }
  openxlsx::addStyle(
    wb, "StartHere", openxlsx::createStyle(wrapText = TRUE, valign = "top"),
    rows = (guide_table_row + 1L):(guide_table_row + nrow(workbook_guide)),
    cols = c(3, 4), gridExpand = TRUE, stack = TRUE)
  for (i in seq_len(nrow(workbook_guide))) {
    style <- if (workbook_guide$Status[i] %in% c("COMPLETE", "READY")) {
      status_styles$green
    } else if (workbook_guide$Status[i] %in% c("ACTION REQUIRED", "WARNINGS")) {
      status_styles$yellow
    } else if (workbook_guide$Status[i] == "BLOCKED") {
      status_styles$red
    } else status_styles$gray
    openxlsx::addStyle(wb, "StartHere", style,
                       rows = guide_table_row + i, cols = 5, stack = TRUE)
  }
  empty_display <- function(status, action) {
    data.table::data.table(Status = status, RequiredAction = action)
  }
  column_decisions_display <- if (n_column_decisions > 0L) column_decisions else
    empty_display("COMPLETE", "No column decisions are required.")
  compatibility_decisions_display <- if (n_compatibility_decisions > 0L) compatibility_decisions else
    empty_display("COMPLETE", "No compatibility decisions are required.")
  source_issues_display <- if (nrow(source_issues) > 0L) source_issues else
    empty_display("COMPLETE", "No source issues were identified.")
  compatibility_overview_display <- if (nrow(compatibility_overview) > 0L) compatibility_overview else
    empty_display("COMPLETE", "No compatibility candidates were identified.")
  type_history_display <- if (nrow(type_history) > 0L) type_history else
    empty_display("COMPLETE", "No changing types or reader warnings were identified.")
  policy_report_display <- if (nrow(policy_report) > 0L) policy_report else
    empty_display("INFORMATIONAL", "No policy conflicts were identified.")
  sheets <- list(ColumnDecisions = column_decisions_display,
                 CompatibilityDecisions = compatibility_decisions_display,
                 SourceIssues = source_issues_display,
                 ColumnOverview = column_overview,
                 CompatibilityOverview = compatibility_overview_display,
                 TypeHistory = type_history_display,
                 PolicyReport = policy_report_display,
                 Registry = registry,
                 CompatibilityRegistry = compatibility,
                 Settings = settings)
  header_style <- openxlsx::createStyle(fgFill = "#1F4E78", fontColour = "#FFFFFF",
                                        textDecoration = "bold", border = "bottom")
  for (sheet in names(sheets)) {
    tab_colour <- if (sheet %in% c("ColumnDecisions", "CompatibilityDecisions")) "#FFC000" else
      if (sheet == "SourceIssues" && n_source_errors > 0L) "#C00000" else
      if (sheet %in% c("ColumnOverview", "CompatibilityOverview", "TypeHistory")) "#5B9BD5" else
      if (sheet == "PolicyReport") "#A5A5A5" else "#D9E1F2"
    openxlsx::addWorksheet(wb, sheet, gridLines = FALSE, tabColour = tab_colour)
    value <- data.table::as.data.table(sheets[[sheet]])
    if (nrow(value) > 0L) {
      openxlsx::writeDataTable(wb, sheet, value, tableStyle = "TableStyleMedium2")
    } else if (ncol(value) > 0L) {
      #### openxlsx cannot create an Excel table with zero data rows. Keep ####
      #### the sheet useful by writing its headers as ordinary cells.      ####
      openxlsx::writeData(wb, sheet, value, colNames = TRUE)
    }
    openxlsx::freezePane(wb, sheet, firstRow = TRUE)
    if (ncol(value) > 0L) openxlsx::addStyle(wb, sheet, header_style, rows = 1, cols = seq_len(ncol(value)), gridExpand = TRUE)
    widths <- pmin(45, pmax(12, nchar(names(value)) + 2))
    if (ncol(value) > 0L) openxlsx::setColWidths(wb, sheet, cols = seq_len(ncol(value)), widths = widths)
    wide_cols <- which(names(value) %in% c(
      "RequiredAction", "RecommendationReason", "TypeHistory", "Tables",
      "SourcePath", "SurveyMessage", "ReaderWarning", "Outcome"))
    if (length(wide_cols) > 0L) {
      openxlsx::setColWidths(wb, sheet, cols = wide_cols, widths = 45)
      if (nrow(value) > 0L) {
        openxlsx::addStyle(wb, sheet, openxlsx::createStyle(wrapText = TRUE, valign = "top"),
                           rows = 2:(nrow(value) + 1L), cols = wide_cols,
                           gridExpand = TRUE, stack = TRUE)
      }
    }
  }
  edit_style <- openxlsx::createStyle(fgFill = "#FFF2CC", fontColour = "#000000")
  if (nrow(column_decisions) > 0L) {
    type_col <- match("ApprovedType", names(column_decisions))
    decision_col <- match("Decision", names(column_decisions))
    notes_col <- match("UserNotes", names(column_decisions))
    openxlsx::dataValidation(wb, "ColumnDecisions", cols = type_col, rows = 2:(nrow(column_decisions) + 1L),
                             type = "list", value = paste0('"', paste(setdiff(allowed_canonical_types(), "decimal(p,s)"), collapse = ","), '"'))
    openxlsx::dataValidation(wb, "ColumnDecisions", cols = decision_col, rows = 2:(nrow(column_decisions) + 1L),
                             type = "list", value = '"Accept,Override"')
    openxlsx::addStyle(wb, "ColumnDecisions", edit_style,
                       rows = 2:(nrow(column_decisions) + 1L),
                       cols = c(decision_col, type_col, notes_col), gridExpand = TRUE, stack = TRUE)
    risk_col <- match("Risk", names(column_decisions))
    openxlsx::conditionalFormatting(wb, "ColumnDecisions", cols = risk_col, rows = 2:(nrow(column_decisions) + 1L),
                                    rule = '=="Potential loss"', style = openxlsx::createStyle(fgFill = "#F4CCCC"))
    openxlsx::conditionalFormatting(wb, "ColumnDecisions", cols = risk_col, rows = 2:(nrow(column_decisions) + 1L),
                                    rule = '=="Review"', style = openxlsx::createStyle(fgFill = "#FFF2CC"))
  }
  if (nrow(compatibility_decisions) > 0L) {
    type_col <- match("ApprovedCommonType", names(compatibility_decisions))
    decision_col <- match("Decision", names(compatibility_decisions))
    role_col <- match("SuggestedRole", names(compatibility_decisions))
    notes_col <- match("UserNotes", names(compatibility_decisions))
    rows <- 2:(nrow(compatibility_decisions) + 1L)
    openxlsx::dataValidation(wb, "CompatibilityDecisions", cols = type_col, rows = rows,
                             type = "list", value = paste0('"', paste(setdiff(allowed_canonical_types(), "decimal(p,s)"), collapse = ","), '"'))
    openxlsx::dataValidation(wb, "CompatibilityDecisions", cols = decision_col, rows = rows,
                             type = "list", value = '"Accept,Override,Ignore"')
    openxlsx::dataValidation(wb, "CompatibilityDecisions", cols = role_col, rows = rows,
                             type = "list", value = '"join_key,compatible_column,data"')
    openxlsx::addStyle(wb, "CompatibilityDecisions", edit_style, rows = rows,
                       cols = c(decision_col, type_col, notes_col),
                       gridExpand = TRUE, stack = TRUE)
  }
  sheet_order <- c("StartHere", names(sheets))
  advanced <- c("Registry", "CompatibilityRegistry", "Settings")
  visibility <- ifelse(sheet_order %in% advanced, "hidden", "visible")
  openxlsx::sheetVisibility(wb) <- visibility
  openxlsx::saveWorkbook(wb, tmp, overwrite = TRUE)
  replace_file_safely(tmp, SchemaReviewPath)
  log_msg(sprintf(paste0("[SCHEMA PROPOSAL] Wrote %s (%d columns; %d column decision(s), ",
                         "%d compatibility decision(s), %d blocking source error(s))."),
                  SchemaReviewPath, nrow(registry), n_column_decisions,
                  n_compatibility_decisions, n_source_errors))
  result <- SchemaReviewPath
  attr(result, "ReviewStatus") <- list(
    ColumnDecisions = n_column_decisions,
    CompatibilityDecisions = n_compatibility_decisions,
    BlockingSourceErrors = n_source_errors,
    ReadyToFinalize = ready)
  invisible(result)
}

.overlay_schema_review <- function(registry, review) {
  if (is.null(review) || nrow(review) == 0L) return(registry)
  required <- c("Database", "TableName", "Column")
  if (!all(required %in% names(review))) return(registry)
  key <- function(x) paste(x$Database, x$TableName, x$Column, sep = "\r")
  idx <- match(key(registry), key(review))
  hit <- which(!is.na(idx))
  fields <- intersect(c("ApprovedType", "Decision", "Role", "MergeGroup", "UserNotes"),
                      names(review))
  .copy_review_character_fields(registry, review, hit, idx[hit], fields)
}

.overlay_compatibility_decisions <- function(compatibility, decisions) {
  if (is.null(decisions) || nrow(decisions) == 0L) return(compatibility)
  required <- c("Scope", "Database", "Column")
  if (!all(required %in% names(decisions))) return(compatibility)
  if (is.null(compatibility) || nrow(compatibility) == 0L) return(decisions)
  key <- function(x) paste(x$Scope, x$Database, x$Column, sep = "\r")
  idx <- match(key(compatibility), key(decisions))
  hit <- which(!is.na(idx))
  fields <- intersect(c("ApprovedCommonType", "Decision", "SuggestedRole", "UserNotes"),
                      names(decisions))
  .copy_review_character_fields(compatibility, decisions, hit, idx[hit], fields)
}

.apply_compatibility_review <- function(registry, compatibility, strict = TRUE) {
  registry <- data.table::copy(data.table::as.data.table(registry))
  for (field in c("ApprovedType", "Role", "MergeGroup")) {
    if (field %in% names(registry)) {
      data.table::set(registry, j = field, value = as.character(registry[[field]]))
    }
  }
  if (!"MergeReviewed" %in% names(registry)) registry[, MergeReviewed := FALSE]
  if (!"CompatibilityApplied" %in% names(registry)) registry[, CompatibilityApplied := FALSE]
  if (is.null(compatibility) || nrow(compatibility) == 0L) return(registry)
  compatibility <- data.table::as.data.table(compatibility)
  required <- c("Scope", "Database", "Column", "MergeGroup", "RecommendedCommonType",
                "ApprovedCommonType", "SuggestedRole", "Decision")
  missing <- setdiff(required, names(compatibility))
  if (length(missing) > 0L) {
    stop("The compatibility decision table is missing required columns: ", paste(missing, collapse = ", "))
  }
  decision <- tolower(trimws(as.character(compatibility$Decision)))
  decision[is.na(decision)] <- ""
  unresolved <- !decision %in% c("accept", "override", "ignore")
  if (any(unresolved)) {
    examples <- utils::head(sprintf("%s/%s/%s", compatibility$Scope[unresolved],
                                    compatibility$Database[unresolved],
                                    compatibility$Column[unresolved]), 5L)
    message <- sprintf(paste0(
      "%d compatibility decision(s) remain unresolved in CompatibilityDecisions. ",
      "First unresolved: %s"), sum(unresolved), paste(examples, collapse = ", "))
    if (isTRUE(strict)) stop(message) else log_msg(paste("[SCHEMA REVIEW WARNING]", message))
  }
  active <- which(!unresolved)
  approved <- rep(NA_character_, nrow(compatibility))
  typed <- active[decision[active] %in% c("accept", "override")]
  if (length(typed) > 0L) {
    raw <- trimws(as.character(compatibility$ApprovedCommonType[typed]))
    if (any(is.na(raw) | !nzchar(raw))) stop("ApprovedCommonType is blank for an accepted compatibility group.")
    approved[typed] <- vapply(raw, normalize_type_name, character(1))
    if (any(!is_allowed_canonical_type(approved[typed]))) {
      stop("CompatibilityDecisions contains an invalid ApprovedCommonType.")
    }
    recommended <- vapply(compatibility$RecommendedCommonType[typed], normalize_type_name, character(1))
    bad_override <- approved[typed] != recommended & decision[typed] != "override"
    if (any(bad_override)) {
      stop("Compatibility rows whose ApprovedCommonType differs from RecommendedCommonType must use Decision='Override'.")
    }
  }
  assignments <- list()
  for (i in active) {
    scope <- tolower(trimws(as.character(compatibility$Scope[i])))
    column <- canonical_colnames(compatibility$Column[i])
    target <- which(registry$Column == column &
      (scope == "cross_database" | registry$Database == as.character(compatibility$Database[i])))
    if (length(target) == 0L) next
    registry[target, MergeReviewed := TRUE]
    if (decision[i] == "ignore") next
    assignments[[length(assignments) + 1L]] <- data.table::data.table(
      Row = target, CanonicalType = approved[i],
      MergeGroup = toupper(trimws(as.character(compatibility$MergeGroup[i]))),
      Role = trimws(as.character(compatibility$SuggestedRole[i])),
      Priority = if (scope == "cross_database") 2L else 1L)
  }
  assigned <- data.table::rbindlist(assignments, fill = TRUE)
  if (nrow(assigned) > 0L) {
    #### A cross-database decision intentionally supersedes a narrower     ####
    #### within-database decision for the same physical column. Detect     ####
    #### only disagreements among rules at the highest applicable scope.  ####
    highest <- assigned[, .(Priority = max(Priority)), by = Row]
    candidates <- assigned[highest, on = .(Row, Priority), nomatch = 0L]
    conflicts <- candidates[, .(
      NTypes = data.table::uniqueN(CanonicalType),
      Types = paste(sort(unique(CanonicalType)), collapse = ",")),
      by = Row][NTypes > 1L]
    if (nrow(conflicts) > 0L) {
      stop("Compatibility decisions at the same scope assign different types to the same table column.")
    }
    data.table::setorder(candidates, Row, -Priority)
    chosen <- candidates[!duplicated(Row)]
    registry[chosen$Row, `:=`(
      ApprovedType = chosen$CanonicalType,
      MergeGroup = chosen$MergeGroup,
      Role = ifelse(is.na(chosen$Role) | !nzchar(chosen$Role), Role, chosen$Role),
      CompatibilityApplied = TRUE
    )]
  }
  registry
}

#' Finalize a reviewed schema proposal into the writer catalog
#' @export
FinalizeRepositorySchema <- function(SchemaReviewPath, TableSchemaPath, strict = TRUE) {
  if (!file.exists(SchemaReviewPath)) stop("Schema review workbook not found: ", SchemaReviewPath)
  sheets <- openxlsx::getSheetNames(SchemaReviewPath)
  if (!"Registry" %in% sheets) stop("Schema review workbook is missing the Registry sheet.")
  registry <- data.table::as.data.table(openxlsx::read.xlsx(SchemaReviewPath, sheet = "Registry"))
  review_sheet <- if ("ColumnDecisions" %in% sheets) "ColumnDecisions" else
    if ("Review" %in% sheets) "Review" else NA_character_
  review <- if (!is.na(review_sheet)) {
    data.table::as.data.table(openxlsx::read.xlsx(SchemaReviewPath, sheet = review_sheet))
  } else NULL
  required <- c("Database", "TableName", "DuckDBTable", "Column", "ObservedTypes",
                "RecommendedType", "ApprovedType", "RequiresReview", "Decision", "Role", "MergeGroup")
  missing <- setdiff(required, names(registry))
  if (length(missing) > 0L) {
    stop("Schema review Registry sheet is missing required columns: ", paste(missing, collapse = ", "))
  }
  #### Source errors invalidate downstream decisions. Report these before  ####
  #### asking for column review, because a corrected/resurveyed source can ####
  #### change the proposal and its observation signatures.                  ####
  if ("SourceIssues" %in% sheets) {
    source_issues <- data.table::as.data.table(openxlsx::read.xlsx(SchemaReviewPath, sheet = "SourceIssues"))
    source_errors <- if ("SurveyStatus" %in% names(source_issues)) {
      tolower(as.character(source_issues$SurveyStatus)) == "error"
    } else logical(0)
    if (any(source_errors, na.rm = TRUE)) {
      error_rows <- which(source_errors %in% TRUE)
      examples <- utils::head(basename(as.character(source_issues$SourcePath[error_rows])), 5L)
      message <- sprintf(paste0(
        "Schema survey contains %d source error(s) in SourceIssues: %s. Update the MDT or reader configuration, ",
        "then rerun PrepareSchemaRegistry() before finalizing."),
        sum(source_errors, na.rm = TRUE), paste(examples, collapse = ", "))
      if (isTRUE(strict)) stop(message) else log_msg(paste("[SCHEMA REVIEW WARNING]", message))
    }
  }
  registry <- .overlay_schema_review(registry, review)
  compatibility_sheet <- if ("CompatibilityRegistry" %in% sheets) "CompatibilityRegistry" else
    if ("CompatibilityReview" %in% sheets) "CompatibilityReview" else
      if ("CompatibilityDecisions" %in% sheets) "CompatibilityDecisions" else NA_character_
  compatibility <- if (!is.na(compatibility_sheet)) {
    data.table::as.data.table(openxlsx::read.xlsx(SchemaReviewPath, sheet = compatibility_sheet))
  } else NULL
  if ("CompatibilityDecisions" %in% sheets &&
      !identical(compatibility_sheet, "CompatibilityDecisions")) {
    compatibility_decisions <- data.table::as.data.table(openxlsx::read.xlsx(
      SchemaReviewPath, sheet = "CompatibilityDecisions"))
    compatibility <- .overlay_compatibility_decisions(compatibility, compatibility_decisions)
  }
  parse_review_flag <- function(x) {
    text <- tolower(trimws(as.character(x)))
    out <- rep(NA, length(text))
    out[text %in% c("true", "t", "yes", "y", "1")] <- TRUE
    out[text %in% c("false", "f", "no", "n", "0")] <- FALSE
    out
  }
  needs_review <- parse_review_flag(registry$RequiresReview)
  if (anyNA(needs_review)) stop("RequiresReview contains blank or invalid logical values.")
  decision <- tolower(trimws(as.character(registry$Decision)))
  decision[is.na(decision)] <- ""
  unresolved <- needs_review & !decision %in% c("accept", "override")
  if (any(unresolved)) {
    examples <- utils::head(sprintf("%s/%s/%s", registry$Database[unresolved],
                                    registry$TableName[unresolved], registry$Column[unresolved]), 5L)
    message <- sprintf(paste0(
      "%d schema decision(s) remain unresolved in ColumnDecisions. Set Decision to Accept, ",
      "or choose ApprovedType and set Decision to Override. First unresolved: %s"),
      sum(unresolved), paste(examples, collapse = ", "))
    if (isTRUE(strict)) stop(message) else log_msg(paste("[SCHEMA REVIEW WARNING]", message))
  }
  raw_approved <- trimws(as.character(registry$ApprovedType))
  if (any(is.na(raw_approved) | !nzchar(raw_approved))) {
    stop("ApprovedType contains blank values; choose a canonical type before finalizing.")
  }
  approved <- vapply(raw_approved, normalize_type_name, character(1))
  invalid <- !is_allowed_canonical_type(approved)
  if (any(invalid)) stop("Invalid ApprovedType value(s): ", paste(unique(registry$ApprovedType[invalid]), collapse = ", "))
  recommended <- vapply(registry$RecommendedType, normalize_type_name, character(1))
  changed_without_override <- approved != recommended & decision != "override"
  if (any(changed_without_override)) stop("Rows whose ApprovedType differs from RecommendedType must use Decision='Override'.")
  registry[, ApprovedType := approved]
  registry[, Decision := decision]
  registry <- .apply_compatibility_review(registry, compatibility, strict = strict)
  merge_group <- toupper(trimws(as.character(registry$MergeGroup)))
  registry[, MergeGroup := merge_group]
  declared <- registry[!is.na(MergeGroup) & nzchar(MergeGroup)]
  if (nrow(declared) > 0L) {
    conflicts <- declared[, .(NTypes = data.table::uniqueN(ApprovedType),
                              Types = paste(sort(unique(ApprovedType)), collapse = ",")), by = MergeGroup][NTypes > 1L]
    if (nrow(conflicts) > 0L) stop("Approved merge groups contain incompatible types: ",
                                    paste(sprintf("%s (%s)", conflicts$MergeGroup, conflicts$Types), collapse = "; "))
  }
  if (!"DataRecommendedType" %in% names(registry)) registry[, DataRecommendedType := RecommendedType]
  if (!"PolicyPattern" %in% names(registry)) registry[, PolicyPattern := NA_character_]
  if (!"PolicyStatus" %in% names(registry)) registry[, PolicyStatus := "not_configured"]
  if (!"MergeReviewed" %in% names(registry)) registry[, MergeReviewed := FALSE]
  if (!"CompatibilityApplied" %in% names(registry)) registry[, CompatibilityApplied := FALSE]
  table_schema <- registry[, .(
    Database, TableName, DuckDBTable, Column,
    CanonicalType = ApprovedType,
    InferredType = ObservedTypes,
    RegistryOverride = ApprovedType != DataRecommendedType,
    Role = ifelse(is.na(Role) | !nzchar(Role), "data", Role),
    RegistryPattern = PolicyPattern,
    MergeGroup, MergeReviewed,
    Source = ifelse(CompatibilityApplied, "user_approved",
             ifelse(tolower(Decision) == "auto-approved" & ApprovedType == DataRecommendedType,
                    "auto_approved",
                    ifelse(tolower(Decision) == "auto-approved", "policy_approved", "user_approved")))
  )]
  ValidateSchemaMergeKeys(table_schema, strict = strict)
  write_table_schema_catalog(table_schema, TableSchemaPath)
  log_msg(sprintf("[SCHEMA FINALIZED] Wrote %d approved column definitions to %s.",
                  nrow(table_schema), TableSchemaPath))
  invisible(load_table_schema_catalog(TableSchemaPath, strict = TRUE))
}

#' Prepare the Parquet observations and compact review workbook
#' @export
PrepareSchemaRegistry <- function(MDT, MasterDBPath, ObservationPath, SchemaReviewPath,
                                  DBLoad = NULL, n_workers = 1,
                                  SourceFingerprintMode = c("metadata", "sha256", "none"),
                                  StrictReaders = FALSE, SchemaRegistryPath = NULL,
                                  schema_registry = NULL,
                                  SchemaProfile = c("none", "generic", "hcup"),
                                  LogPath = NULL, RunId = NULL) {
  survey <- SurveyRepositorySchema(MDT = MDT, MasterDBPath = MasterDBPath,
                                   ObservationPath = ObservationPath, DBLoad = DBLoad,
                                   n_workers = n_workers,
                                   SourceFingerprintMode = match.arg(SourceFingerprintMode),
                                   StrictReaders = StrictReaders, LogPath = LogPath, RunId = RunId)
  proposal <- RecommendRepositorySchema(
    survey = survey, SchemaRegistryPath = SchemaRegistryPath,
    schema_registry = schema_registry, SchemaProfile = match.arg(SchemaProfile))
  written <- WriteSchemaProposal(proposal, SchemaReviewPath)
  review_status <- attr(written, "ReviewStatus")
  log_msg(sprintf(paste0("[SCHEMA READY] Open StartHere in %s: %d column decision(s), ",
                         "%d compatibility decision(s), %d blocking source error(s)."),
                  SchemaReviewPath,
                  review_status$ColumnDecisions %||% proposal$summary$NeedsReview,
                  review_status$CompatibilityDecisions %||% proposal$summary$CompatibilityConflicts,
                  review_status$BlockingSourceErrors %||% proposal$summary$SourceIssues))
  invisible(list(survey = survey, proposal = proposal,
                 ObservationPath = ObservationPath, SchemaReviewPath = SchemaReviewPath))
}

#' Finalize the reviewed registry using the existing table-catalog contract
#' @export
FinalizeSchemaRegistry <- function(SchemaReviewPath, TableSchemaPath, strict = TRUE) {
  FinalizeRepositorySchema(SchemaReviewPath, TableSchemaPath, strict = strict)
}

#' Convert a named class map to a long schema table
#' @export
schema_map_to_long <- function(class_map, database, table_name, duckdb_table, source = "inferred") {
  if (is.null(class_map) || length(class_map) == 0L) {
    return(data.table::data.table(Database = character(), TableName = character(), DuckDBTable = character(), Column = character(), CanonicalType = character(), Source = character()))
  }
  nm <- canonical_colnames(names(class_map))
  data.table::data.table(Database = as.character(database), TableName = as.character(table_name), DuckDBTable = as.character(duckdb_table),
                         Column = nm, CanonicalType = vapply(unname(class_map), normalize_type_name, character(1)), Source = source)
}

#' Write the table schema catalog as Excel or CSV
#' @export
write_table_schema_catalog <- function(table_schema, TableSchemaPath, label_catalog = NULL) {
  if (is.null(TableSchemaPath) || !nzchar(TableSchemaPath)) return(invisible(NULL))
  dir.create(dirname(TableSchemaPath), recursive = TRUE, showWarnings = FALSE)
  table_schema <- data.table::as.data.table(table_schema)
  #### Preserve the data dictionary across writers that don't harvest       ####
  #### labels themselves (e.g. ParquetBackEndCreate's end-of-run write):    ####
  #### rewriting the workbook without re-reading the Labels sheet would     ####
  #### silently destroy it.                                                 ####
  if (is.null(label_catalog)) label_catalog <- load_label_catalog(TableSchemaPath)
  if (is_excel_workbook_path(TableSchemaPath)) {
    sheets <- list(TableSchemas = table_schema)
    if (nrow(table_schema) > 0L) {
      if (!"MergeGroup" %in% names(table_schema)) table_schema[, MergeGroup := NA_character_]
      if (!"MergeReviewed" %in% names(table_schema)) table_schema[, MergeReviewed := FALSE]
      merge_keys <- table_schema[Role %in% c("join_key", "partition") |
                                   (!is.na(MergeGroup) & nzchar(MergeGroup)),
                                 .(Database, TableName, DuckDBTable, Column,
                                   CanonicalType, Role, MergeGroup, MergeReviewed,
                                   RegistryOverride)]
      column_inventory <- table_schema[, .(Tables = paste(sort(unique(DuckDBTable)), collapse = "; "), Databases = paste(sort(unique(Database)), collapse = "; "), Types = paste(sort(unique(CanonicalType)), collapse = "; "), Roles = paste(sort(unique(Role)), collapse = "; ")), by = Column]
      sheets$MergeKeys <- merge_keys
      sheets$ColumnInventory <- column_inventory
    }
    if (!is.null(label_catalog) && nrow(data.table::as.data.table(label_catalog)) > 0L) {
      sheets$Labels <- data.table::as.data.table(label_catalog)
    }
    write_xlsx_safely(sheets, TableSchemaPath)
  } else {
    write_csv_safely(table_schema, TableSchemaPath)
    if (!is.null(label_catalog) && nrow(data.table::as.data.table(label_catalog)) > 0L) {
      write_csv_safely(data.table::as.data.table(label_catalog), label_catalog_path(TableSchemaPath))
    }
  }
  invisible(TableSchemaPath)
}

#### Sibling file used for the data dictionary when the catalog is CSV.     ####
label_catalog_path <- function(TableSchemaPath) {
  paste0(tools::file_path_sans_ext(TableSchemaPath), "_Labels.csv")
}

#' Read the data-dictionary (Labels) catalog back from disk
#'
#' Companion reader for the \code{Labels} sheet of \code{TableSchemas.xlsx}
#' (or the \code{*_Labels.csv} sibling when the catalog is CSV). Returns NULL
#' when absent.
#' @export
load_label_catalog <- function(TableSchemaPath) {
  if (is.null(TableSchemaPath) || !nzchar(TableSchemaPath) || !file.exists(TableSchemaPath)) return(NULL)
  lab <- tryCatch({
    if (is_excel_workbook_path(TableSchemaPath)) {
      if (!"Labels" %in% openxlsx::getSheetNames(TableSchemaPath)) return(NULL)
      data.table::as.data.table(openxlsx::read.xlsx(TableSchemaPath, sheet = "Labels"))
    } else {
      lp <- label_catalog_path(TableSchemaPath)
      if (!file.exists(lp)) return(NULL)
      data.table::fread(lp)
    }
  }, error = function(e) {
    #### The dictionary is optional, so a read failure must not block the   ####
    #### workflow -- but it must not be silent either, or the Labels sheet  ####
    #### quietly vanishes on the next catalog rewrite.                      ####
    log_msg(sprintf("[LABELS WARNING] Could not read the Labels dictionary for %s: %s", TableSchemaPath, conditionMessage(e)))
    NULL
  })
  if (is.null(lab) || nrow(lab) == 0L) return(NULL)
  lab
}

#' Search the data dictionary for variables by label, name, or value text
#'
#' Makes the harvested \code{Labels} sheet queryable: find every column across
#' every table whose variable label, column name, or value labels match a
#' pattern -- e.g. \code{search_labels("payer", TableSchemaPath)} to locate the
#' payer variables in all databases at once. Resolved canonical types are
#' merged in from the schema catalog, and when \code{ParquetBasePath} is given
#' each hit also reports which partitions of its table exist on disk.
#' @param pattern Regular expression (case-insensitive by default).
#' @param TableSchemaPath Path to the schema catalog holding the Labels sheet.
#' @param search_in Any of "label", "column", "values" (default: all three).
#' @param ignore_case Logical, default TRUE.
#' @param ParquetBasePath Optional. When given, adds a PartitionsOnDisk column.
#' @return data.table of matches: Database, TableName, DuckDBTable, Column,
#'   CanonicalType, VariableLabel, ValueLabels (and PartitionsOnDisk).
#' @seealso \code{\link{column_availability}} for per-year column presence.
#' @export
search_labels <- function(pattern, TableSchemaPath, search_in = c("label", "column", "values"),
                          ignore_case = TRUE, ParquetBasePath = NULL) {
  lab <- load_label_catalog(TableSchemaPath)
  if (is.null(lab)) {
    stop("No Labels dictionary found for this catalog. Run BuildRepositoryCatalog(HarvestLabels = TRUE) first.")
  }
  search_in <- match.arg(search_in, several.ok = TRUE)
  hit <- rep(FALSE, nrow(lab))
  if ("label" %in% search_in)  hit <- hit | grepl(pattern, lab$VariableLabel, ignore.case = ignore_case)
  if ("column" %in% search_in) hit <- hit | grepl(pattern, lab$Column, ignore.case = ignore_case)
  if ("values" %in% search_in) hit <- hit | grepl(pattern, lab$ValueLabels, ignore.case = ignore_case)
  out <- lab[hit]
  if (nrow(out) == 0L) return(out)
  sch <- load_table_schema_catalog(TableSchemaPath)
  if (!is.null(sch)) {
    ts <- unique(data.table::as.data.table(sch$table_schema)[, c("Database", "TableName", "Column", "CanonicalType"), with = FALSE])
    out <- merge(out, ts, by = c("Database", "TableName", "Column"), all.x = TRUE, sort = FALSE)
  }
  if (!is.null(ParquetBasePath) && nzchar(ParquetBasePath)) {
    part_of <- function(tb) {
      d <- file.path(ParquetBasePath, tb)
      if (!dir.exists(d)) return(NA_character_)
      parts <- basename(list.dirs(d, recursive = FALSE))
      parts <- parts[grepl("=", parts, fixed = TRUE)]
      if (length(parts) == 0L) NA_character_ else paste(sort(parts), collapse = "; ")
    }
    tbl_parts <- vapply(unique(out$DuckDBTable), part_of, character(1))
    out[, PartitionsOnDisk := tbl_parts[DuckDBTable]]
  }
  data.table::setorder(out, Database, TableName, Column)
  out[]
}

#### Parse a serialized value-label string ("0 = Did not die; 1 = Died")     ####
#### back into its code set. Returns NULL when the domain is unknowable      ####
#### (truncated during harvest), so callers skip validation rather than      ####
#### false-flagging.                                                         ####
parse_value_label_codes <- function(x) {
  x <- as.character(x)[1]
  if (is.na(x) || !nzchar(x)) return(character(0))
  if (endsWith(x, " ...")) return(NULL)
  pieces <- strsplit(x, "; ", fixed = TRUE)[[1]]
  #### Pieces without " = " are continuations of a label that itself        ####
  #### contained "; " -- they carry no code, so skip them.                   ####
  has_code <- grepl(" = ", pieces, fixed = TRUE)
  codes <- trimws(sub(" = .*$", "", pieces[has_code]))
  unique(codes[nzchar(codes)])
}

#' Validate table contents against the data dictionary's value domains
#'
#' Content-integrity companion to \code{\link{audit_repository}}: for every
#' column whose harvested value labels define a code domain (e.g.
#' \code{DIED} in \{0, 1\}), reports how many stored values fall outside it.
#' This catches the silent-coercion class of problem -- source values the type
#' normalization turned into NA, or codes that drifted between releases.
#'
#' Interpretation caveat: HCUP labels continuous variables' \emph{special}
#' codes only (e.g. AGE might label just \code{999 = missing}), so a high
#' out-of-domain share is not automatically an error -- it can simply mean the
#' column is continuous. Filter on \code{DomainSize} or sort by
#' \code{PctOutOfDomain} and judge; nothing here mutates data.
#' @param con Live DuckDB connection with the views registered (or readable
#'   tables of the same names).
#' @param TableSchemaPath Path to the schema catalog holding the Labels sheet.
#' @param tables Optional character vector restricting which DuckDB tables run.
#' @param min_domain Integer. Only validate columns whose parsed domain has a
#'   least this many codes (default 2).
#' @param verbose Logical. Log a summary line per table.
#' @return data.table: DuckDBTable, Column, DomainSize, Domain (first codes),
#'   Total, NNull, OutOfDomain, PctOutOfDomain -- sorted worst-first.
#' @export
validate_against_dictionary <- function(con, TableSchemaPath, tables = NULL,
                                        min_domain = 2L, verbose = TRUE) {
  lab <- load_label_catalog(TableSchemaPath)
  if (is.null(lab)) {
    stop("No Labels dictionary found for this catalog. Run BuildRepositoryCatalog(HarvestLabels = TRUE) first.")
  }
  lab <- lab[!is.na(lab$ValueLabels) & nzchar(lab$ValueLabels), ]
  if (!is.null(tables)) lab <- lab[lab$DuckDBTable %in% tables, ]
  available <- DBI::dbListTables(con)
  results <- list()
  for (tb in sort(unique(lab$DuckDBTable))) {
    if (!tb %in% available) {
      if (verbose) log_msg(sprintf("[DICT VALIDATE] %s: not registered in DuckDB -- skipped.", tb))
      next
    }
    qtbl <- quote_duckdb_ident(tb)
    desc <- DBI::dbGetQuery(con, paste("DESCRIBE", qtbl))
    desc$canon <- canonical_colnames(desc$column_name)
    cols <- lab[lab$DuckDBTable == tb, ]
    sums <- character(0); meta <- list()
    for (j in seq_len(nrow(cols))) {
      codes <- parse_value_label_codes(cols$ValueLabels[j])
      if (is.null(codes) || length(codes) < min_domain) next
      di <- match(canonical_colnames(cols$Column[j]), desc$canon)
      if (is.na(di)) next
      qcol <- quote_duckdb_ident(desc$column_name[di])
      is_numeric_col <- grepl("INT|DOUBLE|FLOAT|REAL|DECIMAL|NUMERIC|HUGEINT", desc$column_type[di], ignore.case = TRUE)
      lits <- if (is_numeric_col) {
        num <- suppressWarnings(as.numeric(codes))
        num <- num[!is.na(num)]
        if (length(num) == 0L) next
        format(num, scientific = FALSE, trim = TRUE)
      } else {
        vapply(codes, quote_duckdb_string, character(1))
      }
      k <- length(meta) + 1L
      sums <- c(sums,
                sprintf("SUM(CASE WHEN %s IS NULL THEN 1 ELSE 0 END) AS null_%d", qcol, k),
                sprintf("SUM(CASE WHEN %s IS NOT NULL AND %s NOT IN (%s) THEN 1 ELSE 0 END) AS out_%d",
                        qcol, qcol, paste(lits, collapse = ", "), k))
      meta[[k]] <- list(column = desc$column_name[di], domain = codes)
    }
    if (length(meta) == 0L) next
    q <- sprintf("SELECT COUNT(*) AS total, %s FROM %s", paste(sums, collapse = ", "), qtbl)
    res <- tryCatch(DBI::dbGetQuery(con, q), error = function(e) NULL)
    if (is.null(res)) {
      if (verbose) log_msg(sprintf("[DICT VALIDATE] %s: query failed -- skipped.", tb))
      next
    }
    for (k in seq_along(meta)) {
      dom <- meta[[k]]$domain
      results[[length(results) + 1L]] <- data.table::data.table(
        DuckDBTable = tb, Column = meta[[k]]$column,
        DomainSize = length(dom),
        Domain = paste(utils::head(dom, 8L), collapse = ", "),
        Total = as.numeric(res$total[1]),
        NNull = as.numeric(res[[sprintf("null_%d", k)]][1]),
        OutOfDomain = as.numeric(res[[sprintf("out_%d", k)]][1]),
        PctOutOfDomain = round(100 * as.numeric(res[[sprintf("out_%d", k)]][1]) / max(1, as.numeric(res$total[1])), 4))
    }
    if (verbose) log_msg(sprintf("[DICT VALIDATE] %s: %d labeled column(s) checked.", tb, length(meta)))
  }
  out <- if (length(results) > 0L) data.table::rbindlist(results) else
    data.table::data.table(DuckDBTable = character(), Column = character(), DomainSize = integer(),
                           Domain = character(), Total = numeric(), NNull = numeric(),
                           OutOfDomain = numeric(), PctOutOfDomain = numeric())
  data.table::setorder(out, -PctOutOfDomain, DuckDBTable, Column)
  out[]
}

#' Harvest SPSS variable and value labels from SAV headers
#'
#' Reads each file with \code{n_max = 0} (schema only -- no data rows) and
#' collects, per column, the human-readable variable label and the value-label
#' map that \code{\link{strip_haven}} discards during loading. This is the raw
#' material for the \code{Labels} sheet of the schema catalog: a searchable
#' data dictionary spanning every table.
#' @param files Character vector of full paths to \code{.sav} files, ordered
#'   most-authoritative first (the first non-empty label per column wins, so
#'   pass the newest year first).
#' @param n_workers Parallel workers for the header reads.
#' @return data.table with Column, VariableLabel, ValueLabels, SourceFile --
#'   one row per column that carries at least one label.
#' @export
harvest_sav_labels <- function(files, n_workers = 1) {
  harvest_source_labels(files, reader = "sav", n_workers = n_workers)
}

#' Harvest variable and value labels from any labeled source format
#'
#' Generalization of \code{\link{harvest_sav_labels}} across the reader
#' registry: any FileType whose reader declares \code{has_labels} (SPSS
#' \code{.sav}, Stata \code{.dta}, SAS \code{.sas7bdat}/\code{.xpt}) yields
#' the same data-dictionary rows, since the haven family shares the label
#' attribute structure.
#' @export
harvest_source_labels <- function(files, reader, n_workers = 1) {
  empty <- data.table::data.table(Column = character(), VariableLabel = character(),
                                  ValueLabels = character(), SourceFile = character())
  if (length(files) == 0L) return(empty)
  rd <- get_file_reader(reader)
  if (!isTRUE(rd$has_labels) || is.null(rd$read_labels_header)) return(empty)
  read_labels_header <- rd$read_labels_header
  scan_one <- function(p) {
    tryCatch({
      hdr <- read_labels_header(p)
      cols <- names(hdr)
      var_lab <- vapply(cols, function(cn) {
        lb <- attr(hdr[[cn]], "label", exact = TRUE)
        if (is.null(lb) || !nzchar(trimws(as.character(lb)[1]))) NA_character_ else trimws(as.character(lb)[1])
      }, character(1))
      val_lab <- vapply(cols, function(cn) {
        lv <- attr(hdr[[cn]], "labels", exact = TRUE)
        if (is.null(lv) || length(lv) == 0L) return(NA_character_)
        out <- paste(sprintf("%s = %s", as.character(unname(lv)), names(lv)), collapse = "; ")
        if (nchar(out) > 5000L) out <- paste0(substr(out, 1L, 5000L), " ...")
        out
      }, character(1))
      data <- data.table::data.table(Column = cols, VariableLabel = var_lab,
                                    ValueLabels = val_lab, SourceFile = basename(p))
      list(ok = TRUE, path = p, data = data, error = NA_character_)
    }, error = function(e) {
      list(ok = FALSE, path = p, data = NULL, error = conditionMessage(e))
    })
  }
  label_results <- .parallel_scan_with_serial_retry(
    files, scan_one, n_workers = n_workers,
    future_packages = c("haven", "data.table"),
    is_failure = function(x) !is.list(x) || !isTRUE(x$ok),
    context = sprintf("%s label harvesting", reader)
  )
  failed <- vapply(label_results, function(x) !isTRUE(x$ok), logical(1))
  if (any(failed)) {
    for (result in label_results[failed]) {
      log_msg(sprintf("[LABEL WARNING] Could not harvest labels from %s: %s",
                      result$path, result$error))
    }
  }
  per_file <- lapply(label_results[!failed], `[[`, "data")
  if (length(per_file) == 0L) return(empty)
  all_lab <- data.table::rbindlist(per_file, fill = TRUE)
  all_lab[, Column := canonical_colnames(Column)]
  #### First non-empty label per column wins (files arrive newest-first).   ####
  has_any <- !(is.na(all_lab$VariableLabel) & is.na(all_lab$ValueLabels))
  all_lab <- all_lab[has_any]
  if (nrow(all_lab) == 0L) return(empty)
  all_lab[!duplicated(Column)]
}

#' Read the table schema catalog back as the authoritative column-type source
#'
#' Reads \code{TableSchemas.xlsx} (sheet \code{TableSchemas}) or the CSV
#' equivalent and converts it into the per-table \code{col_classes} maps the
#' loader consumes. This is the read half of the curation workflow: edit a
#' row's \code{CanonicalType} and set its \code{Source} to \code{"manual"} to
#' pin that column's type; \code{\link{BuildRepositoryCatalog}} preserves such
#' rows on re-inference. In strict mode, unrecognised \code{CanonicalType}
#' values stop the load; non-strict inspection logs them and falls back to
#' character.
#' @param TableSchemaPath Character. Path to the catalog written by
#'   \code{\link{write_table_schema_catalog}}.
#' @return \code{NULL} when the file is absent or unusable; otherwise a list
#'   with \code{table_schema} (data.table), \code{col_classes} (nested list:
#'   \code{col_classes[[Database]][[TableName]]} is a named list of
#'   column -> canonical type, YEAR excluded), and \code{TableSchemaPath}.
#' @seealso \code{\link{BuildRepositoryCatalog}}, \code{\link{ParquetBackEndCreate}}
#' @export
load_table_schema_catalog <- function(TableSchemaPath, strict = FALSE) {
  if (is.null(TableSchemaPath) || !nzchar(TableSchemaPath) || !file.exists(TableSchemaPath)) return(NULL)
  fail_catalog <- function(message) {
    if (isTRUE(strict)) stop(message)
    log_msg(message)
    NULL
  }
  ts <- tryCatch({
    if (is_excel_workbook_path(TableSchemaPath)) {
      data.table::as.data.table(openxlsx::read.xlsx(TableSchemaPath, sheet = "TableSchemas"))
    } else {
      data.table::fread(TableSchemaPath)
    }
  }, error = function(e) e)
  if (inherits(ts, "error")) {
    return(fail_catalog(sprintf("[CATALOG ERROR] Could not read %s (%s). Refusing to bypass an existing schema catalog.", TableSchemaPath, ts$message)))
  }
  required <- c("Database", "TableName", "Column", "CanonicalType")
  missing_cols <- setdiff(required, names(ts))
  if (length(missing_cols) > 0L) {
    return(fail_catalog(sprintf("[CATALOG ERROR] %s lacks required column(s): %s. Refusing to bypass an existing schema catalog.",
                                TableSchemaPath, paste(missing_cols, collapse = ", "))))
  }
  if (!"Source" %in% names(ts)) ts[, Source := NA_character_]
  if (!"Role" %in% names(ts)) ts[, Role := NA_character_]
  ts[, Database := as.character(Database)]
  ts[, TableName := as.character(TableName)]
  ts[, Column := canonical_colnames(Column)]
  ts[, CanonicalType := vapply(as.character(CanonicalType), normalize_type_name, character(1))]
  dup <- duplicated(ts[, .(Database, TableName, Column)]) |
    duplicated(ts[, .(Database, TableName, Column)], fromLast = TRUE)
  if (any(dup)) {
    conflicts <- ts[dup, .(NTypes = data.table::uniqueN(CanonicalType),
                           Types = paste(sort(unique(CanonicalType)), collapse = ", ")),
                    by = .(Database, TableName, Column)][NTypes > 1L]
    if (nrow(conflicts) > 0L && isTRUE(strict)) {
      stop(sprintf("[CATALOG ERROR] Conflicting duplicate schema rows in %s: %s",
                   TableSchemaPath,
                   paste(sprintf("%s/%s/%s (%s)", conflicts$Database, conflicts$TableName,
                                 conflicts$Column, conflicts$Types), collapse = "; ")))
    }
    log_msg(sprintf("[CATALOG WARNING] %d duplicate Database/TableName/Column row(s) in %s -- keeping the first of each.",
                    sum(duplicated(ts[, .(Database, TableName, Column)])), TableSchemaPath))
    ts <- ts[!duplicated(ts[, .(Database, TableName, Column)])]
  }
  allowed <- allowed_canonical_types()
  bad <- which(!is_allowed_canonical_type(ts$CanonicalType))
  if (length(bad) > 0L) {
    bad_details <- paste(sprintf("%s/%s/%s='%s'", ts$Database[bad], ts$TableName[bad],
                                 ts$Column[bad], ts$CanonicalType[bad]), collapse = "; ")
    if (isTRUE(strict)) {
      stop(sprintf("[CATALOG ERROR] Invalid CanonicalType value(s) in %s: %s. Valid types: %s",
                   TableSchemaPath, bad_details, paste(allowed, collapse = ", ")))
    }
    for (i in bad) log_msg(sprintf("[CATALOG WARNING] %s/%s column %s: unrecognised CanonicalType '%s' -- using character. Valid types: %s",
                                   ts$Database[i], ts$TableName[i], ts$Column[i], ts$CanonicalType[i],
                                   paste(allowed, collapse = ", ")))
    ts[bad, CanonicalType := "character"]
  }
  col_classes <- list()
  #### Partition columns come from directory names, not file contents;     ####
  #### only explicit catalog Role == "partition" rows are removed from     ####
  #### writer class maps. A real source-file column named YEAR can survive ####
  #### when a table is partitioned by something else.                      ####
  is_partition <- !is.na(ts$Role) & tolower(as.character(ts$Role)) == "partition"
  body_rows <- ts[!is_partition]
  for (db in unique(body_rows$Database)) {
    db_rows <- body_rows[Database == db]
    col_classes[[db]] <- lapply(split(db_rows, by = "TableName"),
                                function(tr) as.list(stats::setNames(tr$CanonicalType, tr$Column)))
  }
  list(table_schema = ts, col_classes = col_classes, TableSchemaPath = TableSchemaPath)
}

#' Merge a freshly inferred schema catalog with an existing one
#'
#' Preserves human curation across preflight runs: existing rows whose
#' \code{Source} is \code{"manual"} or \code{"user_approved"} keep their
#' reviewed fields (human decisions outrank both inference and the registry),
#' and rows for tables or databases the fresh pass did not cover are carried
#' forward unchanged.
#' @param new_schema data.table of freshly inferred/resolved catalog rows.
#' @param existing_schema data.table of the catalog currently on disk (or NULL).
#' @return data.table combining both per the precedence rules above.
#' @export
merge_table_schema_catalog <- function(new_schema, existing_schema = NULL) {
  new_schema <- data.table::as.data.table(new_schema)
  if (is.null(existing_schema)) return(new_schema)
  existing <- data.table::as.data.table(existing_schema)
  if (nrow(existing) == 0L) return(new_schema)
  if (nrow(new_schema) == 0L) return(existing)
  for (nm in setdiff(names(existing), names(new_schema))) new_schema[, (nm) := NA]
  if (!"Source" %in% names(existing)) existing[, Source := NA_character_]
  key_of <- function(x) paste(x$Database, x$TableName, x$Column, sep = "||")
  curated <- existing[tolower(as.character(Source)) %in% c("manual", "user_approved")]
  if (nrow(curated) > 0L && nrow(new_schema) > 0L) {
    idx <- match(key_of(new_schema), key_of(curated))
    hit <- which(!is.na(idx))
    if (length(hit) > 0L) {
      preserve <- setdiff(intersect(names(new_schema), names(curated)),
                          c("Database", "TableName", "DuckDBTable", "Column", "InferredType"))
      for (nm in preserve) data.table::set(new_schema, i = hit, j = nm, value = curated[[nm]][idx[hit]])
    }
  }
  #### Rows absent from a freshly scanned table are stale columns and must ####
  #### be pruned. Only tables that were not part of this refresh are carried ####
  #### forward; manual metadata for columns still present was applied above. ####
  refreshed_tables <- unique(paste(new_schema$Database, new_schema$TableName, sep = "||"))
  existing_table <- paste(existing$Database, existing$TableName, sep = "||")
  carried <- existing[!existing_table %in% refreshed_tables]
  data.table::rbindlist(list(new_schema, carried), fill = TRUE)
}

#' Get registry metadata for a column
#' @export
schema_registry_match <- function(column, schema_registry = NULL, database = NULL, table_name = NULL) {
  if (is.null(schema_registry) || nrow(schema_registry) == 0L) return(NULL)
  column <- canonical_colnames(column)
  applies <- if ("AppliesTo" %in% names(schema_registry)) as.character(schema_registry$AppliesTo) else rep("all", nrow(schema_registry))
  for (i in seq_len(nrow(schema_registry))) {
    if (schema_registry_applies(applies[i], database = database, table_name = table_name) &&
        grepl(schema_registry$ColumnPattern[i], column, perl = TRUE, ignore.case = TRUE)) {
      return(schema_registry[i])
    }
  }
  NULL
}

#' Build a repository schema object for a database subset
#'
#' This is the phase-2 schema engine. It infers schemas per Database/TableName,
#' applies the repository schema registry, writes a transparent long-form schema
#' catalog, and returns the comprehensive column map plus table-specific class maps
#' needed by the writer.
#' @export
BuildRepositorySchema <- function(MDTSelect, MasterDBPath, Database = NULL, n_workers = 1,
                                  SchemaRegistryPath = NULL, TableSchemaPath = NULL,
                                  schema_registry = NULL, write_catalog = TRUE,
                                  known_col_classes = NULL, harvest_labels = FALSE) {
  if (is.null(schema_registry)) schema_registry <- load_schema_registry(SchemaRegistryPath, create_if_missing = TRUE)
  MDTdt <- data.table::as.data.table(MDTSelect)
  if (is.null(Database)) Database <- unique(MDTdt$Database)
  if (length(unique(MDTdt$Database)) > 1L && length(Database) == 1L) {
    db_filter <- as.character(Database[1])
    MDTdt <- MDTdt[as.character(MDTdt$Database) == db_filter]
  }
  MDTdt[, FileTypeLower := tolower(FileType)]
  comprehensive <- list()
  col_classes <- list()
  catalog_rows <- list()
  label_rows <- list()
  for (tbl in sort(unique(MDTdt$TableName))) {
    rows_tbl <- MDTdt[TableName == tbl]
    physical_tables <- unique(repository_table_names(rows_tbl))
    if (length(physical_tables) != 1L) {
      stop(sprintf("Table %s/%s resolves to multiple PhysicalTableName values: %s",
                   paste(unique(rows_tbl$Database), collapse = ","), tbl,
                   paste(physical_tables, collapse = ", ")))
    }
    physical_table <- physical_tables[1]
    reader_tbl <- as.character(rows_tbl$FileTypeLower)
    reader_opts <- lapply(seq_len(nrow(rows_tbl)), function(i) reader_options_for_row(rows_tbl[i, ]))
    full_paths <- file.path(MasterDBPath, rows_tbl$MDBDir, rows_tbl$Path)
    #### Missing files are excluded from schema inference so one absent     ####
    #### file cannot abort the whole database; each missing file still      ####
    #### fails individually at load time. Present-but-unreadable files      ####
    #### keep triggering the strict inference stop.                         ####
    missing_mask <- !file.exists(full_paths)
    if (any(missing_mask)) {
      log_msg(sprintf("[SCHEMA WARNING] %s/%s: %d source file(s) missing on disk (first: %s) -- excluded from schema inference; they will fail individually at load time.",
                      rows_tbl$Database[1], tbl, sum(missing_mask), basename(full_paths[which(missing_mask)[1]])))
      full_paths <- full_paths[!missing_mask]
      reader_tbl <- reader_tbl[!missing_mask]
      reader_opts <- reader_opts[!missing_mask]
    }
    #### Partition keys live in the directory names, never in the files, so ####
    #### they are excluded from the column union and the class maps. This   ####
    #### also errors early if the table's rows disagree on PartitionKey.    ####
    part_keys <- table_partition_keys(rows_tbl)
    part_types <- table_partition_types(rows_tbl)
    comprehensive[[tbl]] <- setdiff(build_comprehensive(files = full_paths, base_path = "", suffixes = rep(tbl, length(full_paths)),
                                                        uni_suffixes = tbl, n_workers = n_workers, reader = reader_tbl,
                                                        reader_options = reader_opts)[[tbl]],
                                    part_keys)
    #### Data dictionary: capture the SPSS variable/value labels that       ####
    #### strip_haven() discards during loading. Newest file first so a      ####
    #### label that evolved across years reflects the latest definition.    ####
    if (isTRUE(harvest_labels) && length(full_paths) > 0L) {
      rows_kept <- rows_tbl[!missing_mask]
      for (label_reader in unique(reader_tbl[vapply(reader_tbl, reader_supports_labels, logical(1))])) {
        ridx <- which(reader_tbl == label_reader)
        sort_val <- suppressWarnings(as.integer(vapply(ridx, function(i) {
          partition_spec_for_row(rows_kept[i, ])$values[1]
        }, character(1))))
        ord <- ridx[order(sort_val, decreasing = TRUE, na.last = TRUE)]
        lab <- harvest_source_labels(full_paths[ord], reader = label_reader, n_workers = n_workers)
        if (nrow(lab) > 0L) {
          lab[, Database := as.character(rows_tbl$Database[1])]
          lab[, TableName := as.character(tbl)]
          lab[, DuckDBTable := physical_table]
          data.table::setcolorder(lab, c("Database", "TableName", "DuckDBTable", "Column",
                                         "VariableLabel", "ValueLabels", "SourceFile"))
          label_rows[[length(label_rows) + 1L]] <- lab
        }
      }
    }
    from_catalog <- is.list(known_col_classes) && !is.null(known_col_classes[[tbl]])
    new_cols <- character(0)
    if (from_catalog) {
      #### Catalog mode: the curated catalog is the source of truth, so no   ####
      #### sampling and no registry re-application (manual edits would be    ####
      #### clobbered). Headers are still scanned above, and only columns the ####
      #### catalog has never seen get a fresh inference pass.                ####
      class_map <- known_col_classes[[tbl]]
      names(class_map) <- canonical_colnames(names(class_map))
      class_map <- class_map[!names(class_map) %in% part_keys]
      class_map <- class_map[names(class_map) %in% comprehensive[[tbl]]]
      new_cols <- setdiff(comprehensive[[tbl]], names(class_map))
      if (length(new_cols) > 0L) {
        log_msg(sprintf("[CATALOG] %s/%s: %d column(s) absent from the schema catalog (%s%s) -- inferring their types now. Re-run BuildRepositoryCatalog to record them.",
                        rows_tbl$Database[1], tbl, length(new_cols),
                        paste(utils::head(new_cols, 10L), collapse = ", "),
                        if (length(new_cols) > 10L) ", ..." else ""))
        sampled <- build_col_classes(files = full_paths, base_path = "", n_workers = n_workers,
                                     reader = reader_tbl, reader_options = reader_opts)
        names(sampled) <- canonical_colnames(names(sampled))
        sampled <- apply_schema_registry(sampled, schema_registry = schema_registry, database = rows_tbl$Database[1], table_name = tbl)
        class_map <- c(class_map, sampled[intersect(new_cols, names(sampled))])
        still_missing <- setdiff(new_cols, names(class_map))
        if (length(still_missing) > 0L) class_map[still_missing] <- "character"
      }
      inferred_map <- class_map
      source_label <- "catalog"
    } else {
      class_map <- build_col_classes(files = full_paths, base_path = "", n_workers = n_workers,
                                     reader = reader_tbl, reader_options = reader_opts)
      names(class_map) <- canonical_colnames(names(class_map))
      class_map <- class_map[!names(class_map) %in% part_keys]
      inferred_map <- class_map
      class_map <- apply_schema_registry(class_map, schema_registry = schema_registry, database = rows_tbl$Database[1], table_name = tbl)
      source_label <- "resolved"
    }
    col_classes[[tbl]] <- class_map
    long <- schema_map_to_long(class_map, database = rows_tbl$Database[1], table_name = tbl,
                               duckdb_table = physical_table, source = source_label)
    if (from_catalog && length(new_cols) > 0L && nrow(long) > 0L) {
      long[Column %in% new_cols, Source := "inferred_at_load"]
    }
    #### One catalog row per hive partition key. YEAR stays integer; other  ####
    #### keys are character, matching what DuckDB autocasts from the        ####
    #### directory names.                                                   ####
    year_row <- data.table::data.table(Database = as.character(rows_tbl$Database[1]),
                                       TableName = as.character(tbl),
                                       DuckDBTable = physical_table,
                                       Column = part_keys,
                                       CanonicalType = part_types,
                                       InferredType = "partition",
                                       RegistryOverride = TRUE,
                                       Role = "partition",
                                       RegistryPattern = paste0("^", part_keys, "$"),
                                       Source = "hive_partition")
    if (nrow(long) > 0L) {
      long[, InferredType := vapply(Column, function(cc) normalize_type_name(inferred_map[[cc]] %||% NA_character_), character(1))]
      long[, RegistryOverride := CanonicalType != InferredType]
      long[, Role := "inferred"]
      long[, RegistryPattern := NA_character_]
      for (j in seq_len(nrow(long))) {
        hit <- schema_registry_match(long$Column[j], schema_registry,
                                     database = rows_tbl$Database[1],
                                     table_name = tbl)
        if (!is.null(hit)) {
          long$Role[j] <- if ("Role" %in% names(hit)) as.character(hit$Role[1]) else "registry"
          long$RegistryPattern[j] <- as.character(hit$ColumnPattern[1])
        }
      }
      long <- long[, .(Database, TableName, DuckDBTable, Column, CanonicalType, InferredType, RegistryOverride, Role, RegistryPattern, Source)]
      long <- data.table::rbindlist(list(long, year_row), fill = TRUE)
      catalog_rows[[length(catalog_rows) + 1L]] <- long
    } else {
      catalog_rows[[length(catalog_rows) + 1L]] <- year_row
    }
  }
  table_schema <- if (length(catalog_rows) > 0L) data.table::rbindlist(catalog_rows, fill = TRUE) else data.table::data.table()
  label_catalog <- if (length(label_rows) > 0L) data.table::rbindlist(label_rows, fill = TRUE) else data.table::data.table()
  if (isTRUE(write_catalog)) write_table_schema_catalog(table_schema, TableSchemaPath,
                                                        label_catalog = if (nrow(label_catalog) > 0L) label_catalog else NULL)
  structure(list(comprehensive = comprehensive, col_classes = col_classes, table_schema = table_schema,
                 label_catalog = label_catalog,
                 schema_registry = schema_registry, TableSchemaPath = TableSchemaPath), class = "RepositorySchema")
}

# Provide a small infix helper without requiring rlang.
`%||%` <- function(x, y) if (is.null(x) || length(x) == 0L) y else x

#' Validate table-schema compatibility for merge-key columns within each year
#' @export
merge_key_name_candidate <- function(column) {
  column <- canonical_colnames(column)
  grepl(paste0("(^|_)KEY($|_)|(^|_)(ID|IDENTIFIER)($|_)|",
               "^(VISITLINK|NRD_VISITLINK|HOSP_NIS|HOSPID)$|",
               "^(PATIENT|ENCOUNTER|VISIT|CASE|FACILITY|PROVIDER|HOSPITAL)_?ID$"),
        column, perl = TRUE)
}

ValidateSchemaMergeKeys <- function(table_schema, strict = FALSE) {
  if (is.null(table_schema) || nrow(table_schema) == 0L) return(invisible(data.table::data.table()))
  ts <- data.table::copy(data.table::as.data.table(table_schema))
  if (!"Role" %in% names(ts)) ts[, Role := NA_character_]
  if (!"MergeGroup" %in% names(ts)) ts[, MergeGroup := NA_character_]
  if (!"MergeReviewed" %in% names(ts)) ts[, MergeReviewed := FALSE]
  if (!"DuckDBTable" %in% names(ts) && all(c("Database", "TableName") %in% names(ts))) {
    ts[, DuckDBTable := paste(Database, TableName, sep = "_")]
  }
  ts[, Column := canonical_colnames(Column)]
  ts[, CanonicalType := vapply(CanonicalType, normalize_type_name, character(1))]
  ts[, RoleNormalized := tolower(trimws(as.character(Role)))]
  ts[, MergeGroup := toupper(trimws(as.character(MergeGroup)))]
  ts[, MergeReviewed := as.logical(MergeReviewed)]
  ts[is.na(MergeReviewed), MergeReviewed := FALSE]
  ts[, ExplicitGroup := !is.na(MergeGroup) & nzchar(MergeGroup)]
  ts[, ExplicitMergeKey := RoleNormalized %in% c("join_key", "partition")]
  ts[, CandidateMergeKey := ExplicitMergeKey | vapply(Column, merge_key_name_candidate, logical(1))]
  issues_groups <- ts[ExplicitGroup == TRUE,
    .(Scope = "approved_merge_group", Database = "GROUP", Column = MergeGroup,
      Types = paste(sort(unique(CanonicalType)), collapse = ","),
      NTypes = data.table::uniqueN(CanonicalType),
      Tables = paste(sort(unique(DuckDBTable)), collapse = "; "),
      Databases = paste(sort(unique(Database)), collapse = "; "),
      Detection = "approved_compatibility"),
    by = MergeGroup][NTypes > 1L]
  #### Parentheses force data.table to read this as a logical filter on the ####
  #### column rather than a variable lookup in calling scope.               ####
  #### Once a candidate has been reviewed, only its approved MergeGroup is ####
  #### authoritative. This permits a user to approve within-database groups ####
  #### while explicitly keeping identically named cross-database fields     ####
  #### separate.                                                             ####
  keys <- ts[(CandidateMergeKey) & !MergeReviewed & !ExplicitGroup]
  issues_within_db <- keys[, .(Scope = "within_database",
                               Types = paste(sort(unique(CanonicalType)), collapse = ","),
                               NTypes = data.table::uniqueN(CanonicalType),
                               Tables = paste(sort(unique(DuckDBTable)), collapse = "; "),
                               Detection = if (any(ExplicitMergeKey)) "registry/catalog" else "name_candidate"),
                           by = .(Database, Column)][NTypes > 1L]
  issues_cross_db <- keys[, .(Scope = "cross_database",
                              Database = "ALL",
                              Types = paste(sort(unique(CanonicalType)), collapse = ","),
                              NTypes = data.table::uniqueN(CanonicalType),
                              Tables = paste(sort(unique(DuckDBTable)), collapse = "; "),
                              Databases = paste(sort(unique(Database)), collapse = "; "),
                              Detection = if (any(ExplicitMergeKey)) "registry/catalog" else "name_candidate"),
                          by = Column][NTypes > 1L]
  issues <- data.table::rbindlist(list(issues_groups, issues_within_db, issues_cross_db), fill = TRUE)
  if (nrow(issues) > 0L) {
    for (i in seq_len(nrow(issues))) log_msg(sprintf("[SCHEMA WARNING] Merge key %s/%s (%s; detected by %s) has incompatible resolved types: %s across %s",
                                                     issues$Database[i], issues$Column[i], issues$Scope[i], issues$Detection[i],
                                                     issues$Types[i], issues$Tables[i]))
    if (isTRUE(strict)) stop("Schema merge-key validation failed.")
  }
  invisible(issues)
}

#' Preflight: build the full cross-database schema catalog before loading
#'
#' Runs schema inference plus the registry for every database in \code{DBLoad}
#' and writes one combined catalog to \code{TableSchemaPath}, so all column
#' types across all databases and tables are established -- and reviewable --
#' before \code{\link{ParquetBackEndCreate}} writes any Parquet. The loader
#' then reads this catalog instead of re-inferring, which also makes resumed
#' runs skip the expensive per-file sampling pass.
#'
#' Curation workflow: open the workbook, change a row's \code{CanonicalType},
#' and set that row's \code{Source} to \code{"manual"}. Manual rows survive
#' subsequent preflight runs and outrank both inference and the registry.
#' A type change only affects future writes; to apply it to a table already on
#' disk, run \code{\link{reset_table_for_reload}} and re-run the loader.
#' Databases not listed in \code{DBLoad} keep their existing catalog rows.
#'
#' Merge-key validation runs on the combined catalog, so join-key type
#' mismatches are caught across databases, not just within one.
#' @param MDT Data frame. Master Database Table.
#' @param DBLoad Character vector of databases to (re)infer. Default NULL means
#'   every database present in \code{MDT}.
#' @param MasterDBPath Character. Root directory of the source files.
#' @param n_workers Integer. Parallel workers for the sampling scans.
#' @param SchemaRegistryPath Character. Registry path (created if missing).
#' @param TableSchemaPath Character. Catalog destination (.xlsx or .csv).
#' @param StrictSchemaValidation Logical. Stop on merge-key type mismatches.
#' @return Invisibly, the loaded catalog (as from
#'   \code{\link{load_table_schema_catalog}}) after the round trip to disk.
#' @export
BuildRepositoryCatalog <- function(MDT, DBLoad = NULL, MasterDBPath, n_workers = 1,
                                   SchemaRegistryPath = NULL, TableSchemaPath = NULL,
                                   StrictSchemaValidation = TRUE, HarvestLabels = TRUE,
                                   LockRepository = TRUE, LockPath = NULL, LockStaleMinutes = 720,
                                   LogPath = NULL, RunId = NULL) {
  previous_run <- if (!is.null(LogPath) || !is.null(RunId)) begin_repository_run(LogPath, RunId) else NULL
  if (!is.null(previous_run)) on.exit(restore_repository_run(previous_run), add = TRUE)
  MDTdt <- data.table::as.data.table(MDT)
  if (is.null(DBLoad)) DBLoad <- unique(as.character(MDTdt$Database))
  #### The preflight writes the catalog and registry; take the same writer   ####
  #### lock the loader uses so a catalog rewrite can never race a loader     ####
  #### finishing on another machine.                                         ####
  if (isTRUE(LockRepository)) {
    if (is.null(LockPath)) {
      LockPath <- if (!is.null(TableSchemaPath) && nzchar(TableSchemaPath)) {
        file.path(dirname(dirname(TableSchemaPath)), ".repository.lock")
      } else { NULL }
    }
    if (!is.null(LockPath)) {
      cat_lock <- acquire_repository_lock(LockPath, stale_minutes = LockStaleMinutes, owner_note = "BuildRepositoryCatalog")
      on.exit(release_repository_lock(cat_lock), add = TRUE)
    } else {
      log_msg("[LOCK] BuildRepositoryCatalog: no TableSchemaPath to derive a lock location from -- proceeding unlocked.")
    }
  }
  schema_registry <- load_schema_registry(SchemaRegistryPath, create_if_missing = TRUE)
  existing <- load_table_schema_catalog(TableSchemaPath, strict = TRUE)
  fresh_rows <- list()
  fresh_labels <- list()
  for (db in DBLoad) {
    MDTSelect <- MDTdt[as.character(Database) == db]
    if (nrow(MDTSelect) == 0L) {
      log_msg(sprintf("[CATALOG] %s: no MDT rows -- skipping", db))
      next
    }
    log_msg(sprintf("[CATALOG] Building schema for %s (%d files)", db, nrow(MDTSelect)))
    schema_obj <- BuildRepositorySchema(MDTSelect = MDTSelect, MasterDBPath = MasterDBPath, Database = db,
                                        n_workers = n_workers, SchemaRegistryPath = SchemaRegistryPath,
                                        TableSchemaPath = NULL, schema_registry = schema_registry,
                                        write_catalog = FALSE, harvest_labels = HarvestLabels)
    fresh_rows[[length(fresh_rows) + 1L]] <- schema_obj$table_schema
    if (!is.null(schema_obj$label_catalog) && nrow(schema_obj$label_catalog) > 0L) {
      fresh_labels[[length(fresh_labels) + 1L]] <- schema_obj$label_catalog
    }
  }
  fresh <- if (length(fresh_rows) > 0L) data.table::rbindlist(fresh_rows, fill = TRUE) else data.table::data.table()
  combined <- merge_table_schema_catalog(fresh, if (is.null(existing)) NULL else existing$table_schema)
  ValidateSchemaMergeKeys(combined, strict = StrictSchemaValidation)
  #### Data dictionary: freshly harvested labels replace those databases'   ####
  #### rows; databases not in DBLoad keep their existing dictionary rows.   ####
  label_combined <- NULL
  if (isTRUE(HarvestLabels)) {
    fresh_lab <- if (length(fresh_labels) > 0L) data.table::rbindlist(fresh_labels, fill = TRUE) else data.table::data.table()
    existing_lab <- load_label_catalog(TableSchemaPath)
    carried_lab <- if (!is.null(existing_lab) && "Database" %in% names(existing_lab)) {
      existing_lab[!existing_lab$Database %in% DBLoad]
    } else { NULL }
    label_combined <- data.table::rbindlist(Filter(Negate(is.null), list(fresh_lab, carried_lab)), fill = TRUE)
    if (nrow(label_combined) == 0L) label_combined <- NULL
  }
  write_table_schema_catalog(combined, TableSchemaPath, label_catalog = label_combined)
  n_manual <- if ("Source" %in% names(combined)) sum(tolower(as.character(combined$Source)) == "manual", na.rm = TRUE) else 0L
  n_labels <- if (is.null(label_combined)) 0L else nrow(label_combined)
  log_msg(sprintf("[CATALOG] Wrote schema catalog: %s (%d tables, %d columns, %d manual override(s) preserved, %d dictionary label(s))",
                  TableSchemaPath, data.table::uniqueN(combined$DuckDBTable), nrow(combined), n_manual, n_labels))
  #### Round-trip through the reader so the returned object matches exactly ####
  #### what ParquetBackEndCreate will consume.                              ####
  invisible(load_table_schema_catalog(TableSchemaPath, strict = TRUE))
}

#' Loader that chooses FileType per file rather than globally
#' @export
generic_db_loader <- function(files, base_path, db_prefix, completed_checkpoint,
                              CheckpointPath, ParquetBasePath, MDTSelect,
                              comprehensive, col_classes = NULL, reader = NULL,
                              PartitionBy, RAMThreshold, SAV_ROW_THRESHOLD,
                              LogPath, SAV_CHUNK_SIZE, PrintStatus = FALSE,
                              ManifestPath = NULL, SchemaRegistryPath = NULL, schema_registry = NULL,
                              TerminalHivePartition = FALSE,
                              MaxFileStemTruncate = FALSE,
                              chunk_size_decrement = NULL, min_chunk_size = NULL,
                              registry_resolved = FALSE, RepositoryLock = NULL,
                              MaxCoerceNAPct = NULL,
                              SourceFingerprintMode = c("metadata", "sha256", "none"),
                              RunId = NULL) {
  SourceFingerprintMode <- match.arg(SourceFingerprintMode)
  MDTSelect <- data.table::as.data.table(MDTSelect)
  MDTSelect[, RepositoryKey := repository_checkpoint_key(
    MDTSelect, MasterDBPath = if (SourceFingerprintMode == "none") NULL else base_path,
    SourceFingerprintMode = SourceFingerprintMode)]
  completed_mask <- checkpoint_completed_mask(
    MDTSelect, completed_checkpoint, accept_legacy = SourceFingerprintMode == "none",
    MasterDBPath = if (SourceFingerprintMode == "none") NULL else base_path,
    SourceFingerprintMode = SourceFingerprintMode)
  pending_meta <- MDTSelect[!completed_mask]
  skipped <- nrow(MDTSelect) - nrow(pending_meta)
  if(skipped > 0){ log_msg(sprintf("Skipping %d already-completed files", skipped)) }
  failures <- list()
  completed_rows <- list()
  for(a in seq_len(nrow(pending_meta))){
    row_meta <- pending_meta[a,]
    source_path <- row_meta$Path[1]
    repository_key <- row_meta$RepositoryKey[1]
    table_name <- NULL; year_val <- NULL; year_dir <- NULL; pspec <- NULL
    suffix <- NULL; table_col_classes <- NULL; completion_status <- NULL
    full_source_path <- source_path_for_row(row_meta, base_path)
    source_meta <- source_fingerprint(full_source_path, mode = SourceFingerprintMode)
    if(PrintStatus){ print(paste("Working on", source_path)) }
    Comp <- FALSE
    empty_ok <- FALSE
    tryCatch({
      suffix <- row_meta$TableName[1]
      all_cols_v <- unique(canonical_colnames(comprehensive[[suffix]]))
      pspec <- partition_spec_for_row(row_meta)
      #### year_val is provenance for the manifest/logs and the value       ####
      #### injected when YEAR is a partition key; NA for tables partitioned ####
      #### by something else.                                               ####
      year_val <- if ("YEAR" %in% pspec$keys) {
        pspec$values[match("YEAR", pspec$keys)]
      } else if ("Year" %in% names(row_meta)) {
        row_meta$Year[1]
      } else { NA }
      table_name <- repository_table_name_for_row(row_meta)
      reader_file <- tolower(row_meta$FileType[1])
      table_col_classes <- if (is.list(col_classes) && !is.null(col_classes[[suffix]])) col_classes[[suffix]] else col_classes
      #### registry_resolved = TRUE means col_classes already carries the    ####
      #### registry (and any manual catalog overrides, which must win) --    ####
      #### re-applying the registry here would clobber manual curation.      ####
      if (!isTRUE(registry_resolved)) {
        if (is.null(schema_registry)) schema_registry <- load_schema_registry(SchemaRegistryPath, create_if_missing = FALSE)
        table_col_classes <- apply_schema_registry(table_col_classes, schema_registry, database = db_prefix, table_name = suffix)
      }
      year_dir <- file.path(ParquetBasePath, table_name, pspec$dir)
      dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
      safe_stem <- parquet_output_stem(source_path, partition_dir = year_dir,
                                       MaxFileStemTruncate = MaxFileStemTruncate)
      out_path <- file.path(year_dir, paste0(safe_stem, ".parquet"))
      if(PrintStatus){ print("Performing database Read") }
      gc(verbose = FALSE)
      touch_repository_lock(RepositoryLock)
      accept_partial <- "AcceptPartial" %in% names(row_meta) &&
        isTRUE(suppressWarnings(as.logical(row_meta$AcceptPartial[1])))
      df <- read_fn(path = source_path, year_dir = year_dir, out_path = out_path, PrintStatus = PrintStatus,
                    all_cols = all_cols_v, col_classes = table_col_classes, year_val = year_val, TerminalHivePartition = TerminalHivePartition,
                    MDTSelect = row_meta, MasterDBPath = base_path, reader = reader_file, PartitionBy = PartitionBy,
                    SAV_ROW_THRESHOLD = SAV_ROW_THRESHOLD, RAMThreshold = RAMThreshold, SAV_CHUNK_SIZE = SAV_CHUNK_SIZE,
                    chunk_size_decrement = chunk_size_decrement, min_chunk_size = min_chunk_size,
                    partition_keys = pspec$keys, partition_values = pspec$values,
                    max_coerce_na_pct = MaxCoerceNAPct,
                    accept_partial = accept_partial,
                    ManifestPath = ManifestPath, Database = db_prefix, TableName = suffix,
                    DuckDBTable = table_name, SourcePath = source_path,
                    SchemaHash = schema_hash_from_classes(table_col_classes),
                    MaxFileStemTruncate = MaxFileStemTruncate,
                    RepositoryLock = RepositoryLock)
      gc(verbose = FALSE)
      if (is.list(df) && isTRUE(df$written)) {
        if (identical(df$n_rows, 0L)) empty_ok <- TRUE
        completion_status <- as.character(df$status %||% if (empty_ok) "empty" else "completed")
        log_msg(sprintf("[OK] %d/%d  %s -> %s (%s, %d rows)", a, nrow(pending_meta), source_path, table_name, pspec$dir, df$n_rows))
      } else {
        if (is.list(df) && !is.null(df$data) && isFALSE(df$written)) {
          pre_aligned <- isTRUE(df$pre_aligned)
          df <- df$data
        } else {
          pre_aligned <- FALSE
        }
        if (!is.data.frame(df) || nrow(df) == 0) {
          #### Verified empty vs failed read: safe_read_sav/safe_read_csv    ####
          #### return an empty frame on read ERRORS too, so re-verify        ####
          #### directly against the source before declaring the file empty.  ####
          full_source_path <- file.path(base_path, row_meta$MDBDir[1], source_path)
          if (is.data.frame(df) && verify_source_empty(
              full_source_path, reader_file, reader_options_for_row(row_meta))) {
            log_msg(sprintf("[WARN] %d/%d  %s: source file verified empty (0 rows) -- recording as complete", a, nrow(pending_meta), source_path))
            empty_ok <- TRUE
            stop("__EMPTY_OK__")
          }
          log_msg(sprintf("[FAIL] %d/%d  %s: 0 rows", a, nrow(pending_meta), source_path))
          stop("__SKIP__")
        }
        if(PrintStatus){ print("Performing Column type alignment") }
        if (!pre_aligned) {
          if ("YEAR" %in% pspec$keys) df <- add_year_if_missing(df, year_val)
          df <- align_columns(df, all_cols_v, table_col_classes, max_coerce_na_pct = MaxCoerceNAPct)
        }
        if(PrintStatus){ print("Writing complete parquet file") }
        n_rows_written <- nrow(df)
        out_file <- write_year_parquet(df = df, ParquetBasePath = ParquetBasePath, table_name = table_name,
                                       year_val = year_val, source_path = source_path, col_classes = table_col_classes,
                                       MaxFileStemTruncate = MaxFileStemTruncate,
                                       partition_keys = pspec$keys, partition_values = pspec$values,
                                       max_coerce_na_pct = MaxCoerceNAPct)
        update_parquet_manifest(ManifestPath = ManifestPath, Database = db_prefix, TableName = suffix,
                                DuckDBTable = table_name, Year = year_val, SourcePath = source_path,
                                ParquetPath = out_file, NRows = n_rows_written,
                                SchemaHash = schema_hash_from_classes(table_col_classes),
                                Status = "written", Notes = "single_file",
                                PartitionKey = pspec$keys, PartitionValue = pspec$values,
                                RunId = RunId, RepositoryKey = repository_key,
                                SourceSize = source_meta$size, SourceMTimeUTC = source_meta$mtime_utc,
                                SourceSHA256 = source_meta$sha256, SourceFingerprint = source_meta$fingerprint)
        rm(df); gc(verbose = FALSE)
        log_msg(sprintf("[OK] %d/%d  %s -> %s (year=%s)", a, nrow(pending_meta), source_path, table_name, year_val))
        completion_status <- "completed"
      }
      Comp <- TRUE
    }, error = function(e) {
      #### "__EMPTY_OK__" is the verified-empty escape: the source file      ####
      #### provably contains 0 rows, so it completes (and checkpoints)       ####
      #### without writing Parquet. "__SKIP__" stays a silent failure.       ####
      if (identical(conditionMessage(e), "__EMPTY_OK__")) {
        Comp <<- TRUE
        return(invisible(NULL))
      }
      if (!identical(conditionMessage(e), "__SKIP__")) {
        log_msg(sprintf("[ERROR] %d/%d  %s: %s", a, nrow(pending_meta), source_path, conditionMessage(e)))
      }
      failures[[length(failures) + 1L]] <<- data.table::data.table(
        Database = as.character(db_prefix), TableName = as.character(suffix %||% row_meta$TableName[1]),
        DuckDBTable = as.character(table_name %||% repository_table_name_for_row(row_meta)),
        SourcePath = as.character(source_path), RepositoryKey = as.character(repository_key),
        Message = if (identical(conditionMessage(e), "__SKIP__")) "Reader returned zero rows or failed." else conditionMessage(e))
      Comp <<- FALSE
    })
    print(paste("Comp is:", Comp))
    if(Comp){
      if (!is.null(table_name) && !is.null(year_dir)) {
        final_status <- if (isTRUE(empty_ok)) "empty" else completion_status %||% "completed"
        final_notes <- switch(final_status,
                              empty = "verified_empty_source",
                              partial_accepted = "checkpoint_complete_partial_accepted",
                              "checkpoint_complete")
        update_parquet_manifest(ManifestPath = ManifestPath, Database = db_prefix, TableName = suffix,
                                DuckDBTable = table_name, Year = year_val, SourcePath = source_path,
                                ParquetPath = year_dir,
                                NRows = if (isTRUE(empty_ok)) 0 else NA_real_,
                                SchemaHash = schema_hash_from_classes(table_col_classes),
                                Status = final_status, Notes = final_notes,
                                PartitionKey = if (!is.null(pspec)) pspec$keys else NA_character_,
                                PartitionValue = if (!is.null(pspec)) pspec$values else NA_character_,
                                RunId = RunId, RepositoryKey = repository_key,
                                SourceSize = source_meta$size, SourceMTimeUTC = source_meta$mtime_utc,
                                SourceSHA256 = source_meta$sha256, SourceFingerprint = source_meta$fingerprint)
      }
      repository_base <- sub("\\|\\|SOURCE=.*$", "", repository_key)
      completed_base <- sub("\\|\\|SOURCE=.*$", "", completed_checkpoint)
      completed_checkpoint <- c(completed_checkpoint[completed_base != repository_base], repository_key)
      save_checkpoint(completed_checkpoint, CheckpointPath)
      completed_rows[[length(completed_rows) + 1L]] <- data.table::data.table(
        Database = as.character(db_prefix), TableName = as.character(suffix),
        DuckDBTable = as.character(table_name), SourcePath = as.character(source_path),
        RepositoryKey = as.character(repository_key), Status = as.character(completion_status %||% if (empty_ok) "empty" else "completed"))
      if(PrintStatus){ print(paste("Checkpoint save completed")) }
      log_msg(sprintf("[CHECKPOINT] %s: %s", repository_key, table_name))
    }
    touch_repository_lock(RepositoryLock)
    gc(verbose = FALSE)
  }
  structure(list(
    checkpoint = unique(completed_checkpoint),
    completed = if (length(completed_rows) > 0L) data.table::rbindlist(completed_rows, fill = TRUE) else data.table::data.table(),
    failures = if (length(failures) > 0L) data.table::rbindlist(failures, fill = TRUE) else data.table::data.table()
  ), class = "RepositoryLoadResult")
}

write_repository_run_summary <- function(result, path) {
  if (is.null(path) || !nzchar(path)) return(invisible(NULL))
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  serializable <- unclass(result)
  serializable$completed <- as.data.frame(serializable$completed)
  serializable$file_failures <- as.data.frame(serializable$file_failures)
  serializable$database_failures <- as.data.frame(serializable$database_failures)
  if (tolower(tools::file_ext(path)) == "json" && requireNamespace("jsonlite", quietly = TRUE)) {
    tmp <- tempfile(pattern = paste0(basename(path), ".tmp_"), tmpdir = dirname(path), fileext = ".json")
    jsonlite::write_json(serializable, tmp, pretty = TRUE, auto_unbox = TRUE, na = "null")
    replace_file_safely(tmp, path)
  } else {
    tmp <- tempfile(pattern = paste0(basename(path), ".tmp_"), tmpdir = dirname(path), fileext = ".rds")
    saveRDS(serializable, tmp)
    replace_file_safely(tmp, path)
  }
  invisible(path)
}

print.RepositoryRunResult <- function(x, ...) {
  cat(sprintf("Repository run %s: %s; %d completed source(s), %d file failure(s), %d database failure(s).\n",
              x$run_id, x$status, nrow(x$completed), nrow(x$file_failures), nrow(x$database_failures)))
  invisible(x)
}

#' Orchestrate repository schema first, then write Parquet
#' @export
ParquetBackEndCreate <- function(MDT, DBLoad, MasterDBPath, completed_checkpoint, CheckpointPath, ParquetBasePath, SAV_ROW_THRESHOLD = 1000000L,
                                 PartitionBy, RAMThreshold, SAV_CHUNK_SIZE = 1000000L, LogPath, n_workers = 1, PrintStatus = FALSE,
                                 TerminalHivePartition = FALSE, MaxFileStemTruncate = TRUE,
                                 chunk_size_decrement = NULL, min_chunk_size = NULL,
                                 SchemaRegistryPath = NULL, TableSchemaPath = NULL, ManifestPath = NULL,
                                 StrictPreflight = TRUE, StrictSchemaValidation = TRUE,
                                 UseSchemaCatalog = TRUE,
                                 LockRepository = TRUE, LockPath = NULL, LockStaleMinutes = 720,
                                 RunPreflight = TRUE,
                                 SnapshotState = TRUE, StateBackupDir = NULL, SnapshotKeep = 20L,
                                 StopOnDatabaseError = TRUE, StopOnFileError = TRUE,
                                 MaxCoerceNAPct = NULL,
                                 SourceFingerprintMode = c("metadata", "sha256", "none"),
                                 ReturnRunResult = FALSE, RunId = NULL, RunSummaryPath = NULL) {
  PartitionBy <- match.arg(PartitionBy, c("NRows", "RAMEstimate", "FAIL"))
  SourceFingerprintMode <- match.arg(SourceFingerprintMode)
  previous_run <- begin_repository_run(LogPath = LogPath, RunId = RunId)
  on.exit(restore_repository_run(previous_run), add = TRUE)
  RunId <- resolve_run_id()
  run_started_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  #### Fresh coercion-damage accounting for this run; the aggregated report ####
  #### is written next to the manifest at the end.                          ####
  coercion_report_reset()
  log_msg(sprintf("Parallel workers available: %d (scoped per scan)", n_workers), log_path = LogPath)
  if (is.null(ManifestPath)) ManifestPath <- manifest_path_default(ParquetBasePath)
  if (is.null(TableSchemaPath)) TableSchemaPath <- file.path(dirname(ParquetBasePath), "Schema", "TableSchemas.xlsx")
  if (is.null(RunSummaryPath)) {
    RunSummaryPath <- file.path(dirname(ManifestPath), "RunSummaries", paste0("run_", RunId, ".json"))
  }
  coercion_path <- file.path(dirname(ManifestPath), "CoercionReport.csv")
  coercion_written <- FALSE
  on.exit(if (!coercion_written) try(coercion_report_write(coercion_path), silent = TRUE), add = TRUE)
  on.exit(try(flush_log_buffer(log_path = LogPath), silent = TRUE), add = TRUE)
  repo_lock <- NULL
  if (isTRUE(LockRepository)) {
    if (is.null(LockPath)) LockPath <- repository_lock_path_default(ParquetBasePath)
    repo_lock <- acquire_repository_lock(LockPath, stale_minutes = LockStaleMinutes, owner_note = "ParquetBackEndCreate")
    on.exit(release_repository_lock(repo_lock), add = TRUE)
  }
  if (SourceFingerprintMode != "none") {
    upgraded <- upgrade_checkpoint_source_fingerprints(
      completed_checkpoint, MDT, MasterDBPath, SourceFingerprintMode = SourceFingerprintMode)
    if (!identical(sort(unique(upgraded)), sort(unique(completed_checkpoint)))) {
      n_changed <- length(setdiff(upgraded, completed_checkpoint))
      log_msg(sprintf(paste0("[CHECKPOINT MIGRATION] Anchored %d legacy checkpoint entrie(s) to the current source %s fingerprints. ",
                             "Future source changes will invalidate those entries."),
                      n_changed, SourceFingerprintMode), log_path = LogPath)
      completed_checkpoint <- upgraded
      save_checkpoint(completed_checkpoint, CheckpointPath)
    }
  }
  #### Snapshot the repository's bookkeeping (checkpoint, manifest, catalog, ####
  #### registry) before this run can touch any of it, so divergence found by ####
  #### audit_repository() is recoverable, not just detectable.               ####
  if (isTRUE(SnapshotState)) {
    if (is.null(StateBackupDir)) StateBackupDir <- file.path(dirname(ParquetBasePath), "StateBackups")
    snapshot_repository_state(CheckpointPath = CheckpointPath, ManifestPath = ManifestPath,
                              TableSchemaPath = TableSchemaPath, SchemaRegistryPath = SchemaRegistryPath,
                              BackupDir = StateBackupDir, keep_last = SnapshotKeep)
  }
  #### RunPreflight = FALSE skips the loader's own structural MDT/output     ####
  #### validation pass when the caller has already run it explicitly.        ####
  if (isTRUE(RunPreflight)) {
    ValidateMDTPreflight(MDT, strict = StrictPreflight, logStatus = TRUE,
                         ParquetBasePath = ParquetBasePath,
                         MaxFileStemTruncate = MaxFileStemTruncate,
                         TerminalHivePartition = TerminalHivePartition,
                         MasterDBPath = MasterDBPath)
  } else {
    log_msg("[PREFLIGHT] Skipped inside loader (RunPreflight = FALSE) -- caller is responsible for having run ValidateMDTPreflight.", log_path = LogPath)
  }
  schema_registry <- load_schema_registry(SchemaRegistryPath, create_if_missing = TRUE)
  catalog <- if (isTRUE(UseSchemaCatalog)) load_table_schema_catalog(TableSchemaPath, strict = TRUE) else NULL
  if (!is.null(catalog)) {
    log_msg(sprintf("[SCHEMA CATALOG] Using reviewed column types from %s.", TableSchemaPath), log_path = LogPath)
  } else if (isTRUE(UseSchemaCatalog)) {
    stop(paste0("UseSchemaCatalog=TRUE but no finalized table schema catalog exists at ",
                TableSchemaPath, ". Run PrepareSchemaRegistry() and FinalizeSchemaRegistry() before loading."))
  }
  all_schema_rows <- list()
  database_failures <- list()
  file_failures <- list()
  completed_rows <- list()
  record_failure <- function(db, e, phase) {
    database_failures[[length(database_failures) + 1L]] <<- data.table::data.table(
      Database = as.character(db), Phase = as.character(phase), Message = conditionMessage(e))
    log_msg(sprintf("[ERROR] %s failed during %s: %s", db, phase, conditionMessage(e)), log_path = LogPath)
    log_msg(sprintf("[ERROR] Call: %s", paste(deparse(conditionCall(e)), collapse = " ")), log_path = LogPath)
  }
  #### Phase 1: build every database's schema BEFORE any Parquet is written, ####
  #### so combined cross-database merge-key validation can stop the run      ####
  #### while stopping is still free. (Previously the combined check only ran ####
  #### after loading, when incompatible files were already on disk.)         ####
  schema_objs <- list()
  for (f in seq_along(DBLoad)) {
    db <- DBLoad[f]
    if (nrow(MDT[MDT$Database == db, ]) == 0L) {
      log_msg(paste("=== Skipping", db, "No files in MDT to add ==="), log_path = LogPath)
      next
    }
    log_msg(paste("=== Building schema:", db, "==="), log_path = LogPath)
    touch_repository_lock(repo_lock)
    tryCatch({
      MDTSelect <- MDT[MDT$Database == db, ]
      schema_objs[[db]] <- BuildRepositorySchema(MDTSelect = MDTSelect, MasterDBPath = MasterDBPath, Database = db,
                                                 n_workers = n_workers, SchemaRegistryPath = SchemaRegistryPath,
                                                 TableSchemaPath = NULL, schema_registry = schema_registry,
                                                 write_catalog = FALSE,
                                                 known_col_classes = if (!is.null(catalog)) catalog$col_classes[[db]] else NULL)
      all_schema_rows[[length(all_schema_rows) + 1L]] <- schema_objs[[db]]$table_schema
      touch_repository_lock(repo_lock)
    }, error = function(e) record_failure(db, e, "schema inference"))
  }
  #### Combined validation across every database in this run, plus the       ####
  #### existing catalog's rows for databases not being reloaded.             ####
  if (length(all_schema_rows) > 0L) {
    combined_schema <- merge_table_schema_catalog(data.table::rbindlist(all_schema_rows, fill = TRUE),
                                                  if (!is.null(catalog)) catalog$table_schema else NULL)
    ValidateSchemaMergeKeys(combined_schema, strict = StrictSchemaValidation)
  }
  #### Phase 2: load. ####
  for (db in names(schema_objs)) {
    log_msg(paste("=== Loading", db, "==="), log_path = LogPath)
    tryCatch({
      MDTSelect <- MDT[MDT$Database == db, ]
      if(PrintStatus){ print("Performing database integration") }
      load_result <- generic_db_loader(files = MDTSelect$Path,
                                                base_path = MasterDBPath,
                                                completed_checkpoint = completed_checkpoint,
                                                CheckpointPath = CheckpointPath,
                                                ParquetBasePath = ParquetBasePath,
                                                db_prefix = unique(MDTSelect$Database),
                                                MDTSelect = MDTSelect,
                                                comprehensive = schema_objs[[db]]$comprehensive,
                                                col_classes = schema_objs[[db]]$col_classes,
                                                reader = NULL,
                                                PartitionBy = PartitionBy,
                                                RAMThreshold = RAMThreshold,
                                                SAV_ROW_THRESHOLD = SAV_ROW_THRESHOLD,
                                                LogPath = LogPath,
                                                PrintStatus = PrintStatus,
                                                ManifestPath = ManifestPath,
                                                SchemaRegistryPath = SchemaRegistryPath,
                                                schema_registry = schema_registry,
                                                MaxFileStemTruncate = MaxFileStemTruncate,
                                                TerminalHivePartition = TerminalHivePartition,
                                                SAV_CHUNK_SIZE = SAV_CHUNK_SIZE,
                                                chunk_size_decrement = chunk_size_decrement,
                                                min_chunk_size = min_chunk_size,
                                                registry_resolved = TRUE,
                                                RepositoryLock = repo_lock,
                                                MaxCoerceNAPct = MaxCoerceNAPct,
                                                SourceFingerprintMode = SourceFingerprintMode,
                                                RunId = RunId)
      completed_checkpoint <- load_result$checkpoint
      if (nrow(load_result$completed) > 0L) completed_rows[[length(completed_rows) + 1L]] <- load_result$completed
      if (nrow(load_result$failures) > 0L) file_failures[[length(file_failures) + 1L]] <- load_result$failures
      log_msg(paste(db, "complete. Checkpoint size:", length(completed_checkpoint)), log_path = LogPath)
    }, error = function(e) {
      record_failure(db, e, "loading")
      n_saved <- tryCatch(length(load_checkpoint(path = CheckpointPath)), error = function(e2) NA_integer_)
      log_msg(sprintf("[ERROR] Checkpoint on disk: %s files. Continuing to next database.", ifelse(is.na(n_saved), "unknown", as.character(n_saved))), log_path = LogPath)
    })
  }
  if (length(all_schema_rows) > 0L) {
    #### Merge instead of overwrite: keep manual rows and databases that ####
    #### were not part of this DBLoad.                                   ####
    final_schema <- merge_table_schema_catalog(data.table::rbindlist(all_schema_rows, fill = TRUE),
                                               if (!is.null(catalog)) catalog$table_schema else NULL)
    ValidateSchemaMergeKeys(final_schema, strict = StrictSchemaValidation)
    write_table_schema_catalog(final_schema, TableSchemaPath)
    log_msg(sprintf("[SCHEMA CATALOG] Wrote table schema catalog: %s", TableSchemaPath), log_path = LogPath)
  }
  db_failures <- if (length(database_failures) > 0L) data.table::rbindlist(database_failures, fill = TRUE) else data.table::data.table()
  source_failures <- if (length(file_failures) > 0L) data.table::rbindlist(file_failures, fill = TRUE) else data.table::data.table()
  completed_dt <- if (length(completed_rows) > 0L) data.table::rbindlist(completed_rows, fill = TRUE) else data.table::data.table()
  status <- if (nrow(db_failures) == 0L && nrow(source_failures) == 0L) "success" else "partial_failure"
  run_result <- structure(list(
    run_id = RunId, status = status, started_at = run_started_at, finished_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z"),
    checkpoint = unique(completed_checkpoint), completed = completed_dt,
    file_failures = source_failures, database_failures = db_failures,
    ManifestPath = ManifestPath, TableSchemaPath = TableSchemaPath,
    CheckpointPath = CheckpointPath, LogPath = LogPath
  ), class = "RepositoryRunResult")
  write_repository_run_summary(run_result, RunSummaryPath)
  if (nrow(db_failures) > 0L) {
    summary <- paste(sprintf("%s: %s", db_failures$Database, db_failures$Message), collapse = " | ")
    log_msg(sprintf("[RUN FAILED] %d database phase(s) failed: %s", nrow(db_failures), summary), log_path = LogPath)
  }
  if (nrow(source_failures) > 0L) {
    summary <- paste(sprintf("%s: %s", source_failures$SourcePath, source_failures$Message), collapse = " | ")
    log_msg(sprintf("[RUN FAILED] %d source file(s) failed: %s", nrow(source_failures), summary), log_path = LogPath)
  }
  coercion_report_write(coercion_path)
  coercion_written <- TRUE
  log_msg(paste("ParquetBackEndCreate function complete. Final checkpoint size:", length(completed_checkpoint)), log_path = LogPath)
  flush_log_buffer(log_path = LogPath)
  if (nrow(db_failures) > 0L && isTRUE(StopOnDatabaseError)) {
    stop(sprintf("ParquetBackEndCreate failed for %d database phase(s). Run summary: %s", nrow(db_failures), RunSummaryPath), call. = FALSE)
  }
  if (nrow(source_failures) > 0L && isTRUE(StopOnFileError)) {
    stop(sprintf("ParquetBackEndCreate failed for %d source file(s). Run summary: %s", nrow(source_failures), RunSummaryPath), call. = FALSE)
  }
  if (status != "success") log_msg("[RUN PARTIAL] Failure stopping is disabled; returning the partial run result/checkpoint.", log_path = LogPath)
  if (isTRUE(ReturnRunResult)) run_result else completed_checkpoint
}
