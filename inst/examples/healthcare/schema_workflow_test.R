################################################################################
#### Clean schema-workflow test helpers ########################################
################################################################################

RepoquetSourcePath <- Sys.getenv(
  "REPOQUET_SOURCE",
  unset = "C:/Users/breng/Dropbox/github/repoquet/R/repoquet.R"
)
if (!file.exists(RepoquetSourcePath)) {
  stop("repoquet development source not found: ", RepoquetSourcePath,
       ". Set REPOQUET_SOURCE to the cloned repository's R/repoquet.R file.")
}
source(RepoquetSourcePath, local = .GlobalEnv)

run_schema_workflow_test <- function(MDT, MasterDBPath, ObservationPath,
                                     SchemaReviewPath, SchemaRegistryPath = NULL,
                                     SchemaProfile = "none", n_workers = 1L,
                                     LogPath = NULL, RunId = NULL) {
  DBLoad <- sort(unique(as.character(MDT$Database)))
  PrepareSchemaRegistry(
    MDT = MDT,
    DBLoad = DBLoad,
    MasterDBPath = MasterDBPath,
    ObservationPath = ObservationPath,
    SchemaReviewPath = SchemaReviewPath,
    SchemaRegistryPath = SchemaRegistryPath,
    SchemaProfile = SchemaProfile,
    n_workers = n_workers,
    SourceFingerprintMode = "metadata",
    StrictReaders = FALSE,
    LogPath = LogPath,
    RunId = RunId
  )
}

finalize_schema_workflow_test <- function(SchemaReviewPath, TableSchemaPath) {
  FinalizeSchemaRegistry(
    SchemaReviewPath = SchemaReviewPath,
    TableSchemaPath = TableSchemaPath,
    strict = TRUE
  )
}
