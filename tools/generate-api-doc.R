source_file <- "R/repoquet.R"
if (!file.exists(source_file)) stop("Run this script from the repository root.")

env <- new.env(parent = globalenv())
sys.source(source_file, envir = env)
exports <- sort(ls(env, pattern = "^[[:alpha:]]+"))

lines <- c(
  "\\name{repoquet-api}",
  "\\alias{repoquet-api}",
  sprintf("\\alias{%s}", exports),
  "\\title{repoquet Repository Workflow API}",
  "\\description{",
  "Functions used to initialize, load, validate, catalog, audit, and query a",
  "schema-normalized Parquet repository. Low-level helpers remain exported for",
  "compatibility with the original script-based workflow.",
  "}",
  "\\details{",
  "Start with \\code{create_repository_project()} for a scaffolded project,",
  "\\code{RepositoryInitialize()} for an existing project, and",
  "\\code{ValidateMDTPreflight()} before calling",
  "\\code{ParquetBackEndCreate()}. Use \\code{BuildRepositoryCatalog()},",
  "\\code{audit_repository()}, and \\code{validate_data_contracts()} to inspect",
  "and validate the resulting repository. See the package README for complete",
  "workflow examples and configuration fields.",
  "}",
  "\\keyword{database}",
  "\\keyword{utilities}"
)

dir.create("man", recursive = TRUE, showWarnings = FALSE)
writeLines(lines, "man/repoquet-api.Rd", useBytes = TRUE)
message("Documented ", length(exports), " exported objects in man/repoquet-api.Rd")
