if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv", repos = "https://cloud.r-project.org")
}

renv::consent(provided = TRUE)
renv::activate()

if (file.exists("renv.lock")) {
  renv::restore(prompt = FALSE)
} else {
  renv::init(bare = TRUE, restart = FALSE)
  description <- read.dcf("DESCRIPTION")
  fields <- intersect(c("Imports", "Suggests"), colnames(description))
  packages <- unique(trimws(unlist(strsplit(
    paste(description[1, fields], collapse = ","), ","
  ))))
  packages <- sub("\\s*\\(.*$", "", packages)
  packages <- packages[nzchar(packages) & packages != "R"]
  packages[packages == "ComplexHeatmap"] <- "bioc::ComplexHeatmap"
  renv::install(packages)
  renv::snapshot(type = "all", prompt = FALSE)
}
