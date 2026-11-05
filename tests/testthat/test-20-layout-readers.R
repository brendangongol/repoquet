#### SPSS load-program layouts (LayoutFile) and the fixed-width "asc" reader ####
#### Synthetic files only: the real HCUP data is restricted and never used.  ####

write_text_file <- function(path, lines, eol = "\n", final_eol = TRUE) {
  body <- paste(lines, collapse = eol)
  if (final_eol) body <- paste0(body, eol)
  con <- file(path, open = "wb")
  on.exit(close(con))
  writeBin(charToRaw(body), con)
  invisible(path)
}

#### A fixed-width program: 23-character records, special codes on three fields.
write_fixed_sps <- function(path) {
  writeLines(c(
    "/*****************************************************************************",
    "* SPSSload_DEMO.SPS -- synthetic",
    "*****************************************************************************/",
    "",
    "*** Read data elements from the ASCII file ***.",
    "DATA LIST FILE = 'DEMO.ASC' FIXED /",
    "    ID                       1-   4",
    "    NAME                     5-   9 (A)",
    "    FLAG                    10-  11",
    "    WEIGHT                  12-  20",
    "    CODE                    21-  23 (A).",
    "",
    "***  Specify write formats  ***.",
    "FORMATS",
    "    ID                       (F4.0) /",
    "    FLAG                     (F2.0) /",
    "    WEIGHT                   (F9.4) .",
    "",
    "***  Assign labels to the data elements ***.",
    "VARIABLE LABELS",
    "    ID                       \"Identifier.\"",
    "    NAME                     \"Name\" .",
    "",
    "*** Convert special values to system missing values ***.",
    "RECODE ID                      (-999 -888=SYSMIS).",
    "RECODE FLAG                    (-9 -8=SYSMIS).",
    "RECODE WEIGHT                  (-99.9999 -88.8888=SYSMIS)."), path)
  invisible(path)
}

asc_demo_line <- function(id, name, flag, weight, code) {
  paste0(formatC(id, width = 4), formatC(name, width = -5), formatC(flag, width = 2),
         formatC(weight, format = "f", digits = 4, width = 9), formatC(code, width = -3))
}

asc_demo_lines <- function() {
  c(asc_demo_line(1, "AB", 1, 1.5, "007"),
    asc_demo_line(2, "  CD", 0, 2.25, "A"),
    asc_demo_line(-999, "", -9, -99.9999, ""),
    asc_demo_line(4, "EFGHI", 1, 4, "12"),
    asc_demo_line(5, "J", -8, -88.8888, "X"),
    asc_demo_line(6, "K", 1, 6.125, "Y"),
    asc_demo_line(7, "L", 0, 7, "Z"))
}

asc_demo_expected <- function() {
  data.table::data.table(
    ID = c(1, 2, NA, 4, 5, 6, 7),
    NAME = c("AB", "  CD", "", "EFGHI", "J", "K", "L"),
    FLAG = c(1, 0, NA, 1, NA, 1, 0),
    WEIGHT = c(1.5, 2.25, NA, 4, NA, 6.125, 7),
    CODE = c("007", "A", "", "12", "X", "Y", "Z"))
}

#### A delimited program for a headerless CSV.
write_csv_sps <- function(path) {
  writeLines(c(
    "/*****************************************************************************/",
    "/* SPSSload_DEMO.SPS -- synthetic                                           */",
    "/*****************************************************************************/",
    "",
    "/* Read data elements from the csv file */",
    "get data /type = txt",
    "    /file = \"DEMO.csv\"",
    "    /delimiters = ','",
    "    /variables =",
    "    ID                       F3.0",
    "    CODE                     A4",
    "    AMT                      F8.2",
    "    LABEL                    A6",
    ".",
    "",
    "VARIABLE LABELS",
    "    ID                       \"Identifier\" .",
    "",
    "/* Convert special values to system missing values */",
    "RECODE ID                      (-99 -88=SYSMIS).",
    "RECODE AMT                     (-99.99 -88.88=SYSMIS)."), path)
  invisible(path)
}

csv_demo_lines <- function() {
  c("1,0012,10.5,abc", "-99,0099,-99.99,def", "3,,-88.88,", "4,0100,7,ghi", "-88,0007,8.25,jkl")
}

new_layout_dir <- function() {
  root <- tempfile("layouts_")
  dir.create(root)
  root
}

collect_chunks <- function(...) {
  chunks <- list()
  info <- .stream_asc_records(..., callback = function(df) chunks[[length(chunks) + 1L]] <<- df)
  list(chunks = chunks, info = info)
}

test_that("a fixed-width load program parses to fields, record length, and special codes", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  layout <- .parse_spss_layout(write_fixed_sps(file.path(root, "demo.SPS")))
  expect_identical(layout$Style, "fixed")
  expect_identical(layout$Fields$Name, c("ID", "NAME", "FLAG", "WEIGHT", "CODE"))
  expect_identical(layout$Fields$Start, c(1L, 5L, 10L, 12L, 21L))
  expect_identical(layout$Fields$End, c(4L, 9L, 11L, 20L, 23L))
  expect_identical(layout$Fields$Type, c("numeric", "character", "numeric", "numeric", "character"))
  expect_identical(layout$RecordLength, 23L)
  expect_identical(sort(names(layout$Missing)), c("FLAG", "ID", "WEIGHT"))
  expect_equal(unname(layout$Missing$ID), rbind(c(-999, -999), c(-888, -888)))
  expect_equal(unname(layout$Missing$WEIGHT), rbind(c(-99.9999, -99.9999), c(-88.8888, -88.8888)))
})

test_that("a delimited load program parses to names, types, delimiter, and special codes", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  layout <- .parse_spss_layout(write_csv_sps(file.path(root, "demo.SPS")))
  expect_identical(layout$Style, "csv")
  expect_identical(layout$Delimiter, ",")
  expect_identical(layout$Fields$Name, c("ID", "CODE", "AMT", "LABEL"))
  expect_identical(layout$Fields$Type, c("numeric", "character", "numeric", "character"))
  expect_true(is.na(layout$RecordLength))
  expect_identical(sort(names(layout$Missing)), c("AMT", "ID"))
})

test_that("RECODE value lists support THRU, LO, and HI; unsupported forms fail clearly", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  sps <- function(recode) {
    path <- file.path(root, "r.SPS")
    writeLines(c("DATA LIST FILE = 'X.ASC' FIXED /", "    A   1-  3", "    B   4-  8 (2)", "    C   9- 10 (A).",
                 recode), path)
    path
  }
  layout <- .parse_spss_layout(sps(c("RECODE A (-9 THRU -5 99 LO THRU -100 900 THRU HI=SYSMIS).",
                                     "RECODE a b (-1=SYSMIS).")))
  expect_equal(unname(layout$Missing$A), rbind(c(-9, -5), c(99, 99), c(-Inf, -100), c(900, Inf), c(-1, -1)))
  expect_equal(unname(layout$Missing$B), rbind(c(-1, -1)))
  expect_identical(layout$Fields$Decimals, c(0L, 2L, 0L))
  expect_error(.parse_spss_layout(sps("RECODE A (1=2).")), "RECODE clause .* is not supported")
  expect_error(.parse_spss_layout(sps("RECODE A (MISSING=SYSMIS).")), "RECODE value 'MISSING'")
  expect_error(.parse_spss_layout(sps("RECODE Z (-1=SYSMIS).")), "'Z', which is not a variable")
  expect_error(.parse_spss_layout(sps("RECODE A (-1=SYSMIS) INTO D.")), "RECODE statement .* is not supported")

  free <- file.path(root, "free.SPS")
  writeLines(c("DATA LIST FILE = 'X.ASC' FREE /", "    A   1-  3."), free)
  expect_error(.parse_spss_layout(free), "is not FIXED")
  none <- file.path(root, "none.SPS")
  writeLines(c("* nothing to load here.", "EXECUTE."), none)
  expect_error(.parse_spss_layout(none), "unrecognized layout program")
  junk <- file.path(root, "junk.SPS")
  writeLines(c("DATA LIST FILE = 'X.ASC' FIXED /", "    A   1-  3", "    @@ ??", "    B   4-  5."), junk)
  expect_error(.parse_spss_layout(junk), "unrecognized text in the DATA LIST block")
  multi <- file.path(root, "multi.SPS")
  writeLines(c("DATA LIST FILE = 'X.ASC' FIXED RECORDS=2 /", "    A   1-  3", "/2", "    B   1-  5."), multi)
  expect_error(.parse_spss_layout(multi), "multi-record")
  dup <- file.path(root, "dup.SPS")
  writeLines(c("DATA LIST FILE = 'X.ASC' FIXED /", "    A   1-  3", "    a   4-  5."), dup)
  expect_error(.parse_spss_layout(dup), "defined more than once")
  missing_file <- file.path(root, "nope.SPS")
  expect_error(.load_layout(missing_file), "Layout file not found")
})

test_that("the public HCUP load programs parse with the expected record lengths", {
  base <- Sys.getenv("REPOQUET_HCUP_LOAD_PROGRAMS", unset = "")
  if (!nzchar(base) && !is.na(repoquet_root)) {
    base <- file.path(dirname(repoquet_root), "CECORC", "demo", "DatabaseLoad", "healthcare",
                      "hcup_load_programs")
  }
  testthat::skip_if_not(dir.exists(base), "HCUP load programs not found; set REPOQUET_HCUP_LOAD_PROGRAMS")
  nis <- file.path(base, "nis_spss_load_pgms")
  nrd <- file.path(base, "nrd_spss_load_prgms")
  grps19 <- .parse_spss_layout(file.path(nis, "SPSSLoad_NIS_2019_DX_PR_GRPS_V1.SPS"))
  expect_identical(grps19$Style, "fixed")
  expect_identical(nrow(grps19$Fields), 918L)
  expect_identical(grps19$RecordLength, 1026L)
  expect_identical(.parse_spss_layout(file.path(nis, "SPSSLoad_NIS_2020_DX_PR_GRPS_V1.SPS"))$RecordLength, 1027L)
  expect_identical(.parse_spss_layout(file.path(nis, "SPSSLoad_NIS_2022_DX_PR_GRPS.SPS"))$RecordLength, 1030L)
  expect_identical(grps19$Fields$Type[grps19$Fields$Name == "CMR_VERSION"], "character")
  core19 <- .parse_spss_layout(file.path(nis, "SPSSLoad_NIS_2019_Core_V1.SPS"))
  expect_identical(core19$Fields[core19$Fields$Name == "DISCWT", c("Start", "End")],
                   data.frame(Start = 12L, End = 22L)[1, ], ignore_attr = TRUE)
  expect_equal(unname(core19$Missing$DISCWT),
               rbind(c(-99.9999999, -99.9999999), c(-88.8888888, -88.8888888), c(-66.6666666, -66.6666666)))
  expect_equal(unname(core19$Missing$AGE), rbind(c(-99, -99), c(-88, -88), c(-66, -66)))
  nrd_core <- .parse_spss_layout(file.path(nrd, "SPSSLoad_NRD_2020_Core.SPS"))
  expect_identical(nrd_core$Style, "csv")
  expect_identical(nrd_core$Delimiter, ",")
  expect_identical(nrd_core$Fields$Type[nrd_core$Fields$Name == "I10_DX1"], "character")
  expect_identical(nrow(.parse_spss_layout(file.path(nrd, "SPSSLoad_NRD_2022_DX_PR_GRPS.SPS"))$Fields), 922L)

  #### Every 2005+ program (the style HCUP still publishes) must parse.
  programs <- list.files(base, pattern = "^SPSSLoad_.*\\.SPS$", recursive = TRUE, full.names = TRUE,
                         ignore.case = FALSE)
  expect_gt(length(programs), 200L)
  failures <- Filter(Negate(is.null), lapply(programs, function(p) {
    tryCatch({ .parse_spss_layout(p); NULL }, error = function(e) paste(basename(p), conditionMessage(e)))
  }))
  expect_length(failures, 0L)
})

test_that("the asc reader reads records, special codes, and text like a SAV file would", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  sps <- write_fixed_sps(file.path(root, "demo.SPS"))
  asc <- write_text_file(file.path(root, "demo.ASC"), asc_demo_lines())
  options <- list(LayoutFile = sps)
  expected <- asc_demo_expected()

  expect_identical(.read_asc_header(asc, options), c("ID", "NAME", "FLAG", "WEIGHT", "CODE"))
  got <- read_asc_full(asc, reader_options = options)
  expect_equal(got, expected, ignore_attr = TRUE)
  expect_true(is.character(got$NAME) && is.numeric(got$ID))
  expect_equal(.count_asc_rows(asc, options), 7)
  expect_equal(nrow(.read_asc_sample(asc, options, n_max = 3L)), 3L)

  #### Text matches haven::read_sav on the same values: trailing spaces trimmed, leading
  #### spaces kept, an all-blank field is "" (not NA).
  padded <- data.frame(NAME = c("AB   ", "  CD ", "     ", "EFGHI", "J    ", "K    ", "L    "),
                       stringsAsFactors = FALSE)
  sav <- tempfile(fileext = ".sav")
  haven::write_sav(padded, sav)
  expect_identical(got$NAME, as.character(haven::read_sav(sav)$NAME))

  #### The reader is registered, chunkable, and reached through the registry.
  rd <- get_file_reader("asc")
  expect_true(rd$chunkable)
  expect_equal(nrow(call_reader(rd, "read_full", asc, reader_options = options)), 7L)
  expect_identical(call_reader(rd, "read_header", asc, reader_options = options), got |> names())
  expect_error(read_asc_full(asc), "needs ReaderOptions\\$LayoutFile")
})

test_that("fixed-width reads agree across chunk boundaries and line endings", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  sps <- write_fixed_sps(file.path(root, "demo.SPS"))
  options <- list(LayoutFile = sps)
  expected <- asc_demo_expected()
  for (eol in c("\n", "\r\n")) {
    asc <- write_text_file(file.path(root, "demo.ASC"), asc_demo_lines(), eol = eol)
    expect_equal(.count_asc_rows(asc, options), 7)
    for (size in c(1L, 2L, 3L, 7L, 100L)) {
      streamed <- collect_chunks(asc, options, chunk_size = size)
      expect_length(streamed$chunks, ceiling(7 / size))
      expect_true(all(vapply(streamed$chunks, nrow, integer(1)) <= size))
      expect_equal(data.table::rbindlist(streamed$chunks), expected, ignore_attr = TRUE)
      expect_identical(streamed$info$LogicalRows, 7)
    }
    for (size in c(2L, 3L, 5L)) {
      offsets <- seq(0L, 6L, by = size)
      stitched <- data.table::rbindlist(lapply(offsets, function(offset) {
        .read_asc_chunk(asc, offset, size, reader_options = options)
      }))
      expect_equal(stitched, expected, ignore_attr = TRUE)
    }
    #### Out-of-order requests reopen the file rather than returning the wrong records.
    expect_equal(.read_asc_chunk(asc, 4L, 2L, reader_options = options), expected[5:6], ignore_attr = TRUE)
    expect_equal(.read_asc_chunk(asc, 0L, 2L, reader_options = options), expected[1:2], ignore_attr = TRUE)
    expect_equal(.read_asc_chunk(asc, 6L, 10L, reader_options = options), expected[7], ignore_attr = TRUE)
    expect_equal(nrow(.read_asc_chunk(asc, 7L, 10L, reader_options = options)), 0L)
    expect_equal(nrow(.read_asc_sample(asc, options, n_max = 100000L)), 7L)
  }
  expect_length(ls(.asc_cursors), 0L)
})

test_that("trailing blank lines and a missing final newline do not change the records", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  options <- list(LayoutFile = write_fixed_sps(file.path(root, "demo.SPS")))
  expected <- asc_demo_expected()
  variants <- list(
    no_final_newline = list(lines = asc_demo_lines(), final_eol = FALSE, eol = "\n"),
    blank_tail = list(lines = c(asc_demo_lines(), "", ""), final_eol = TRUE, eol = "\n"),
    blank_tail_crlf = list(lines = c(asc_demo_lines(), ""), final_eol = TRUE, eol = "\r\n"),
    crlf_no_final = list(lines = asc_demo_lines(), final_eol = FALSE, eol = "\r\n"))
  for (name in names(variants)) {
    v <- variants[[name]]
    asc <- write_text_file(file.path(root, "demo.ASC"), v$lines, eol = v$eol, final_eol = v$final_eol)
    expect_equal(.count_asc_rows(asc, options), 7, info = name)
    expect_equal(read_asc_full(asc, reader_options = options), expected, ignore_attr = TRUE, info = name)
    expect_equal(data.table::rbindlist(collect_chunks(asc, options, chunk_size = 3L)$chunks), expected,
                 ignore_attr = TRUE, info = name)
    expect_equal(nrow(.read_asc_chunk(asc, 6L, 5L, reader_options = options)), 1L, info = name)
  }
  empty <- write_text_file(file.path(root, "empty.ASC"), character(0), final_eol = FALSE)
  expect_equal(.count_asc_rows(empty, options), 0)
  expect_equal(nrow(read_asc_full(empty, reader_options = options)), 0L)
})

test_that("a record of the wrong length stops the read with its line number and both lengths", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  options <- list(LayoutFile = write_fixed_sps(file.path(root, "demo.SPS")))
  lines <- asc_demo_lines()
  lines[5] <- substr(lines[5], 1L, 20L)
  asc <- write_text_file(file.path(root, "short.ASC"), lines)
  pattern <- "short\\.ASC line 5 has 20 character\\(s\\), but layout demo\\.SPS defines 23-character records"
  expect_error(read_asc_full(asc, reader_options = options), pattern)
  expect_error(collect_chunks(asc, options, chunk_size = 2L), pattern)
  expect_error(.read_asc_chunk(asc, 3L, 3L, reader_options = options), pattern)
  expect_length(ls(.asc_cursors), 0L)

  lines <- asc_demo_lines()
  lines[3] <- paste0(lines[3], "X")
  long <- write_text_file(file.path(root, "long.ASC"), lines, eol = "\r\n")
  expect_error(read_asc_full(long, reader_options = options), "long\\.ASC line 3 has 24 character\\(s\\)")

  lines <- asc_demo_lines()
  lines[4] <- ""
  blank <- write_text_file(file.path(root, "blank.ASC"), lines)
  expect_error(read_asc_full(blank, reader_options = options), "blank\\.ASC line 4 is blank but is followed")
  expect_error(collect_chunks(blank, options, chunk_size = 2L), "line 4 is blank")

  lines <- asc_demo_lines()
  lines[3] <- paste0(substr(lines[3], 1L, 11L), "  abc    ", substr(lines[3], 21L, 23L))
  garbled <- write_text_file(file.path(root, "garbled.ASC"), lines)
  expect_error(read_asc_full(garbled, reader_options = options),
               "garbled\\.ASC line 3: column WEIGHT \\(positions 12-20\\) holds 'abc'")
})

test_that("implied decimals, text fields, and every special-code form work in fixed-width reads", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  sps <- file.path(root, "dec.SPS")
  writeLines(c("DATA LIST FILE = 'D.ASC' FIXED /", "    AMT  1-  6 (2)", "    ROW  7-  9",
               "    TXT 10- 12 (A).",
               "RECODE AMT (-999.99 999.99=SYSMIS).",
               "RECODE ROW (-9 THRU -5 900 THRU HI=SYSMIS)."), sps)
  asc <- write_text_file(file.path(root, "D.ASC"), c(
    "  1234  1abc", "  12.5  2de ", "-99999 -7   ", " 99999950f g", "   -50 -4x  "))
  got <- read_asc_full(asc, reader_options = list(LayoutFile = sps))
  expect_equal(got$AMT, c(12.34, 12.5, NA, NA, -0.5))
  expect_equal(got$ROW, c(1, 2, NA, NA, -4))
  expect_identical(got$TXT, c("abc", "de", "", "f g", "x"))
})

test_that("a headerless CSV is read through LayoutFile with names, types, and special codes", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  sps <- write_csv_sps(file.path(root, "demo.SPS"))
  options <- list(LayoutFile = sps)
  header <- c("ID", "CODE", "AMT", "LABEL")
  for (eol in c("\n", "\r\n")) {
    csv <- write_text_file(file.path(root, "demo.csv"), csv_demo_lines(), eol = eol)
    info <- .delimited_header_info(csv, options)
    expect_identical(info$Header, header)
    expect_false(info$HasHeader)
    expect_identical(info$Delimiter, ",")
    expect_identical(.read_delimited_header(csv, options), header)
    expect_equal(.count_delimited_rows(csv, options), 5)

    got <- read_delimited_full(csv, reader_options = options)
    expect_identical(names(got), header)
    expect_identical(got$ID, c(1L, NA, 3L, 4L, NA))
    expect_identical(got$CODE, c("0012", "0099", "", "0100", "0007"))
    expect_equal(got$AMT, c(10.5, NA, NA, 7, 8.25))
    expect_identical(got$LABEL, c("abc", "def", "", "ghi", "jkl"))

    for (offset in c(0L, 2L, 4L)) {
      chunk <- .read_delimited_chunk(csv, offset, 2L, header, reader_options = options)
      rows <- seq.int(offset + 1L, min(5L, offset + 2L))
      expect_identical(chunk$CODE, got$CODE[rows], info = sprintf("offset %d", offset))
      expect_equal(chunk$AMT, got$AMT[rows], info = sprintf("offset %d", offset))
    }
    expect_equal(nrow(.read_delimited_sample(csv, options)), 5L)

    #### Catalog classes that read a numeric column as text still honor the special codes.
    as_text <- read_delimited_full(csv, col_classes = list(ID = "character", CODE = "character",
                                                         AMT = "character", LABEL = "character"),
                                   reader_options = options)
    expect_identical(as_text$ID, c("1", NA, "3", "4", NA))
    expect_identical(as_text$AMT, c("10.5", NA, NA, "7", "8.25"))
    expect_identical(as_text$CODE, got$CODE)
  }

  #### Header = TRUE reads a headed file with the layout's names.
  headed <- write_text_file(file.path(root, "headed.csv"), c("x,y,z,w", csv_demo_lines()))
  headed_options <- list(LayoutFile = sps, Header = TRUE)
  expect_identical(.read_delimited_header(headed, headed_options), header)
  expect_identical(read_delimited_full(headed, reader_options = headed_options)$CODE,
                   c("0012", "0099", "", "0100", "0007"))
  expect_equal(.count_delimited_rows(headed, headed_options), 5)
})

test_that("ColumnNames and LayoutFile must agree, and the layout must match the file", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  sps <- write_csv_sps(file.path(root, "demo.SPS"))
  csv <- write_text_file(file.path(root, "demo.csv"), csv_demo_lines())
  header <- c("ID", "CODE", "AMT", "LABEL")
  same <- list(LayoutFile = sps, ColumnNames = header)
  expect_identical(.read_delimited_header(csv, same), header)
  expect_identical(names(read_delimited_full(csv, reader_options = same)), header)
  expect_error(.read_delimited_header(csv, list(LayoutFile = sps, ColumnNames = c("ID", "CODE", "VALUE", "LABEL"))),
               "ColumnNames .* and LayoutFile demo\\.SPS .* disagree: column 3 is 'VALUE' in ColumnNames and 'AMT'")
  expect_error(.read_delimited_header(csv, list(LayoutFile = sps, ColumnNames = c("ID", "CODE"))),
               "disagree")
  three_fields <- write_text_file(file.path(root, "three.csv"), c("1,0012,10.5", "2,0099,3"))
  expect_error(.read_delimited_header(three_fields, list(LayoutFile = sps)),
               "ColumnNames has 4 name\\(s\\) but the first record .* has 3 field\\(s\\)")

  fixed <- write_fixed_sps(file.path(root, "fixed.SPS"))
  expect_error(.read_delimited_header(csv, list(LayoutFile = fixed)), "fixed-width layout")
  asc <- write_text_file(file.path(root, "demo.ASC"), asc_demo_lines())
  expect_error(read_asc_full(asc, reader_options = list(LayoutFile = sps)), "describes a delimited file")
})

test_that("LayoutFile resolves against the layout root, absolute paths, and the MDT ReaderOptions", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  dir.create(file.path(root, "nis"))
  sps <- write_fixed_sps(file.path(root, "nis", "demo.SPS"))
  old_option <- getOption("repoquet.layout_root")
  old_env <- Sys.getenv("REPOQUET_LAYOUT_ROOT", unset = NA)
  on.exit({
    options(repoquet.layout_root = old_option)
    if (is.na(old_env)) Sys.unsetenv("REPOQUET_LAYOUT_ROOT") else Sys.setenv(REPOQUET_LAYOUT_ROOT = old_env)
  }, add = TRUE)

  set_layout_root(NULL)
  expect_error(.resolve_layout_path("nis/demo.SPS"), "no layout root is set")
  expect_identical(.resolve_layout_path(sps), normalizePath(sps, winslash = "/"))

  expect_identical(set_layout_root(root), normalizePath(root, winslash = "/"))
  expect_identical(Sys.getenv("REPOQUET_LAYOUT_ROOT"), normalizePath(root, winslash = "/"))
  expect_identical(.resolve_layout_path("nis/demo.SPS"), normalizePath(sps, winslash = "/"))
  expect_error(.resolve_layout_path("nis/other.SPS"), "Layout file not found")
  expect_error(set_layout_root(file.path(root, "absent")), "does not exist")

  #### The environment variable alone (what a worker process inherits) is enough.
  options(repoquet.layout_root = NULL)
  expect_identical(.resolve_layout_path("nis/demo.SPS"), normalizePath(sps, winslash = "/"))

  row <- data.frame(ReaderOptions = '{"LayoutFile": "nis/demo.SPS"}')
  expect_identical(reader_options_for_row(row)$LayoutFile, normalizePath(sps, winslash = "/"))
  expect_identical(reader_options_for_row(data.frame(ReaderOptions = '{"Header": false}'))$LayoutFile, NULL)

  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "T", Path = c("a.ASC", "b.ASC", "c.csv"),
                  FileType = c("asc", "asc", "csv"), PartitionKey = "year", PartitionValue = "2019",
                  ReaderOptions = c('{"LayoutFile": "nis/demo.SPS"}', "", '{"LayoutFile": "nis/missing.SPS"}'))
  out <- utils::capture.output(issues <- ValidateMDTPreflight(M, strict = FALSE))
  expect_true("bad_layout_file" %in% issues$Check)
  expect_identical(issues[issues$Check == "bad_layout_file", ]$N, 2L)
  M$ReaderOptions <- c('{"LayoutFile": "nis/demo.SPS"}', '{"LayoutFile": "nis/demo.SPS"}', "")
  M$FileType[3] <- "csv"
  out <- utils::capture.output(issues <- ValidateMDTPreflight(M, strict = FALSE))
  expect_false("bad_layout_file" %in% issues$Check)
})

test_that("asc sources survey, load to Parquet, and record exact row counts end to end", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  layouts <- new_layout_dir(); on.exit(unlink(layouts, recursive = TRUE), add = TRUE)
  sps <- write_fixed_sps(file.path(layouts, "demo.SPS"))
  n <- 25L
  ids <- seq_len(n)
  lines <- vapply(ids, function(i) asc_demo_line(if (i %% 5L == 0L) -999 else i, sprintf("N%d", i), i %% 3L,
                                                 i + 0.5, sprintf("%03d", i)), character(1))
  write_text_file(file.path(fx$src, "DEMO_2019.ASC"), lines, eol = "\r\n")
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Fixed", Path = "DEMO_2019.ASC",
                  FileType = "asc", PartitionKey = "year", PartitionValue = "2019",
                  ReaderOptions = jsonlite::toJSON(list(LayoutFile = sps), auto_unbox = TRUE))

  #### Schema survey: types come from the layout; special codes are not data.
  obs <- file.path(fx$root, "obs.parquet")
  out <- utils::capture.output(SurveyRepositorySchema(
    MDT = M, MasterDBPath = fx$root, ObservationPath = obs, n_workers = 1,
    SchemaSurveyMode = "full", SourceFingerprintMode = "none"))
  observed <- data.table::as.data.table(arrow::read_parquet(obs))
  expect_true(all(observed$SurveyStatus == "ok"))
  expect_setequal(toupper(observed$Column[!observed$IsPartitionColumn]), c("ID", "NAME", "FLAG", "WEIGHT", "CODE"))
  id_row <- observed[toupper(observed$Column) == "ID", ]
  expect_equal(as.numeric(id_row$NonMissingCount), 20)
  expect_equal(as.numeric(id_row$RowsSampled), n)
  expect_identical(observed[toupper(observed$Column) == "NAME", ]$ObservedType, "character")

  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  pieces <- list.files(file.path(fx$pq, "REG_Fixed", "year=2019"), pattern = "parquet$", full.names = TRUE)
  expect_gte(length(pieces), 1L)
  stored <- data.table::rbindlist(lapply(pieces, function(p) data.table::as.data.table(arrow::read_parquet(p))))
  data.table::setorder(stored, CODE)
  expect_identical(nrow(stored), n)
  expect_identical(stored$CODE, sprintf("%03d", ids))
  expect_identical(stored$NAME, sprintf("N%d", ids))
  expect_equal(sum(is.na(stored$ID)), 5L)
  expect_equal(stored$WEIGHT, ids + 0.5)
  manifest <- data.table::fread(fx$mf)
  expect_equal(sum(manifest[manifest$Status == "written", ]$NRows), n)
})

test_that("an asc source whose layout holds the partition column routes rows to Hive partitions", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  layouts <- new_layout_dir(); on.exit(unlink(layouts, recursive = TRUE), add = TRUE)
  sps <- file.path(layouts, "years.SPS")
  writeLines(c("DATA LIST FILE = 'Y.ASC' FIXED /", "    YEAR  1-  4", "    ID    5-  8", "    TAG   9- 10 (A).",
               "RECODE ID (-999=SYSMIS)."), sps)
  years <- rep(c(2019L, 2020L, 2020L), c(8L, 9L, 8L))
  lines <- sprintf("%4d%4d%-2s", years, seq_along(years), "ab")
  write_text_file(file.path(fx$src, "Y.ASC"), lines)
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Years", Path = "Y.ASC", FileType = "asc",
                  PartitionKey = "YEAR", PartitionValue = "2019",
                  ReaderOptions = jsonlite::toJSON(list(LayoutFile = sps), auto_unbox = TRUE))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  expect_true(any(grepl("streamed directly to 2 Hive partitions", r$output)))
  expect_true(all(dir.exists(file.path(fx$pq, "REG_Years", c("year=2019", "year=2020")))))
  parquet <- list.files(file.path(fx$pq, "REG_Years"), pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
  got <- data.table::rbindlist(lapply(parquet, arrow::read_parquet))
  expect_identical(nrow(got), 25L)
  expect_false("YEAR" %in% names(got))
  expect_identical(sort(got$TAG), rep("ab", 25L))
})

test_that("a headerless CSV loads end to end through LayoutFile and keeps text codes intact", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  layouts <- new_layout_dir(); on.exit(unlink(layouts, recursive = TRUE), add = TRUE)
  sps <- write_csv_sps(file.path(layouts, "demo.SPS"))
  n <- 25L
  write_text_file(file.path(fx$src, "demo_2019.csv"),
                  sprintf("%d,%04d,%.2f,lab%d", ifelse(seq_len(n) %% 5L == 0L, -99L, seq_len(n)),
                          seq_len(n), seq_len(n) / 4, seq_len(n)), eol = "\r\n")
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Headerless", Path = "demo_2019.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019",
                  ReaderOptions = jsonlite::toJSON(list(LayoutFile = sps), auto_unbox = TRUE))
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  pieces <- list.files(file.path(fx$pq, "REG_Headerless", "year=2019"), pattern = "parquet$", full.names = TRUE)
  expect_length(pieces, 3L)
  stored <- data.table::rbindlist(lapply(pieces, function(p) data.table::as.data.table(arrow::read_parquet(p))))
  data.table::setorder(stored, CODE)
  expect_identical(stored$CODE, sprintf("%04d", seq_len(n)))
  expect_equal(sum(is.na(stored$ID)), 5L)
  expect_equal(stored$AMT, seq_len(n) / 4)
  expect_identical(stored$LABEL, sprintf("lab%d", seq_len(n)))
})

test_that("parallel survey workers resolve a relative LayoutFile from the layout root", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  layouts <- new_layout_dir(); on.exit(unlink(layouts, recursive = TRUE), add = TRUE)
  dir.create(file.path(layouts, "nis"))
  write_fixed_sps(file.path(layouts, "nis", "demo.SPS"))
  old_option <- getOption("repoquet.layout_root")
  old_env <- Sys.getenv("REPOQUET_LAYOUT_ROOT", unset = NA)
  on.exit({
    options(repoquet.layout_root = old_option)
    if (is.na(old_env)) Sys.unsetenv("REPOQUET_LAYOUT_ROOT") else Sys.setenv(REPOQUET_LAYOUT_ROOT = old_env)
  }, add = TRUE)
  for (year in c(2019, 2020)) {
    write_text_file(file.path(fx$src, sprintf("DEMO_%d.ASC", year)), asc_demo_lines())
  }
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Fixed",
                  Path = c("DEMO_2019.ASC", "DEMO_2020.ASC"), FileType = "asc", PartitionKey = "year",
                  PartitionValue = c("2019", "2020"),
                  ReaderOptions = '{"LayoutFile": "nis/demo.SPS"}')
  set_layout_root(layouts)
  options(repoquet.layout_root = NULL)   # a worker process sees only the environment variable
  obs <- file.path(fx$root, "obs.parquet")
  out <- utils::capture.output(SurveyRepositorySchema(
    MDT = M, MasterDBPath = fx$root, ObservationPath = obs, n_workers = 2,
    SchemaSurveyMode = "full", SourceFingerprintMode = "none"))
  observed <- data.table::as.data.table(arrow::read_parquet(obs))
  expect_true(all(observed$SurveyStatus == "ok"))
  expect_identical(sum(observed$Column == "WEIGHT"), 2L)
})

test_that("delimited chunks start at the right record for LF, CRLF, and CR CR LF line endings", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  n <- 23L
  records <- sprintf("%d,%s,%.2f", seq_len(n), LETTERS[(seq_len(n) - 1L) %% 26L + 1L], seq_len(n) / 4)
  col_names <- c("ID", "TAG", "VALUE")
  endings <- c(LF = "\n", CRLF = "\r\n", CRCRLF = "\r\r\n")
  for (ending in names(endings)) {
    for (headed in c(FALSE, TRUE)) {
      label <- sprintf("%s, %s", ending, if (headed) "headed" else "headerless")
      lines <- if (headed) c(paste(col_names, collapse = ","), records) else records
      csv <- write_text_file(file.path(root, "t.csv"), lines, eol = endings[[ending]])
      options <- if (headed) list(Delimiter = ",") else
        list(Delimiter = ",", Header = FALSE, ColumnNames = col_names)
      whole <- read_delimited_full(csv, reader_options = options)
      expect_identical(nrow(whole), n, info = label)
      expect_identical(whole$ID, seq_len(n), info = label)
      for (size in c(1L, 4L, 7L, 23L, 50L)) {
        offsets <- seq(0L, n - 1L, by = size)
        stitched <- data.table::rbindlist(lapply(offsets, function(offset) {
          .read_delimited_chunk(csv, offset, size, col_names, reader_options = options)
        }))
        expect_equal(stitched, whole, ignore_attr = TRUE, info = sprintf("%s, chunk size %d", label, size))
      }
      #### Non-sequential requests reopen the file and still land on the requested record.
      expect_identical(.read_delimited_chunk(csv, 9L, 3L, col_names, reader_options = options)$ID, 10:12, info = label)
      expect_identical(.read_delimited_chunk(csv, 2L, 2L, col_names, reader_options = options)$ID, 3:4, info = label)
      expect_identical(.read_delimited_chunk(csv, 20L, 9L, col_names, reader_options = options)$ID, 21:23, info = label)
      expect_identical(nrow(.read_delimited_chunk(csv, 23L, 5L, col_names, reader_options = options)), 0L, info = label)
      expect_length(ls(.delimited_cursors), 0L)
    }
  }
})

test_that("a CR CR LF headed CSV loads end to end without duplicated or dropped rows", {
  fx <- new_repo_fixture(); on.exit(unlink(fx$root, recursive = TRUE))
  n <- 37L
  write_text_file(file.path(fx$src, "crcrlf_2019.csv"),
                  c("ID,TAG,VALUE", sprintf("%d,t%d,%.1f", seq_len(n), seq_len(n), seq_len(n) / 2)),
                  eol = "\r\r\n")
  M <- data.frame(Database = "REG", MDBDir = "REG", TableName = "Crcrlf", Path = "crcrlf_2019.csv",
                  FileType = "csv", PartitionKey = "year", PartitionValue = "2019")
  r <- run_loader(fx, M, "REG")
  expect_length(r$checkpoint, 1L)
  pieces <- list.files(file.path(fx$pq, "REG_Crcrlf", "year=2019"), pattern = "parquet$", full.names = TRUE)
  expect_gte(length(pieces), 2L)
  stored <- data.table::rbindlist(lapply(pieces, function(p) data.table::as.data.table(arrow::read_parquet(p))))
  data.table::setorder(stored, ID)
  expect_equal(stored$ID, seq_len(n))
  expect_identical(stored$TAG, sprintf("t%d", seq_len(n)))
  expect_length(ls(.delimited_cursors), 0L)
})

test_that("the chunk-positioning audit flags duplicated-rows-plus-missing-tail loads and clears clean ones", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  dir.create(file.path(root, "D"))
  n <- 20L
  records <- sprintf("%d,%d", seq_len(n), seq_len(n))
  write_text_file(file.path(root, "D", "crcrlf.csv"), c("id,v", records), eol = "\r\r\n")
  write_text_file(file.path(root, "D", "lf.csv"), c("id,v", records), eol = "\n")
  write_text_file(file.path(root, "D", "single.csv"), c("id,v", records[1:5]), eol = "\r\r\n")
  M <- data.frame(Database = "D", MDBDir = "D", Path = c("crcrlf.csv", "lf.csv", "single.csv"),
                  TableName = c("Bad", "Good", "One"), FileType = "csv", PartitionKey = "year",
                  PartitionValue = "2024")
  mf <- file.path(root, "manifest.csv")
  add_pieces <- function(table, path, ranges) {
    for (k in seq_along(ranges)) {
      piece <- file.path(root, sprintf("%s_%d.parquet", table, k))
      arrow::write_parquet(data.frame(id = ranges[[k]], v = ranges[[k]]), piece)
      update_parquet_manifest(mf, "D", table, paste0("D_", table), 2024, path, piece, length(ranges[[k]]),
                              Notes = sprintf("chunk_%05d", k), PartitionKey = "year", PartitionValue = "2024")
    }
  }
  add_pieces("Bad", "crcrlf.csv", list(1:10, 6:15))         # chunk 2 restarted early: dup 6:10, tail lost
  add_pieces("Good", "lf.csv", list(1:10, 11:20))
  add_pieces("One", "single.csv", list(1:5))

  audit <- audit_chunk_positioning(M, root, mf)
  expect_setequal(audit$TableName, c("Bad", "Good"))              # single-chunk sources are not audited
  bad <- audit[audit$TableName == "Bad", ]
  expect_true(bad$OldMethodMisplaced)
  expect_identical(bad$FirstMisplacedOffset, 10)
  expect_identical(bad$ParquetRows, 20)
  expect_identical(bad$DuplicateRows, 5)
  expect_identical(bad$Verdict, "AFFECTED: reload")
  good <- audit[audit$TableName == "Good", ]
  expect_false(good$OldMethodMisplaced)
  expect_identical(good$Verdict, "ok")

  #### A CR CR LF source loaded correctly (after the fix) has no repeated rows.
  mf2 <- file.path(root, "manifest2.csv")
  mf <- mf2
  add_pieces("Bad", "crcrlf.csv", list(1:10, 11:20))
  fixed <- audit_chunk_positioning(M, root, mf2)
  expect_identical(fixed$Verdict, "positioning differs, no duplicate rows")
  expect_identical(fixed$DuplicateRows, 0)
  expect_identical(audit_chunk_positioning(M, root, mf2, CheckDuplicates = FALSE)$Verdict,
                   "positioning differs; duplicates not checked")
  expect_identical(nrow(audit_chunk_positioning(M, root, mf2, Databases = "Z")), 0L)
})

test_that("the audit treats one piece per partition as unaffected and explains a failed replay", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  dir.create(file.path(root, "D"))
  M <- data.frame(Database = "D", MDBDir = "D", Path = c("routed.csv", "short.csv"),
                  TableName = c("Routed", "Short"), FileType = "csv", PartitionKey = "year",
                  PartitionValue = "2021")
  mf <- file.path(root, "manifest.csv")
  add_piece <- function(table, path, k, partition, ids) {
    piece <- file.path(root, sprintf("%s_%s_%d.parquet", table, partition, k))
    arrow::write_parquet(data.frame(id = ids), piece)
    update_parquet_manifest(mf, "D", table, paste0("D_", table), partition, path, piece, length(ids),
                            Notes = sprintf("chunk_%05d", k), PartitionKey = "year", PartitionValue = partition)
  }
  #### Routed by its own year column: each partition holds chunk 1 only, and the source file is absent.
  add_piece("Routed", "routed.csv", 1, "2021", 1:5)
  add_piece("Routed", "routed.csv", 1, "2022", 6:10)
  #### Two chunks in one partition, but the file is shorter than the manifest claims.
  write_text_file(file.path(root, "D", "short.csv"), c("id", 1:4), eol = "\n")
  add_piece("Short", "short.csv", 1, "2021", 1:50)
  add_piece("Short", "short.csv", 2, "2021", 51:100)
  audit <- audit_chunk_positioning(M, root, mf)
  routed <- audit[audit$TableName == "Routed", ]
  expect_identical(routed$Verdict, "ok")
  expect_identical(routed$MaxChunksPerPartition, 1L)
  short <- audit[audit$TableName == "Short", ]
  expect_identical(short$MaxChunksPerPartition, 2L)
  expect_match(short$Verdict, "could not replay any chunk start: no record found at offset 50")
})

test_that("a large chunk is parsed in bounded batches that match a single read", {
  root <- new_layout_dir(); on.exit(unlink(root, recursive = TRUE))
  n <- 40L
  records <- sprintf("%d,%s,%.2f", seq_len(n), strrep("x", seq_len(n) %% 7L + 1L), seq_len(n) / 8)
  col_names <- c("ID", "TAG", "VALUE")
  for (ending in c("\n", "\r\r\n")) {
    csv <- write_text_file(file.path(root, "t.csv"), c(paste(col_names, collapse = ","), records), eol = ending)
    options <- list(Delimiter = ",")
    whole <- read_delimited_full(csv, reader_options = options)
    for (settings in list(c(4L, 1e9), c(1000L, 60), c(7L, 1))) {   # row-limited, byte-limited, below one record
      got <- .read_delimited_chunk(csv, 3L, 30L, col_names, reader_options = options,
                                   batch_rows = settings[1], batch_bytes = settings[2])
      expect_equal(got, whole[4:33], ignore_attr = TRUE, info = paste(settings, collapse = "/"))
      .close_chunk_cursors(csv)   # the loader does this on exit; a read that stops short leaves the cursor open
      expect_length(ls(.delimited_cursors), 0L)
    }
    #### A request larger than the file returns what remains.
    tail_rows <- .read_delimited_chunk(csv, 35L, 100L, col_names, reader_options = options, batch_rows = 2L)
    expect_equal(tail_rows, whole[36:40], ignore_attr = TRUE)
  }
})
