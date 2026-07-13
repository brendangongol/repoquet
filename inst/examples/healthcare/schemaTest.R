

> DBLoad <- sort(unique(MDT$Database))
> PrepareSchemaRegistry(
  +   MDT = MDT,
  +   DBLoad = DBLoad,
  +   MasterDBPath = MasterDBPath,
  +   ObservationPath = SchemaObservationPath,
  +   SchemaReviewPath = SchemaReviewPath,
  +   n_workers = n_workers,
  +   SourceFingerprintMode = "metadata",
  +   StrictReaders = FALSE,
  +   LogPath = LogPath,
  +   RunId = RunId
  + )
[2026-07-13 11:22:33] [run_id=20260713T112135_11192_239822] [PARALLEL FALLBACK] repository schema survey failed for 337 item(s); retrying those item(s) serially in the main R process.
[2026-07-13 11:33:51] [run_id=20260713T112135_11192_239822] [PARALLEL FALLBACK] repository schema survey recovered 337 of 337 failed item(s) serially.
[2026-07-13 11:33:51] [run_id=20260713T112135_11192_239822] [SCHEMA SURVEY] 485 source(s), 22519 observation row(s), 0 failed source(s); wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaObservations.parquet
[2026-07-13 11:34:06] [SCHEMA PROPOSAL] Wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx (5338 columns; 369 require review).
[2026-07-13 11:34:06] [SCHEMA READY] 4969 column(s) resolved automatically; 369 require review in X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx.
> #### Optional console preview. The helper queries Parquet with DuckDB, so    ####
> #### it does not pull the full observation store into R.                     ####
> schema_issues <- GetSchemaObservations(
  +   ObservationPath = SchemaObservationPath,
  +   IssuesOnly = TRUE,
  +   Limit = 100L
  + )
> if (nrow(schema_issues) > 0L) print(schema_issues)
Database TableName DuckDBTable
<char>    <char>      <char>
  1:     NTDB    AISDES NTDB_AISDES
2:     NTDB    AISDES NTDB_AISDES
3:     NTDB    AISDES NTDB_AISDES
4:     NTDB    AISDES NTDB_AISDES
5:     NTDB    AISDES NTDB_AISDES
6:      TQP    AISDES  TQP_AISDES
7:      TQP    AISDES  TQP_AISDES
8:      TQP    AISDES  TQP_AISDES
9:      TQP    AISDES  TQP_AISDES
10:      TQP    AISDES  TQP_AISDES
SourcePath
<char>
  1: X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
2: X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
3: X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
4: X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
5: X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
6:                        X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
7:                        X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
8:                        X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
9:                        X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
10:                        X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
FileType PartitionKey PartitionValue SourceSize           SourceModifiedUTC
<char>       <char>         <char>      <num>                      <char>
  1:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
2:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
3:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
4:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
5:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
6:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
7:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
8:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
9:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
10:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
SourceFingerprint DeclaredEncoding DetectedEncoding
<char>           <char>           <char>
  1: meta:169791:2018-11-30T18:31:05.000000Z             auto            UTF-8
2: meta:169791:2018-11-30T18:31:05.000000Z             auto            UTF-8
3: meta:169791:2018-11-30T18:31:05.000000Z             auto            UTF-8
4: meta:169791:2018-11-30T18:31:05.000000Z             auto            UTF-8
5: meta:169791:2018-11-30T18:31:05.000000Z             auto            UTF-8
6: meta:170973:2019-05-17T22:52:40.000000Z             auto            UTF-8
7: meta:170973:2019-05-17T22:52:40.000000Z             auto            UTF-8
8: meta:170973:2019-05-17T22:52:40.000000Z             auto            UTF-8
9: meta:170973:2019-05-17T22:52:40.000000Z             auto            UTF-8
10: meta:170973:2019-05-17T22:52:40.000000Z             auto            UTF-8
EncodingConfidence EncodingUsed EncodingDetectionMethod EncodingValidationStatus
<num>       <char>                  <char>                   <char>
  1:                  1        UTF-8             strict_utf8        sample_valid_utf8
2:                  1        UTF-8             strict_utf8        sample_valid_utf8
3:                  1        UTF-8             strict_utf8        sample_valid_utf8
4:                  1        UTF-8             strict_utf8        sample_valid_utf8
5:                  1        UTF-8             strict_utf8        sample_valid_utf8
6:                  1        UTF-8             strict_utf8        sample_valid_utf8
7:                  1        UTF-8             strict_utf8        sample_valid_utf8
8:                  1        UTF-8             strict_utf8        sample_valid_utf8
9:                  1        UTF-8             strict_utf8        sample_valid_utf8
10:                  1        UTF-8             strict_utf8        sample_valid_utf8
ObservationKind          Column  OriginalColumn IsPartitionColumn
<char>          <char>          <char>            <lgcl>
  1:   source_column AIS_DESCRIPTION AIS_DESCRIPTION             FALSE
2:   source_column      AIS_PREDOT      AIS_PREDOT             FALSE
3:   source_column    AIS_SEVERITY    AIS_SEVERITY             FALSE
4:   source_column     AIS_VERSION     AIS_VERSION             FALSE
5:  hive_partition            YEAR            <NA>              TRUE
6:   source_column         AISDESC         AISDESC             FALSE
7:   source_column          AISVER          AISVER             FALSE
8:   source_column          PREDOT          PREDOT             FALSE
9:   source_column        SEVERITY        SEVERITY             FALSE
10:  hive_partition            YEAR            <NA>              TRUE
InferenceConfidence
<char>
  1:              sampled
2:              sampled
3:              sampled
4:              sampled
5: configured_partition
6:              sampled
7:              sampled
8:              sampled
9:              sampled
10: configured_partition
ReaderWarning
<char>
  1: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  2: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  3: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  4: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  5: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  6: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  7: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  8: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  9: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  10: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  SurveyStatus SurveyMessage ObservedType RowsSampled NonMissingCount MissingPercent
<char>        <char>       <char>       <num>           <num>          <num>
  1:           ok          <NA>    character         175             175              0
2:           ok          <NA>    character         175             175              0
3:           ok          <NA>      integer         175             175              0
4:           ok          <NA>      integer         175             175              0
5:           ok          <NA>      integer         175             175              0
6:           ok          <NA>    character         182             182              0
7:           ok          <NA>      integer         182             182              0
8:           ok          <NA>    character         182             182              0
9:           ok          <NA>      integer         182             182              0
10:           ok          <NA>      integer         182             182              0
IntegerLike FractionalCount LeadingZeroCount NumericParseFailureCount Minimum
<lgcl>           <num>            <num>                    <num>  <char>
  1:          NA              NA                0                      175    <NA>
  2:          NA              NA               22                        0    <NA>
  3:        TRUE               0                0                        0       1
4:        TRUE               0                0                        0    2005
5:        TRUE               0                0                        0    <NA>
  6:          NA              NA                0                      182    <NA>
  7:        TRUE               0                0                        0    2005
8:          NA              NA               22                        0    <NA>
  9:        TRUE               0                0                        0       1
10:        TRUE               0                0                        0    <NA>
  Maximum MaximumTextLength PrecisionRisk
<char>             <num>        <lgcl>
  1:    <NA>               153         FALSE
2:    <NA>                 6         FALSE
3:       9                NA         FALSE
4:    2005                NA         FALSE
5:    <NA>                NA         FALSE
6:    <NA>               153         FALSE
7:    2005                NA         FALSE
8:    <NA>                 6         FALSE
9:       9                NA         FALSE
10:    <NA>                NA         FALSE
> #### On the first run, open SchemaReviewPath and complete every Review row:  ####
> ####   Decision = "Accept" keeps RecommendedType.                            ####
> ####   Decision = "Override" requires the desired ApprovedType.              ####
> #### MergeGroup may identify columns that must share one type across tables. ####
> #### After review, run FinalizeSchemaRegistry below; a second survey is not  ####
> #### needed. Rerun the survey after fixing SourceIssues or changing sources. ####
> #### Existing decisions survive only while their observation signature is   ####
> #### unchanged, so changed evidence always returns to the Review sheet.      ####
> #### Finalization stops here until all required decisions are complete, then ####
> #### writes TableSchemas.xlsx in the exact format ParquetBackEndCreate uses. ####
> repository_catalog <- FinalizeSchemaRegistry(
  +   SchemaReviewPath = SchemaReviewPath,
  +   TableSchemaPath = TableSchemaPath,
  +   strict = TRUE
  + )
[2026-07-13 11:36:33] [SCHEMA FINALIZED] Wrote 5338 approved column definitions to X:/Brendan/NationalDatabases/formattedDatabases/Schema/TableSchemas.xlsx.