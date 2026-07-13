


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
[2026-07-13 10:11:12] [run_id=20260713T100928_21080_204662] [PARALLEL FALLBACK] repository schema survey failed for 337 item(s); retrying those item(s) serially in the main R process.
[2026-07-13 10:19:23] [run_id=20260713T100928_21080_204662] [PARALLEL FALLBACK] repository schema survey recovered 319 of 337 failed item(s) serially.
[2026-07-13 10:19:23] [run_id=20260713T100928_21080_204662] [SCHEMA SURVEY] 485 source(s), 21145 observation row(s), 18 failed source(s); wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaObservations.parquet
[2026-07-13 10:19:38] [SCHEMA PROPOSAL] Wrote X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx (5297 columns; 368 require review).
[2026-07-13 10:19:38] [SCHEMA READY] 4929 column(s) resolved automatically; 368 require review in X:/Brendan/NationalDatabases/formattedDatabases/Schema/SchemaReview.xlsx.
> #### Optional console preview. The helper queries Parquet with DuckDB, so    ####
> #### it does not pull the full observation store into R.                     ####
> schema_issues <- GetSchemaObservations(
  +   ObservationPath = SchemaObservationPath,
  +   IssuesOnly = TRUE,
  +   Limit = 100L
  + )
> if (nrow(schema_issues) > 0L) print(schema_issues)
Database           TableName             DuckDBTable
<char>              <char>                  <char>
  1:    NSQIP             DBTable           NSQIP_DBTable
2:    NSQIP             DBTable           NSQIP_DBTable
3:    NSQIP             DBTable           NSQIP_DBTable
4:    NSQIP             DBTable           NSQIP_DBTable
5:    NSQIP             DBTable           NSQIP_DBTable
6:     NTDB              AISDES             NTDB_AISDES
7:     NTDB              AISDES             NTDB_AISDES
8:     NTDB              AISDES             NTDB_AISDES
9:     NTDB              AISDES             NTDB_AISDES
10:     NTDB              AISDES             NTDB_AISDES
11:     NTDB            DCODEDES           NTDB_DCODEDES
12:     NTDB      ICD10_DCODEDES     NTDB_ICD10_DCODEDES
13:     NTDB      ICD10_DCODEDES     NTDB_ICD10_DCODEDES
14:     NTDB      ICD10_PCODEDES     NTDB_ICD10_PCODEDES
15:      TQP              AISDES              TQP_AISDES
16:      TQP              AISDES              TQP_AISDES
17:      TQP              AISDES              TQP_AISDES
18:      TQP              AISDES              TQP_AISDES
19:      TQP              AISDES              TQP_AISDES
20:      TQP AISDIAGNOSIS_LOOKUP TQP_AISDIAGNOSIS_LOOKUP
21:      TQP AISDIAGNOSIS_LOOKUP TQP_AISDIAGNOSIS_LOOKUP
22:      TQP      ICD10_PCODEDES      TQP_ICD10_PCODEDES
23:      TQP ICDDIAGNOSIS_LOOKUP TQP_ICDDIAGNOSIS_LOOKUP
24:      TQP ICDDIAGNOSIS_LOOKUP TQP_ICDDIAGNOSIS_LOOKUP
25:      TQP ICDDIAGNOSIS_LOOKUP TQP_ICDDIAGNOSIS_LOOKUP
26:      TQP ICDDIAGNOSIS_LOOKUP TQP_ICDDIAGNOSIS_LOOKUP
27:      TQP ICDDIAGNOSIS_LOOKUP TQP_ICDDIAGNOSIS_LOOKUP
28:      TQP ICDDIAGNOSIS_LOOKUP TQP_ICDDIAGNOSIS_LOOKUP
Database           TableName             DuckDBTable
<char>              <char>                  <char>
  SourcePath
<char>
  1:                                                                  X:/National Databases/NSQIP_data/ACS_NSQIP_PUF10_TXT.txt
2:                                                                      X:/National Databases/NSQIP_data/acs_nsqip_puf20.txt
3:                                                                      X:/National Databases/NSQIP_data/acs_nsqip_puf21.txt
4:                                                                      X:/National Databases/NSQIP_data/acs_nsqip_puf22.txt
5:                                                                      X:/National Databases/NSQIP_data/acs_nsqip_puf23.txt
6:                      X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
7:                      X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
8:                      X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
9:                      X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
10:                      X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_AISDES.csv
11:                    X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_DCODEDES.csv
12:              X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2015/CSV/TQIP_RDS_ICD10_DCODEDES.csv
13:              X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2016/CSV/TQIP_RDS_ICD10_DCODEDES.csv
14:              X:/National Databases/NTDB Data/Adult TQIP PUF AY 2010-2016/TQIP PUF AY 2015/CSV/tqip_rds_icd10_pcodedes.csv
15:                                             X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
16:                                             X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
17:                                             X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
18:                                             X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
19:                                             X:/National Databases/TQP Data/TQP All Years/TQP 2016/PUF_AISDES_TQP_2016.csv
20:                                X:/National Databases/TQP Data/TQP All Years/TQP 2017/PUF_AISDIAGNOSIS_LOOKUP_TQP_2017.csv
21:                                X:/National Databases/TQP Data/TQP All Years/TQP 2018/PUF_AISDIAGNOSIS_LOOKUP_TQP_2018.csv
22:                                     X:/National Databases/TQP Data/TQP All Years/TQP 2015/PUF_ICD10_PCODEDES_TQP_2015.csv
23:                                X:/National Databases/TQP Data/TQP All Years/TQP 2017/PUF_ICDDIAGNOSIS_LOOKUP_TQP_2017.csv
24:                                X:/National Databases/TQP Data/TQP All Years/TQP 2018/PUF_ICDDIAGNOSIS_LOOKUP_TQP_2018.csv
25: X:/National Databases/TQP Data/TQP All Years/TQP 2019 Revised/PUF AY 2019 Revised/CSV/PUF_ICDDIAGNOSIS_LOOKUP_REVISED.csv
26:                                  X:/National Databases/TQP Data/TQP All Years/PUF AY 2020/CSV/PUF_ICDDIAGNOSIS_LOOKUP.csv
27:                      X:/National Databases/TQP Data/TQP All Years/PUF AY 2021/PUF AY 2021/CSV/PUF_ICDDIAGNOSIS_LOOKUP.csv
28:                      X:/National Databases/TQP Data/TQP All Years/PUF AY 2022/PUF AY 2022/CSV/PUF_ICDDIAGNOSIS_LOOKUP.csv
SourcePath
<char>
  FileType PartitionKey PartitionValue SourceSize           SourceModifiedUTC
<char>       <char>         <char>      <num>                      <char>
  1:      csv         YEAR           2010  493150043                        <NA>
  2:      csv         YEAR           2020 1482037445                        <NA>
  3:      csv         YEAR           2021 1499013058                        <NA>
  4:      csv         YEAR           2022 1599114354                        <NA>
  5:      csv         YEAR           2023 1558258614                        <NA>
  6:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
7:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
8:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
9:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
10:      csv         YEAR           2016     169791 2018-11-30T18:31:05.000000Z
11:      csv         YEAR           2016     896203                        <NA>
  12:      csv         YEAR           2015   20113152                        <NA>
  13:      csv         YEAR           2016   20707811                        <NA>
  14:      csv         YEAR           2015    8654944                        <NA>
  15:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
16:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
17:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
18:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
19:      csv         YEAR           2016     170973 2019-05-17T22:52:40.000000Z
20:      csv         YEAR           2017     198486                        <NA>
  21:      csv         YEAR           2018     198486                        <NA>
  22:      csv         YEAR           2015    8654944                        <NA>
  23:      csv         YEAR           2017    8357497                        <NA>
  24:      csv         YEAR           2018    8357497                        <NA>
  25:      csv         YEAR           2019    8662389                        <NA>
  26:      csv         YEAR           2020    8662389                        <NA>
  27:      csv         YEAR           2021    8662389                        <NA>
  28:      csv         YEAR           2022    8388772                        <NA>
  FileType PartitionKey PartitionValue SourceSize           SourceModifiedUTC
<char>       <char>         <char>      <num>                      <char>
  SourceFingerprint ObservationKind          Column
<char>          <char>          <char>
  1:                                    <NA>    source_error            <NA>
  2:                                    <NA>    source_error            <NA>
  3:                                    <NA>    source_error            <NA>
  4:                                    <NA>    source_error            <NA>
  5:                                    <NA>    source_error            <NA>
  6: meta:169791:2018-11-30T18:31:05.000000Z   source_column AIS_DESCRIPTION
7: meta:169791:2018-11-30T18:31:05.000000Z   source_column      AIS_PREDOT
8: meta:169791:2018-11-30T18:31:05.000000Z   source_column    AIS_SEVERITY
9: meta:169791:2018-11-30T18:31:05.000000Z   source_column     AIS_VERSION
10: meta:169791:2018-11-30T18:31:05.000000Z  hive_partition            YEAR
11:                                    <NA>    source_error            <NA>
  12:                                    <NA>    source_error            <NA>
  13:                                    <NA>    source_error            <NA>
  14:                                    <NA>    source_error            <NA>
  15: meta:170973:2019-05-17T22:52:40.000000Z   source_column         AISDESC
16: meta:170973:2019-05-17T22:52:40.000000Z   source_column          AISVER
17: meta:170973:2019-05-17T22:52:40.000000Z   source_column          PREDOT
18: meta:170973:2019-05-17T22:52:40.000000Z   source_column        SEVERITY
19: meta:170973:2019-05-17T22:52:40.000000Z  hive_partition            YEAR
20:                                    <NA>    source_error            <NA>
  21:                                    <NA>    source_error            <NA>
  22:                                    <NA>    source_error            <NA>
  23:                                    <NA>    source_error            <NA>
  24:                                    <NA>    source_error            <NA>
  25:                                    <NA>    source_error            <NA>
  26:                                    <NA>    source_error            <NA>
  27:                                    <NA>    source_error            <NA>
  28:                                    <NA>    source_error            <NA>
  SourceFingerprint ObservationKind          Column
<char>          <char>          <char>
  OriginalColumn IsPartitionColumn  InferenceConfidence
<char>            <lgcl>               <char>
  1:            <NA>             FALSE          unavailable
2:            <NA>             FALSE          unavailable
3:            <NA>             FALSE          unavailable
4:            <NA>             FALSE          unavailable
5:            <NA>             FALSE          unavailable
6: AIS_DESCRIPTION             FALSE              sampled
7:      AIS_PREDOT             FALSE              sampled
8:    AIS_SEVERITY             FALSE              sampled
9:     AIS_VERSION             FALSE              sampled
10:            <NA>              TRUE configured_partition
11:            <NA>             FALSE          unavailable
12:            <NA>             FALSE          unavailable
13:            <NA>             FALSE          unavailable
14:            <NA>             FALSE          unavailable
15:         AISDESC             FALSE              sampled
16:          AISVER             FALSE              sampled
17:          PREDOT             FALSE              sampled
18:        SEVERITY             FALSE              sampled
19:            <NA>              TRUE configured_partition
20:            <NA>             FALSE          unavailable
21:            <NA>             FALSE          unavailable
22:            <NA>             FALSE          unavailable
23:            <NA>             FALSE          unavailable
24:            <NA>             FALSE          unavailable
25:            <NA>             FALSE          unavailable
26:            <NA>             FALSE          unavailable
27:            <NA>             FALSE          unavailable
28:            <NA>             FALSE          unavailable
OriginalColumn IsPartitionColumn  InferenceConfidence
<char>            <lgcl>               <char>
  ReaderWarning
<char>
  1:                                                                                                                                      <NA>
  2:                                                                                                                                      <NA>
  3:                                                                                                                                      <NA>
  4:                                                                                                                                      <NA>
  5:                                                                                                                                      <NA>
  6: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  7: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  8: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  9: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  10: Stopped early on line 177. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  11:                                                                                                                                      <NA>
  12:                                                                                                                                      <NA>
  13:                                                                                                                                      <NA>
  14:                                                                                                                                      <NA>
  15: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  16: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  17: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  18: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  19: Stopped early on line 184. Expected 4 fields but found 1. Consider fill=TRUE. First discarded non-empty line: <<<=age 10; 0.6-1cm thick>>
  20:                                                                                                                                      <NA>
  21:                                                                                                                                      <NA>
  22:                                                                                                                                      <NA>
  23:                                                                                                                                      <NA>
  24:                                                                                                                                      <NA>
  25:                                                                                                                                      <NA>
  26:                                                                                                                                      <NA>
  27:                                                                                                                                      <NA>
  28:                                                                                                                                      <NA>
  ReaderWarning
<char>
  SurveyStatus                       SurveyMessage ObservedType RowsSampled
<char>                              <char>       <char>       <num>
  1:        error input string 11986 is invalid UTF-8         <NA>          NA
2:        error input string 21950 is invalid UTF-8         <NA>          NA
3:        error input string 29390 is invalid UTF-8         <NA>          NA
4:        error    input string 74 is invalid UTF-8         <NA>          NA
5:        error    input string 19 is invalid UTF-8         <NA>          NA
6:           ok                                <NA>    character         175
7:           ok                                <NA>    character         175
8:           ok                                <NA>      integer         175
9:           ok                                <NA>      integer         175
10:           ok                                <NA>      integer         175
11:        error   input string 744 is invalid UTF-8         <NA>          NA
12:        error  input string 3320 is invalid UTF-8         <NA>          NA
13:        error  input string 3328 is invalid UTF-8         <NA>          NA
14:        error input string 66121 is invalid UTF-8         <NA>          NA
15:           ok                                <NA>    character         182
16:           ok                                <NA>      integer         182
17:           ok                                <NA>    character         182
18:           ok                                <NA>      integer         182
19:           ok                                <NA>      integer         182
20:        error   input string 383 is invalid UTF-8         <NA>          NA
21:        error   input string 383 is invalid UTF-8         <NA>          NA
22:        error input string 66121 is invalid UTF-8         <NA>          NA
23:        error   input string 212 is invalid UTF-8         <NA>          NA
24:        error    input string 95 is invalid UTF-8         <NA>          NA
25:        error   input string 299 is invalid UTF-8         <NA>          NA
26:        error  input string 1500 is invalid UTF-8         <NA>          NA
27:        error   input string 376 is invalid UTF-8         <NA>          NA
28:        error  input string 1442 is invalid UTF-8         <NA>          NA
SurveyStatus                       SurveyMessage ObservedType RowsSampled
<char>                              <char>       <char>       <num>
  NonMissingCount MissingPercent IntegerLike FractionalCount LeadingZeroCount
<num>          <num>      <lgcl>           <num>            <num>
  1:              NA             NA          NA              NA               NA
2:              NA             NA          NA              NA               NA
3:              NA             NA          NA              NA               NA
4:              NA             NA          NA              NA               NA
5:              NA             NA          NA              NA               NA
6:             175              0          NA              NA                0
7:             175              0          NA              NA               22
8:             175              0        TRUE               0                0
9:             175              0        TRUE               0                0
10:             175              0        TRUE               0                0
11:              NA             NA          NA              NA               NA
12:              NA             NA          NA              NA               NA
13:              NA             NA          NA              NA               NA
14:              NA             NA          NA              NA               NA
15:             182              0          NA              NA                0
16:             182              0        TRUE               0                0
17:             182              0          NA              NA               22
18:             182              0        TRUE               0                0
19:             182              0        TRUE               0                0
20:              NA             NA          NA              NA               NA
21:              NA             NA          NA              NA               NA
22:              NA             NA          NA              NA               NA
23:              NA             NA          NA              NA               NA
24:              NA             NA          NA              NA               NA
25:              NA             NA          NA              NA               NA
26:              NA             NA          NA              NA               NA
27:              NA             NA          NA              NA               NA
28:              NA             NA          NA              NA               NA
NonMissingCount MissingPercent IntegerLike FractionalCount LeadingZeroCount
<num>          <num>      <lgcl>           <num>            <num>
  NumericParseFailureCount Minimum Maximum MaximumTextLength PrecisionRisk
<num>  <char>  <char>             <num>        <lgcl>
  1:                       NA    <NA>    <NA>                NA            NA
2:                       NA    <NA>    <NA>                NA            NA
3:                       NA    <NA>    <NA>                NA            NA
4:                       NA    <NA>    <NA>                NA            NA
5:                       NA    <NA>    <NA>                NA            NA
6:                      175    <NA>    <NA>               153         FALSE
7:                        0    <NA>    <NA>                 6         FALSE
8:                        0       1       9                NA         FALSE
9:                        0    2005    2005                NA         FALSE
10:                        0    <NA>    <NA>                NA         FALSE
11:                       NA    <NA>    <NA>                NA            NA
12:                       NA    <NA>    <NA>                NA            NA
13:                       NA    <NA>    <NA>                NA            NA
14:                       NA    <NA>    <NA>                NA            NA
15:                      182    <NA>    <NA>               153         FALSE
16:                        0    2005    2005                NA         FALSE
17:                        0    <NA>    <NA>                 6         FALSE
18:                        0       1       9                NA         FALSE
19:                        0    <NA>    <NA>                NA         FALSE
20:                       NA    <NA>    <NA>                NA            NA
21:                       NA    <NA>    <NA>                NA            NA
22:                       NA    <NA>    <NA>                NA            NA
23:                       NA    <NA>    <NA>                NA            NA
24:                       NA    <NA>    <NA>                NA            NA
25:                       NA    <NA>    <NA>                NA            NA
26:                       NA    <NA>    <NA>                NA            NA
27:                       NA    <NA>    <NA>                NA            NA
28:                       NA    <NA>    <NA>                NA            NA
NumericParseFailureCount Minimum Maximum MaximumTextLength PrecisionRisk
<num>  <char>  <char>             <num>        <lgcl>
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
Error in FinalizeRepositorySchema(SchemaReviewPath, TableSchemaPath, strict = strict) : 
  Schema survey contains 18 unresolved source error(s); repair and resurvey before finalizing.

> repository_catalog
Error: object 'repository_catalog' not found
