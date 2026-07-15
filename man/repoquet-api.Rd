\name{repoquet-api}
\alias{repoquet-api}
\alias{acquire_repository_lock}
\alias{align_columns}
\alias{allowed_canonical_types}
\alias{apply_builtin_schema_registry_policies}
\alias{apply_schema_registry}
\alias{arrow_schema_from_classes}
\alias{audit_repository}
\alias{availability_sheet_map_name}
\alias{availability_sheet_name_for}
\alias{begin_repository_run}
\alias{build_col_classes}
\alias{build_col_classes_by_table}
\alias{build_comprehensive}
\alias{build_data_contract_template}
\alias{build_default_schema_registry}
\alias{BuildRepositoryCatalog}
\alias{BuildRepositorySchema}
\alias{SurveyRepositorySchema}
\alias{RecommendRepositorySchema}
\alias{WriteSchemaProposal}
\alias{FinalizeRepositorySchema}
\alias{PrepareSchemaRegistry}
\alias{FinalizeSchemaRegistry}
\alias{GetSchemaObservations}
\alias{load_value_dictionary}
\alias{load_column_dictionary}
\alias{column_dictionary_path}
\alias{value_dictionary_path}
\alias{describe_column}
\alias{decode_column}
\alias{cleanup_temp_files}
\alias{safe_unlink}
\alias{call_reader}
\alias{canonical_colnames}
\alias{canonical_type_to_arrow}
\alias{canonicalize_dataframe_names}
\alias{checkpoint_completed_mask}
\alias{coerce_to_class}
\alias{coercion_report_collect}
\alias{coercion_report_reset}
\alias{coercion_report_write}
\alias{column_availability}
\alias{ColumnAvailabilityCompile}
\alias{ColumnAvailabilityView}
\alias{contract_literal}
\alias{create_repository_project}
\alias{DBDimPerTable}
\alias{DBViewSummary}
\alias{delimited_fread_args}
\alias{discover_schema_relationships}
\alias{duckdb_sql_type_for_canonical}
\alias{duckdb_type_matches}
\alias{duckdb_type_pattern}
\alias{enforce_col_classes}
\alias{ExportRepositoryMetadata}
\alias{excel_sheet_name}
\alias{flush_log_buffer}
\alias{fread_col_classes}
\alias{fread_col_classes_positional}
\alias{generate_example_repository}
\alias{generate_real_world_repository}
\alias{real_world_source_catalog}
\alias{generic_db_loader}
\alias{get_file_reader}
\alias{harvest_sav_labels}
\alias{harvest_source_labels}
\alias{hcup_weighted_count}
\alias{hcup_weighted_mean}
\alias{hcup_weighted_summary}
\alias{hive_partition_types}
\alias{is_allowed_canonical_type}
\alias{is_excel_workbook_path}
\alias{label_catalog_path}
\alias{load_checkpoint}
\alias{load_data_contracts}
\alias{load_label_catalog}
\alias{load_repository_config}
\alias{load_schema_registry}
\alias{load_table_schema_catalog}
\alias{log_buffer_get}
\alias{log_buffer_set}
\alias{log_msg}
\alias{make_typed_na}
\alias{manifest_add_missing_columns}
\alias{manifest_is_duckdb}
\alias{manifest_path_default}
\alias{manifest_table_name}
\alias{manifest_transaction}
\alias{metadata_workbook_path_default}
\alias{MDTCompleteStatus}
\alias{merge_key_name_candidate}
\alias{merge_table_schema_catalog}
\alias{migrate_checkpoint_keys}
\alias{MaterializeRemoteSources}
\alias{new_repository_run_id}
\alias{normalize_character_encoding}
\alias{normalize_repo_path}
\alias{normalize_type_name}
\alias{open_duckdb}
\alias{parquet_chunk_stem}
\alias{parquet_output_stem}
\alias{parquet_safe_stem}
\alias{ParquetBackEndCreate}
\alias{parse_reader_options_json}
\alias{parse_value_label_codes}
\alias{partition_spec_for_row}
\alias{partition_types_for_row}
\alias{print.RepositoryRunResult}
\alias{promote_types}
\alias{quote_duckdb_ident}
\alias{quote_duckdb_string}
\alias{read_availability_sheet_map}
\alias{read_delimited_chunked}
\alias{read_delimited_full}
\alias{read_fn}
\alias{read_haven_full}
\alias{read_parquet_manifest}
\alias{read_sav_with_options}
\alias{reader_options_for_row}
\alias{reader_supports_labels}
\alias{regex_escape}
\alias{register_builtin_file_readers}
\alias{register_file_reader}
\alias{register_parquet_view}
\alias{register_parquet_view_compile}
\alias{release_repository_lock}
\alias{remove_parquet_manifest_rows}
\alias{rename_checkpoint_table}
\alias{replace_file_safely}
\alias{repository_checkpoint_key}
\alias{repository_checkpoint_legacy_key}
\alias{repository_lock_path_default}
\alias{repository_table_name_for_row}
\alias{repository_table_names}
\alias{RepositoryInitialize}
\alias{reset_table_for_reload}
\alias{resolve_log_path}
\alias{resolve_run_id}
\alias{restore_repository_run}
\alias{safe_read_csv}
\alias{safe_read_sav}
\alias{safe_read_sav_chunked}
\alias{sanitize_partition_value}
\alias{save_checkpoint}
\alias{scan_for_new_source_files}
\alias{schema_hash_from_classes}
\alias{schema_map_to_long}
\alias{schema_registry_applies}
\alias{schema_registry_match}
\alias{search_diagnosis_codes}
\alias{search_labels}
\alias{snapshot_repository_state}
\alias{source_fingerprint}
\alias{source_path_for_row}
\alias{strip_haven}
\alias{SummaryVerification}
\alias{supported_file_types}
\alias{table_partition_keys}
\alias{table_partition_types}
\alias{touch_repository_lock}
\alias{update_parquet_manifest}
\alias{upgrade_checkpoint_source_fingerprints}
\alias{validate_against_dictionary}
\alias{validate_data_contracts}
\alias{validate_duckdb_table}
\alias{validate_partition_column_values}
\alias{ValidateMDTPreflight}
\alias{ValidateSchemaMergeKeys}
\alias{verify_source_empty}
\alias{weighted_count}
\alias{weighted_mean}
\alias{weighted_summary}
\alias{WorkbookUpdateopenxlsx}
\alias{write_arrow_table_safely}
\alias{write_csv_safely}
\alias{write_parquet_manifest_atomic}
\alias{write_repository_run_summary}
\alias{write_schema_registry}
\alias{write_table_schema_catalog}
\alias{write_xlsx_safely}
\alias{write_year_parquet}
\title{repoquet Repository Workflow API}
\description{
Functions used to initialize, load, validate, catalog, audit, and query a
schema-normalized Parquet repository. Low-level helpers remain exported for
compatibility with the original script-based workflow.
}
\details{
Start with \code{create_repository_project()} for a scaffolded project,
\code{RepositoryInitialize()} for an existing project, and
\code{ValidateMDTPreflight()} before calling
\code{ParquetBackEndCreate()}. Use \code{PrepareSchemaRegistry()} to write a
Parquet-backed observation survey and compact review workbook, then
\code{FinalizeSchemaRegistry()} to produce the approved table catalog. Use
\code{GetSchemaObservations()} for focused access to detailed evidence.
Use \code{ExportRepositoryMetadata()} to create a navigable Excel snapshot of
the authoritative transactional manifest.
Low-cardinality value evidence appears in the review workbook's
\code{ValuePreview} sheet and the finalized catalog's
\code{ValueDictionary} sheet.
Source-provided and user-approved code meanings are reviewed in
\code{DictionaryReview} and finalized to the partition-aware
\code{ColumnDictionary} sheet. Use \code{describe_column()} to inspect the
schema and both value layers, or \code{decode_column()} for a bounded DuckDB
query with a decoded label column.
The legacy \code{BuildRepositoryCatalog()} remains available. Use
\code{audit_repository()} and \code{validate_data_contracts()} to inspect
and validate the resulting repository. See the package README for complete
workflow examples and configuration fields.
}
\keyword{database}
\keyword{utilities}
