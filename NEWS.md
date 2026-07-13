# repoquet 0.1.0

- Extracted the reusable repository engine from the CECORC workflow.
- Added schema-aware ingestion, generalized Hive partitions, source
  fingerprints, transactional manifests, resumable checkpoints, data
  contracts, repository auditing, and validated DuckDB view registration.
- Added a project scaffold, command-line workflow, reproducible dependency
  lockfile, continuous integration, and a 274-expectation regression suite.
- Added a user-guided schema workflow: detailed source observations are stored
  in Parquet, a compact Excel workbook exposes recommendations and type history,
  and approved decisions finalize into the existing writer catalog.
- Redesigned `SchemaReview.xlsx` around a `StartHere` dashboard. Only unresolved
  column and compatibility decisions remain visible; policy results are clearly
  informational and advanced registries/history are hidden by default.
- CSV readers now preserve leading-zero fields by default, with a per-source
  `KeepLeadingZeros` reader option for explicit control.
