import dagster as dg

# ---- Jobs (asset selections) ----
ingest_data_job = dg.define_asset_job(
  name="ingest_data_job",
  description="Ingest and clean our foundational data",
  selection='+key:"clean_companies"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1}),
)
blocking_job = dg.define_asset_job(
  name="blocking_job",
  description="Create company blocks using an adaptive strategy",
  selection='key:"er_company_blocking"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

pairing_job = dg.define_asset_job(
  name="pairing_job",
  selection='key:"er_company_pairs"',
  description="Create our pairs from our blocks",
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)
sharded_pairing_job = dg.define_asset_job(
  name="sharded_pairing_job",
  selection='key:"er_sharded_blocking_pairs"',
  description="Create our pairs from our blocks using sharding",
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

features_job = dg.define_asset_job(
  name="features_job",
  selection='key:"er_pair_features"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

materialize_foundation = dg.define_asset_job(
  name="materialize_foundation_job",
  selection='+key:"er_pair_features"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

## Assumes our foundational assets are all materialized AND we
## have built our model
scoring_job = dg.define_asset_job(
  name="scoring_job",
  selection='key:"er_pair_scores"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1}),
)

matching_selection = dg.AssetSelection.keys(
  "er_pair_matches",
  "er_pair_labels",
  "er_entities",
)

matching_job = dg.define_asset_job(
  name="matching_job",
  selection=matching_selection,
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 1}),
)

resolve_comp_job = dg.define_asset_job(
  name="resolve_comp_job",
  selection='key:"er_resolve_companies"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

# NOTE: the '+' here will force a rebuild of all upstream dependencies
er_pipeline_job = dg.define_asset_job(
  name="er_pipeline_job",
  selection='+key:"er_resolve_companies"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)
