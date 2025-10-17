#from dagster import define_asset_job, multiprocess_executor, AssetSelection
import dagster as dg
#from resolver.defs.assets.er_blocks import er_blocks_adaptive

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
  selection='key:"er_company_blocking_pairs"',
  description="Create our pairs from our blocks using sharding",
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

materialize_foundation = dg.define_asset_job(
  name="materialize_foundation_job",
  selection='+key:"er_company_blocking_pairs"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

features_job = dg.define_asset_job(
  name="features_job",
  selection='key:"er_pair_features"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

# max_concurrent of 2 takes a while but at
# least the machine still operates
# scoring_job_sharded = dg.define_asset_job(
#   name="scoring_job_sharded",
#   selection='key:"er_pair_scores_sharded"',
#   executor_def=dg.multiprocess_executor.configured({"max_concurrent": 2}),
# )

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
  tags={
    "owner": "er",
    "layer": "matching"
  },
)
# uv run dagster asset materialize -m resolver.definitions --select 'key:"sanity_*"'
sanity_job = dg.define_asset_job(
  name="sanity_job",
  selection='key:"sanity_*"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 2}),
)

# NOTE: the '+' here will force a rebuild of all upstream
# dependencies regardless of whether they need them or not
full_build = dg.define_asset_job(
  name="full_build",
  selection='+key:"er_entities"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)
# ---- Concurrency limit (extra guardrail; optional if you use jobs) ----
# Keeps ANY ad-hoc runs from blasting too many feature shards at once.
#limits = [ConcurrencyLimit(key="er-pair-features", limit=2)]
