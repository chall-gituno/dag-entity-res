#from dagster import define_asset_job, multiprocess_executor, AssetSelection
import dagster as dg
#from resolver.defs.assets.er_blocks import er_blocks_adaptive

# ---- Jobs (asset selections) ----
blocking_job = dg.define_asset_job(
  name="blocking_job",
  description="Create company blocks using an adaptive strategy",
  selection='key:"er_company_blocking"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),
)

pairing_job = dg.define_asset_job(
  name="pairing_job",
  # Run all pairing shards, then the union
  selection='key:"er_blocking_pairs"',
  executor_def=dg.multiprocess_executor.configured({"max_concurrent": 4}),  # tune
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
