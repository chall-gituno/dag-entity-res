from dagster import define_asset_job, multiprocess_executor
#from resolver.defs.assets.er_blocks import er_blocks_adaptive

# ---- Jobs (asset selections) ----
blocking_job = define_asset_job(
  name="blocking_job",
  selection='key:"er_company_blocking"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 4}),  # tune
)

pairing_job = define_asset_job(
  name="pairing_job",
  # Run all pairing shards, then the union
  selection='key:"er_blocking_pairs"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 4}),  # tune
)

features_job = define_asset_job(
  name="features_job",
  selection='key:"er_pair_features"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 4}),
)

full_build = define_asset_job(
  name="full_build",
  selection='+key:"er_pair_features"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 4}),
)

# max_concurrent of 2 takes a while but at
# least the machine still operates
scoring_job = define_asset_job(
  name="scoring_job",
  selection='key:"er_pair_scores"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 2}),
)
scoring_job_streaming = define_asset_job(
  name="scoring_job_streaming",
  selection='key:"er_pair_scores_stream"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 1}),
)
# ---- Concurrency limit (extra guardrail; optional if you use jobs) ----
# Keeps ANY ad-hoc runs from blasting too many feature shards at once.
#limits = [ConcurrencyLimit(key="er-pair-features", limit=2)]
