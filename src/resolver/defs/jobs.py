from dagster import Definitions, define_asset_job, multiprocess_executor
#from resolver.defs.assets.er_blocks import er_blocks_adaptive

# ---- Jobs (asset selections) ----
blocking_job = define_asset_job(
  name="blocking_job",
  selection='key:"company_blocks_adaptive"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 4}),  # tune
)

pairing_job = define_asset_job(
  name="pairing_job",
  # Run all pairing shards, then the union
  selection='key:"er_pairs_shard_*" or key:"er_blocking_pairs"',
  executor_def=multiprocess_executor.configured({"max_concurrent": 4}),  # tune
)

features_job = define_asset_job(
  name="features_job",
  # Run all feature shards, then the union
  selection='key:"er_pair_features_shard_*" or key:"er_pair_features"',
  executor_def=multiprocess_executor.configured({"max_concurrent":
                                                 4}),  # start conservative
)

# ---- Concurrency limit (extra guardrail; optional if you use jobs) ----
# Keeps ANY ad-hoc runs from blasting too many feature shards at once.
#limits = [ConcurrencyLimit(key="er-pair-features", limit=2)]
