import os
import dagster as dg
from pydantic import Field

from typing import Type, Any, Callable, Mapping


def get_typed_env(key: str, value_type: Type, default_value: Any) -> Callable[[], Any]:
  """
    Returns a default_factory callable that retrieves and casts an environment 
    variable, falling back to a default value if the variable is not set or 
    casting fails.
    """

  def factory() -> Any:
    env_val = os.getenv(key)

    if env_val is None:
      return default_value
    try:
      if value_type is bool:
        return env_val.lower() not in ('false', '0', 'no', 'f')

      return value_type(env_val)

    except (ValueError, TypeError):
      print(
        f"Warning: Failed to cast env var '{key}' ('{env_val}') to {value_type.__name__}. Using default."
      )
      return default_value

  return factory


class ERSettings(dg.ConfigurableResource):
  db_uri: str = Field(
    default=os.getenv("DUCKDB_DATABASE", "/opt/test-data/experimental.duckdb"))
  duckdb_mem: str = Field(default=os.getenv("ER_DUCKDB_MEM", "6GB"))
  duckdb_tmp: str = Field(default=os.getenv("ER_DUCKDB_TMP", "/tmp/duckdb-temp"))

  model_path: str = Field(
    default=os.getenv("ER_MODEL_PATH", "models/er_pair_clf.joblib"))

  # i.e., how many shards
  shard_modulus: int = Field(default_factory=get_typed_env(
    key="SHARD_MODULUS", value_type=int, default_value=64))

  ## Pairing.
  # if you are using sharding...
  pairs_parquet_out: str = Field(default=os.getenv(
    "ER_PAIRING_PARQUET",
    "/tmp/data/er/blocking_pairs_shards",
  ))
  pairs_with_kind_table: str = Field(
    default=os.getenv("PAIRS_WITH_KIND_TABLE", "er.block_pairs_with_kind"),
    description="Table with block pairs and the kind/key")

  pairs_table: str = Field(
    default=os.getenv("PAIRS_TABLE", "er.block_pairs"),
    description="Table containing candidate company ID pairs generated from blocking.")

  # Features
  features_parquet_out: str = Field(default=os.getenv(
    "ER_PAIRING_PARQUET",
    "/tmp/data/er/pair_features_shards",
  ))

  features_table: str = Field(
    default=os.getenv("FEATURES_TABLE", "er.pair_features"),
    description="Table holding engineered features for each candidate pair.")

  # Scoring
  score_parquet_path: str = Field(
    default=os.getenv("ER_SCORING_PARQUET", "/tmp/data/er/pair_scores/score.parquet"))
  batch_size: int = Field(default=int(os.getenv("ER_BATCH_SIZE", "10000000")))
  scores_table: str = Field(
    default=os.getenv("SCORES_TABLE", "er.pair_scores"),
    description="Table holding scores produced by our model for our pairs")
  scored_feature_pairs: str = Field(default=os.getenv("SCORED_FEATURE_PAIRS",
                                                      "er.v_pair_features_scored"),
                                    description="View with our scored feature pairs")

  # Entity Matching / Final Resolution
  match_parquet_path: str = Field(
    default=os.getenv("ER_MATCHING_PARQUET", "/tmp/data/er/matching/match.parquet"))

  hi: float = Field(default=float(os.getenv("ER_HI", "0.90")))
  lo: float = Field(default=float(os.getenv("ER_LO", "0.10")))

  # Table references
  raw_companies: str = Field(default=os.getenv("RAW_COMPANIES", "bronze.companies"),
                             description="Source table for raw company records.")

  clean_companies: str = Field(default=os.getenv("CLEAN_COMPANIES", "silver.companies"),
                               description="Cleaned and standardized company table.")

  blocks_table: str = Field(
    default=os.getenv("BLOCKS_TABLE", "er.company_blocks"),
    description="Table holding company blocking keys and block membership.")
