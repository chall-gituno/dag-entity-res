import os
import dagster as dg
from pydantic import Field


class ERSettings(dg.ConfigurableResource):
  db_uri: str = Field(default_factory=lambda: os.getenv(
    "DUCKDB_DATABASE", "/opt/test-data/experimental.duckdb"))
  duckdb_mem: str = Field(default_factory=lambda: os.getenv("ER_DUCKDB_MEM", "6GB"))
  duckdb_tmp: str = Field(
    default_factory=lambda: os.getenv("ER_DUCKDB_TMP", "/tmp/duckdb-temp"))

  # Features/Modeling/Scoring

  model_path: str = Field(
    default_factory=lambda: os.getenv("ER_MODEL_PATH", "models/er_pair_clf.joblib"))
  score_parquet_path: str = Field(default_factory=lambda: os.getenv(
    "ER_SCORING_PARQUET", "data/er/pair_scores/score.parquet"))
  batch_size: int = Field(
    default_factory=lambda: int(os.getenv("ER_BATCH_SIZE", "10000000")))

  # Entity Matching / Final Resolution
  match_parquet_path: str = Field(default_factory=lambda: os.getenv(
    "ER_MATCHING_PARQUET", "data/er/matching/match.parquet"))

  hi: float = Field(default_factory=lambda: float(os.getenv("ER_HI", "0.90")))
  lo: float = Field(default_factory=lambda: float(os.getenv("ER_LO", "0.10")))
