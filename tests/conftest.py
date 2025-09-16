# tests/conftest.py
import duckdb
import pandas as pd
import pytest
from pathlib import Path
import os

#from resolver.defs.resources import DuckDBResource
from dagster_duckdb import DuckDBResource

from dotenv import load_dotenv

load_dotenv()
load_dotenv("./tests/.env.test")


def create_sample_db(
  table_name: str,
  src_db: str | None,
  src_parquet_glob: str | None,
  dst_db: str,
  *,
  sample_frac: float = 0.05,
  seed: int = 42,
) -> None:
  """
  Create a small DuckDB at dst_db with bronze tables sampled from either:
    - another DuckDB (src_db), or
    - a parquet glob (src_parquet_glob)
  """
  con = duckdb.connect(dst_db)
  con.execute("create schema if not exists bronze")
  #con.execute("set seed = ?", [seed])

  if src_db:

    con.execute(f"attach '{src_db}' as src (read_only)")
    con.execute(f"drop table if exists bronze.{table_name}")
    con.execute("""
      create table bronze.{table_name} as
      select * from src.bronze.{table_name}
      using sample {frac} percent (bernoulli)
    """.format(frac=sample_frac * 100, table_name=table_name, seed=seed))
    con.execute("detach src")

  elif src_parquet_glob:
    # sample from parquet files (replace column list with * if you trust schema)
    con.execute(f"""
      create table bronze.{table_name} as
      select * from read_parquet('{src_parquet_glob}')
      where random() < {sample_frac}
    """)
  else:
    raise ValueError("Provide either src_db or src_parquet_glob")

  con.close()


@pytest.fixture
def sampled_duckdb(tmp_path) -> DuckDBResource:
  """
  Builds a sampled DB for tests and returns a DuckDBResource bound to it.
  - Adjust one of src_db/src_parquet_glob to your environment.
  """
  dst = os.getenv("TEST_DUCKDB")

  # OPTION A: sample from an existing full DB file
  create_sample_db(table_name="companies",
                   src_db=os.getenv("DUCKDB_DATABASE"),
                   src_parquet_glob=None,
                   dst_db=str(dst))

  # OPTION B: sample from parquet files (Kaggle dump after your load step)
  # create_sample_db(
  #   src_db=None,
  #   src_parquet_glob="data/bronze/companies/*.parquet",  # <- change to your source
  #   dst_db=str(dst),
  #   sample_frac=0.15,
  #   seed=123)

  return DuckDBResource(database=str(dst))
