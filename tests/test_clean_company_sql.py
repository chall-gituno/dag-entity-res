# tests/test_clean_company_sql.py
import duckdb, pandas as pd
from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from dotenv import load_dotenv

load_dotenv("./tests/.env.test")

from resolver.defs.sql_utils import render_sql

from unittest import mock


def test_clean_company_basic():
  con = duckdb.connect(database=':memory:')
  # Fixture (tiny, targeted)
  raw = pd.DataFrame({
    "name": [" Acme Inc ", "Foo, LLC", None],
    "domain": ["https://www.acme.com/", "foo.com", "nan"],
    "industry": ["Manufacturing", "   ", "null"],
    "linkedin_url": ["https://www.linkedin.com/company/acme/", "", None],
    "locality": ["Vancouver, BC, CA", "Seattle, WA", "Toronto, ON, CA"],
    "country": ["CA", "", None],
    "year founded": ["1999", "n/a", ""],
    "size range": ["51-200", None, "1-10"],
    "current employee estimate": ["120", "12", None],
    "total employee estimate": ["140", "15", ""],
  })

  con.execute("create schema bronze")
  con.execute("create schema silver")
  con.register("raw_df", raw)
  con.execute("create table bronze.companies as select * from raw_df")

  sql = render_sql(
    "clean_kag_comp.sql.j2",
    source_schema="bronze",
    target_schema="silver",
  )
  #print(f"Cleaner...\n{sql}")
  con.execute(sql)

  out = con.execute("select * from silver.companies").fetchdf()

  # Schema & type assertions
  assert "company_name" in out.columns
  assert out.year_founded.dtype.name in (
    "int32",
    "int64",
    "Int64",
    "Int32",
  )  # nullable Int okay

  # Content assertions (spot-checks)
  row0 = out.iloc[0]
  print(f"Row:\n{row0}")
  assert row0.company_name == "acme inc"
  assert row0.domain_name == "acme.com"
  assert row0.linkedin_url.startswith("linkedin.com/")
  assert row0.city == "vancouver"
  assert row0.final_country == "ca"
  assert row0.year_founded == 1999
