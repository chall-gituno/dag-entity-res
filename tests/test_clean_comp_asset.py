import duckdb
import pytest_check as check
import pandas as pd
from dagster import materialize
import logging

logging.getLogger("dagster").setLevel(logging.WARNING)
logging.getLogger("dagster._core").setLevel(logging.ERROR)
logging.getLogger("dagster._utils").setLevel(logging.ERROR)

from resolver.defs.assets.companies import clean_companies


def test_clean_pipeline_on_sample(sampled_duckdb):

  # 1) Run only the cleaning asset against the sampled DB resource
  result = materialize([clean_companies],
                       resources={"duckdb": sampled_duckdb},
                       raise_on_error=True)
  assert result.success

  # 2) Open the same sampled DB to inspect results
  with sampled_duckdb.get_connection() as con:
    # Sanity checks
    total = con.execute("select count(*) from silver.companies").fetchone()[0]
    assert total > 0

    # Null-rate thresholds (adjust to your quality targets)
    domain_null = con.execute(
      "select AVG(CAST(domain_name IS NULL AS INT)) from silver.companies").fetchone(
      )[0]
    check.less(domain_null, 0.10, f"domain null rate too high: {domain_null:.3f}")

    # Types / ranges
    # year_founded either NULL or within plausible bounds
    bad_years = con.execute("""
      select count(*) from silver.companies
      where year_founded is not null
        and (year_founded < 1800 or year_founded > 2100)
    """).fetchone()[0]
    check.equal(bad_years, 0, f"There are years outside accepted range")

    # Basic normalization invariants
    sample = con.execute("""
      select company_name, domain_name, linkedin_url, city, final_country
      from silver.companies
      limit 50
    """).fetch_df()

  # 3) Pandas-side assertions on sample invariants
  assert sample["company_name"].str.lower().equals(
    sample["company_name"])  # all lowercased
  assert sample["domain_name"].str.startswith("http").sum() == 0  # stripped scheme
  # country should be 2-char alpha or null (tune if you use country names)
  mask = sample["final_country"].dropna().str.match(r"^[a-z]{2}$")
  assert mask.all()
