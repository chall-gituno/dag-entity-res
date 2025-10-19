# tests/system_state_check.py
import duckdb
import joblib
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from resolver.defs.settings import ERSettings

load_dotenv()


def main():

  settings = ERSettings()

  QUERIES = {
    "blocks_total": f"SELECT COUNT(*) FROM {settings.blocks_table}",
    "blocking_pairs_total": f"SELECT COUNT(*) FROM {settings.pairs_table}",
    "pair_features_total": f"SELECT COUNT(*) FROM {settings.features_table}",
    "pair_scores_total": f"SELECT COUNT(*) FROM {settings.scores_table}",
    "pair_features_scored_total":
    f"SELECT COUNT(*) FROM {settings.scored_feature_pairs}",
    #"entities_total": "SELECT COUNT(*) FROM er.entities",
    #"entities_unique": "SELECT COUNT(DISTINCT entity_id) FROM er.entities",
    # "pair_scores_nulls": """
    #   SELECT COUNT(*) AS null_scores
    #   FROM er.pair_scores
    #   WHERE model_score IS NULL
    # """,
    # "features_dupes": """
    #   SELECT COUNT(*) FROM (
    #     SELECT company_id_a, company_id_b, COUNT(*) AS c
    #     FROM er.pair_features
    #     GROUP BY 1,2 HAVING COUNT(*)>1
    #   )
    # """,
  }

  con = duckdb.connect(settings.db_uri, read_only=True)
  results = {}

  for name, sql in QUERIES.items():
    try:
      val = con.execute(sql).fetchone()[0]
      print(f" ---- {name} -----\n{sql}\n----")
      results[name] = val
    except Exception as e:
      results[name] = f"ERROR: {e}"

  con.close()

  # Try to inspect the model if it exists
  model_path = Path(settings.model_path)
  if model_path.exists():
    try:
      clf = joblib.load(model_path)
      results["model_type"] = type(clf).__name__
      if hasattr(clf, "n_features_in_"):
        results["model_features"] = clf.n_features_in_
    except Exception as e:
      results["model_load_error"] = str(e)
  else:
    results["model_status"] = "missing"

  # Pretty-print summary
  df = pd.DataFrame(list(results.items()), columns=["metric", "value"])
  print(df.to_string(index=False))


if __name__ == "__main__":
  main()
