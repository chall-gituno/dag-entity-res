# src/resolver/defs/sql_utils.py
from pathlib import Path
from jinja2 import Environment, PackageLoader, select_autoescape
import duckdb
import os

# Load templates from resolver.defs.sql
sqlj2_env = Environment(
  loader=PackageLoader("resolver.defs", "sql"),
  autoescape=select_autoescape([])  # no autoescape for .sql.j2
)


def render_sql(template_name: str, **params) -> str:
  return sqlj2_env.get_template(template_name).render(**params)


def render_sql_to_file(template_name: str, out_path: str | Path, **params) -> Path:
  sql = render_sql(template_name, **params)
  out_path = Path(out_path)
  out_path.parent.mkdir(parents=True, exist_ok=True)
  out_path.write_text(sql, encoding="utf-8")
  return out_path


def connect_duckdb(db_uri: str, read_only: bool = True) -> duckdb.DuckDBPyConnection:
  con = duckdb.connect(db_uri, read_only=read_only)
  # keep per-process memory/threading tame
  con.execute("PRAGMA threads=1")
  # You can tune this per box
  con.execute(f"PRAGMA memory_limit='{os.getenv('DUCKDB_MEM', '6GB')}'")
  con.execute(f"PRAGMA temp_directory='{os.getenv('DUCKDB_TMP', '/tmp/duckdb-temp')}'")
  return con
