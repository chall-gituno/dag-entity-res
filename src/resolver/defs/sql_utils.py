# src/resolver/defs/sql_utils.py
from pathlib import Path
from jinja2 import Environment, PackageLoader, select_autoescape

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
