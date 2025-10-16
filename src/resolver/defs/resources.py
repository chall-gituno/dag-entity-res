import dagster as dg

from dagster_duckdb import DuckDBResource
from resolver.defs.iomanagers import EphemeralParquetIOManager
from resolver.defs.settings import ERSettings

# from dagster_dbt import DbtCliResource
# from dagster_exp.defs.project import dbt_project

duckdb_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_DATABASE"), )

#dbt_resource = DbtCliResource(project_dir=dbt_project, )

defs = dg.Definitions(resources={
  "duckdb":
  duckdb_resource,
  "ephemeral_parquet_io":
  EphemeralParquetIOManager(base_dir="data/er/tmp_features_shards"),
  "settings":
  ERSettings(),
}, )
