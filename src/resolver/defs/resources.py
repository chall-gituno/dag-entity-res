import dagster as dg

from dagster_duckdb import DuckDBResource

# from dagster_dbt import DbtCliResource
# from dagster_exp.defs.project import dbt_project

duckdb_resource = DuckDBResource(database=dg.EnvVar("DUCKDB_DATABASE"), )

print(duckdb_resource)
#dbt_resource = DbtCliResource(project_dir=dbt_project, )

defs = dg.Definitions(
  resources={
    "duckdb": duckdb_resource,
    #"dbt": dbt_resource,
  }, )
