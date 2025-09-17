# assets/hello.py
from dagster import asset, define_asset_job


@asset
def hello_world_asset() -> str:
  return "Hello, Dagster!"


hello_job = define_asset_job(name="hello_job", selection="hello_world_asset")
