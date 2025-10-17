import json
import dagster as dg


def get_latest_meta(context, asset_key: str) -> dict:
  asset_key = dg.AssetKey([asset_key])
  # Get the latest materialization event
  last_mat_event = context.instance.get_latest_materialization_event(asset_key)
  if not last_mat_event:
    context.log.warning("No materialization found for asset")
    return

  # Access the metadata from the materialization
  materialization = last_mat_event.asset_materialization
  md = materialization.metadata
  return normalize_metadata(md)


def normalize_metadata(meta: dict) -> dict:
  """Convert Dagster MetadataValue objects to plain Python types for use in Jinja or AI prompts."""
  flat = {}
  for key, val in meta.items():
    # All MetadataValues have a .value or .data or .text attribute
    if isinstance(val, dg.MetadataValue):
      # Defensive handling across known subclasses
      if hasattr(val, "value"):
        flat[key] = val.value
      elif hasattr(val, "data"):
        try:
          flat[key] = json.loads(val.data)
        except json.JSONDecodeError:
          pass
      elif hasattr(val, "text"):
        flat[key] = val.text
      else:
        flat[key] = str(val)
    elif hasattr(val, "value"):  # handle older MetadataValue instances
      flat[key] = val.value
    else:
      flat[key] = val

  return flat
