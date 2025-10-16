import json
import dagster as dg


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
