# src/resolver/defs/iomanagers/ephemeral_parquet.py
from dagster import IOManager, OutputContext, InputContext
from pathlib import Path
import json


def _key(run_id: str, step_key: str, out_name: str, mapping_key: str | None) -> str:
  mk = mapping_key if mapping_key is not None else "nomap"
  # safe filename
  return f"{run_id}__{step_key}__{out_name}__{mk}".replace("/", "_")


class EphemeralParquetIOManager(IOManager):
  """
    Upstream ops return a *path* (string) to an on-disk Parquet file.
    We persist just that path in a tiny sidecar file so downstream
    processes (in multiprocess executors) can read it back.
    """

  def __init__(self, base_dir: str = "data/er/tmp_features_shards"):
    self.base_dir = Path(base_dir)

  # ---- Output side: persist the PATH so another process can load it ----
  def handle_output(self, context: OutputContext, obj: str) -> None:
    p = Path(str(obj)).resolve()
    p.parent.mkdir(parents=True, exist_ok=True)

    # emit convenient metadata
    context.add_output_metadata({
      "ephemeral_parquet_path": str(p),
      "exists": p.exists(),
      "size_bytes": p.stat().st_size if p.exists() else 0,
    })

    # write a tiny sidecar with the path so a different process can load it
    reg_dir = self.base_dir / f"run_{context.run_id}"
    reg_dir.mkdir(parents=True, exist_ok=True)

    key = _key(
      run_id=context.run_id,
      step_key=context.step_key,  # op step key
      out_name=context.name,  # output name (usually "result" or similar)
      mapping_key=context.mapping_key,  # dynamic mapping key (e.g., "0","1",...)
    )
    sidecar = reg_dir / f"{key}.json"
    sidecar.write_text(json.dumps({"path": str(p)}), encoding="utf-8")

  # ---- Input side: read the PATH back and return it to the downstream op ----
  def load_input(self, context: InputContext) -> str:
    u = context.upstream_output  # has run_id/step_key/name/mapping_key of the producer
    reg_dir = self.base_dir / f"run_{u.run_id}"
    key = _key(
      run_id=u.run_id,
      step_key=u.step_key,
      out_name=u.name,
      mapping_key=u.mapping_key,
    )
    sidecar = reg_dir / f"{key}.json"
    data = json.loads(sidecar.read_text(encoding="utf-8"))
    return data["path"]  # return the path string (downstream will use read_parquet)
