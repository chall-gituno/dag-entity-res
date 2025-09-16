from pathlib import Path

from dagster import definitions, load_from_defs_folder
from dotenv import load_dotenv

load_dotenv()


@definitions
def defs():
  return load_from_defs_folder(path_within_project=Path(__file__).parent)
