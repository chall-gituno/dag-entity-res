#!/usr/bin/env python3
import argparse
import duckdb
import os
import sys
from dotenv import load_dotenv


def format_table(headers, rows):
  """Return a pretty-printed table as string."""
  str_rows = [[str(c) for c in row] for row in rows]
  widths = [len(h) for h in headers]

  for row in str_rows:
    for i, col in enumerate(row):
      widths[i] = max(widths[i], len(col))

  def fmt_row(row):
    return " | ".join(col.ljust(widths[i]) for i, col in enumerate(row))

  sep = "-+-".join("-" * w for w in widths)
  lines = [fmt_row(headers), sep]
  for row in str_rows:
    lines.append(fmt_row(row))
  return "\n".join(lines)


def main():
  load_dotenv()  # load .env file if present

  parser = argparse.ArgumentParser(
    description="Run a SQL file against a DuckDB database")
  parser.add_argument(
    "-d",
    "--db",
    help="Path to DuckDB database file (default: env DUCKDB_DATABASE or in-memory)")
  parser.add_argument("-f", "--file", required=True, help="Path to SQL file")
  args = parser.parse_args()

  # precedence: CLI > ENV > default
  db_path = args.db or os.getenv("DUCKDB_DATABASE", ":memory:")

  # Read SQL from file
  try:
    with open(args.file, "r", encoding="utf-8") as f:
      sql = f.read()
  except OSError as e:
    sys.stderr.write(f"Error reading SQL file: {e}\n")
    sys.exit(1)

  try:
    con = duckdb.connect(db_path)
    cur = con.execute(sql)
    rows = cur.fetchall()
    headers = [d[0] for d in cur.description] if cur.description else []

    if headers and rows:
      print(format_table(headers, rows))
    elif headers:
      print("No rows returned")
      print(" | ".join(headers))
    else:
      print("Query executed successfully.")

    con.close()
  except Exception as e:
    sys.stderr.write(f"Query failed: {e}\n")
    sys.exit(1)


if __name__ == "__main__":
  main()
