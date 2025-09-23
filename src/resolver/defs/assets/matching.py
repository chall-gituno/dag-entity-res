import dagster as dg
import duckdb
import pyarrow as pa, pyarrow.parquet as pq
from pathlib import Path
from resolver.defs.resources import DuckDBResource
from resolver.defs.settings import ERSettings
from typing import Dict, List


@dg.asset(
  name="er_pair_matches",
  group_name="er",
  deps=[dg.AssetKey("er_pair_scores")],
  compute_kind="sql",
)
def er_pair_matches(context, duckdb: DuckDBResource,
                    settings: ERSettings) -> dg.MaterializeResult:
  """
    Determine if two entities are the same based on 
    our model.  This is all about probablity and 
    NOT absolutes!
  """
  hi = settings.hi
  lo = settings.lo
  with duckdb.get_connection() as con:
    con.execute(
      """
        CREATE SCHEMA IF NOT EXISTS er;
        CREATE OR REPLACE TABLE er.pair_matches AS
        SELECT company_id_a, company_id_b, model_score,
              CASE WHEN model_score >= $hi THEN 'match'
                    WHEN model_score <= $lo THEN 'nonmatch'
                    ELSE 'review' END AS decision
        FROM er.pair_scores;
      """, {
        "hi": hi,
        "lo": lo
      })
    n = con.execute("SELECT COUNT(*) FROM er.pair_matches").fetchone()[0]

  return dg.MaterializeResult(metadata={
    "rows": dg.MetadataValue.int(int(n)),
    "hi": hi,
    "lo": lo
  })


@dg.asset(
  name="er_pair_labels",
  group_name="er",
  deps=[dg.AssetKey("er_pair_matches")],
  compute_kind="sql",
)
def er_pair_labels(context, duckdb: DuckDBResource) -> dg.MaterializeResult:
  with duckdb.get_connection() as con:
    con.execute("""
        CREATE OR REPLACE TABLE er.pair_labels AS
        SELECT company_id_a, company_id_b,
              CASE WHEN decision='match' THEN 1
                    WHEN decision='nonmatch' THEN 0
                    ELSE NULL END AS label_weak
        FROM er.pair_matches;
      """)
    counts = con.execute("""
        SELECT
          SUM(label_weak=1) AS pos,
          SUM(label_weak=0) AS neg,
          SUM(label_weak IS NULL) AS unlabeled
        FROM er.pair_labels;
      """).fetchone()

  pos, neg, unl = (int(counts[0] or 0), int(counts[1] or 0), int(counts[2] or 0))
  return dg.MaterializeResult(metadata={"pos": pos, "neg": neg, "unlabeled": unl})


# Disjoint Set Union (DSU) data structure, also known as a Union-Find data structure.
#  keep track of elements partitioned into non-overlapping subsets.
# It answers two specific questions very quickly:
#   Are two elements in the same set?
#   How can I merge two sets?


class DSU:
  __slots__ = ("parent", "rank")

  def __init__(self):
    self.parent: List[int] = []
    self.rank: List[int] = []

  def add(self) -> int:
    i = len(self.parent)
    self.parent.append(i)
    self.rank.append(0)
    return i

  def find(self, x: int) -> int:
    p = self.parent[x]
    if p != x:
      self.parent[x] = self.find(p)
    return self.parent[x]

  def union(self, a: int, b: int) -> None:
    ra, rb = self.find(a), self.find(b)
    if ra == rb: return
    if self.rank[ra] < self.rank[rb]:
      ra, rb = rb, ra
    self.parent[rb] = ra
    if self.rank[ra] == self.rank[rb]:
      self.rank[ra] += 1


def _conn(uri, mem, tmp, ro=True):
  con = duckdb.connect(uri, read_only=ro)
  con.execute("PRAGMA threads=1")
  con.execute(f"PRAGMA memory_limit='{mem}'")
  con.execute(f"PRAGMA temp_directory='{tmp}'")
  con.execute("PRAGMA enable_object_cache")
  return con


@dg.asset(
  name="er_entities",
  group_name="er",
  deps=[dg.AssetKey("er_pair_matches")],  # only depends on match decisions
  compute_kind="python",
  description=
  "Compute connected components (entity ids) from 'match' pairs via streaming union-find."
)
def er_entities(context, settings: ERSettings) -> dg.MaterializeResult:
  con = _conn(settings.db_uri, settings.duckdb_mem, settings.duckdb_tmp, ro=True)
  # stream ONLY 'match' edges (this is the whole graph to cluster)
  con.execute("""
        SELECT company_id_a, company_id_b
        FROM er.pair_matches
        WHERE decision = 'match'
    """)
  #reader = con.fetch_record_batch(batch_size=settings.batch_size)
  reader = con.fetch_record_batch(100_000)

  # ID compaction: map sparse company_ids -> 0..N-1 indices for DSU
  dsu = DSU()
  to_idx: Dict[int, int] = {}

  def idx(x: int) -> int:
    i = to_idx.get(x)
    if i is None:
      i = dsu.add()
      to_idx[x] = i
    return i

  # stream & union
  total_edges = 0
  while True:
    try:
      batch = reader.read_next_batch()
    except StopIteration:
      break
    if batch.num_rows == 0:
      break
    a = batch.column("company_id_a").to_pylist()
    b = batch.column("company_id_b").to_pylist()
    for u, v in zip(a, b):
      iu, iv = idx(u), idx(v)
      dsu.union(iu, iv)
    total_edges += batch.num_rows
    if total_edges % 5_000_000 == 0:
      context.log.info(f"Unioned {total_edges:,} edges; nodes so far {len(to_idx):,}")

  con.close()

  # Emit mapping (company_id -> representative). Use smallest original id per set if you prefer.
  # Here: rep = DSU root index; we then choose canonical entity_id as the MIN company_id in that set.
  # First collect members per root:
  from collections import defaultdict
  members: Dict[int, List[int]] = defaultdict(list)
  for cid, i in to_idx.items():
    r = dsu.find(i)
    members[r].append(cid)

  # Build Arrow table with canonical entity_id = min(company_id) of each component
  rows_company: List[int] = []
  rows_entity: List[int] = []
  for r, ids in members.items():
    ent = min(ids)  # canonical choice: smallest company_id in the component
    rows_company.extend(ids)
    rows_entity.extend([ent] * len(ids))
  out_parquet = settings.match_parquet_path
  out_dir = Path(out_parquet).parent
  out_dir.mkdir(parents=True, exist_ok=True)
  out_tbl = pa.table({
    "company_id": pa.array(rows_company, type=pa.int64()),
    "entity_id": pa.array(rows_entity, type=pa.int64()),
  })
  pq.write_table(out_tbl, out_parquet)

  # Register in DuckDB (streaming scan)
  conw = _conn(settings.db_uri, settings.duckdb_mem, settings.duckdb_tmp, ro=False)
  conw.execute("CREATE SCHEMA IF NOT EXISTS er")
  conw.execute(
    """
        CREATE OR REPLACE TABLE er.entities AS
        SELECT * FROM parquet_scan($p)
    """, {"p": out_parquet})

  # (Optional) include singletons that never appeared in any match edge:
  # conw.execute("""
  #   INSERT INTO er.entities
  #   SELECT id AS company_id, id AS entity_id
  #   FROM (SELECT DISTINCT company_id_a AS id FROM er.pair_features
  #         UNION SELECT DISTINCT company_id_b FROM er.pair_features) all_ids
  #   WHERE id NOT IN (SELECT company_id FROM er.entities)
  # """)
  n = conw.execute("SELECT COUNT(*) FROM er.entities").fetchone()[0]
  conw.close()

  return dg.MaterializeResult(
    metadata={
      "edges":
      dg.MetadataValue.int(int(total_edges)),
      "nodes":
      dg.MetadataValue.int(len(to_idx)),
      "entities_rows":
      dg.MetadataValue.int(int(n)),
      "parquet":
      dg.MetadataValue.path(out_parquet),
      "note":
      dg.MetadataValue.text("entity_id = min(company_id) per connected component"),
    })


# Brutally slow
@dg.asset(name="er_entities_sql",
          group_name="er",
          deps=[dg.AssetKey("er_pair_matches")],
          compute_kind="sql",
          description="Clusters companies by transitive closure over match edges.")
def er_entities_sql(context, duckdb: DuckDBResource) -> dg.MaterializeResult:
  with duckdb.get_connection() as con:
    con.execute("""
        -- build labels via simple propagation (can replace with Python union-find)
CREATE OR REPLACE TABLE er.entities AS
WITH RECURSIVE
edges AS (
  SELECT company_id_a AS a, company_id_b AS b
  FROM er.pair_matches
  WHERE decision = 'match'
),
-- make edges undirected
ud AS (
  SELECT a, b FROM edges
  UNION ALL
  SELECT b, a FROM edges
),
-- one seed row per node
seed AS (
  SELECT DISTINCT a AS id FROM ud
),
-- propagate reachability; UNION = DISTINCT to avoid infinite loops
reach(id, root) AS (
  SELECT id, id FROM seed
  UNION
  SELECT ud.b AS id, reach.root
  FROM ud
  JOIN reach ON ud.a = reach.id
),
labels AS (
  SELECT id, MIN(root) AS entity_id
  FROM reach
  GROUP BY 1
)
SELECT id AS company_id, entity_id
FROM labels;
      """)
    cnt = con.execute("SELECT COUNT(*) FROM er.entities").fetchone()[0]

  return dg.MaterializeResult(metadata={"entities": dg.MetadataValue.int(int(cnt))})
