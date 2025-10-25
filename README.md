> **NOTICE**: This repository is published as a read-only snapshot. It is a **work in progress** but we are not accepting pull requests, issues, or contributions. See CONTRIBUTING.md for details.  That being said, feel free to clone and mess around as you see fit.

# Entity Resolution

Entity resolution — determining whether two records refer to the same real-world entity — is a recurring challenge in data engineering:

> "Is this thing (a company, customer, or widget) the same as that other thing from another dataset?"

This project uses a logistic regression model to explore one practical approach to entity resolution.  
In addition to standard validation logic and asset checks, it incorporates AI-assisted evaluations — not as ground truth,  
but as qualitative opinions that complement the statistical model.  
The goal is to illustrate how human-style reasoning can coexist with deterministic validation in assessing data similarity.

The focus is on methodology and reasoning, not on producing a single, authoritative solution.

You can think of it as a concrete expression of a mental model: how to frame, test, and reason about similarity and matching across datasets.

> This is actually just a pipeline i setup as part of testing integration for the [Leifer Project](https://github.com/leifer-labs/leifer)

## Setup

### The Data

The data we use for this experiment is a companies dataset from from kaggle - pretty sure it is [this one](https://www.kaggle.com/datasets/rsaishivani/companies-database). See the [companies asset](src/resolver/defs/assets/companies.py) asset for more info.  It only has a handful of fields so the features we can get from it are limited but it'll do for the purposes of this experiment.

### Environment

This project is managed with `uv` so...

**Step Number One** - [install uv](https://docs.astral.sh/uv/getting-started/installation)

Next, copy the dotenv.example to .env and adjust as needed.  Most importantly, you need to point it at your companies data file you downloaded.  If you want to enable AI feedback, add your `OPENAI_API_KEY` to .env as well.

We are using [DuckDB](https://duckdb.org) as our embedded analytical database and [Dagster](https://dagster.io) as the workflow orchestration framework.  For the most part, processing is handled via SQL so we can offload heavy lifting to the database.

There is a primitive [sql templating system](src/resolver/defs/sql/) that uses jinja for some of our more complex SQL scripts...sort of a rudimentary `dbt`.  There is also a templating system for [AI prompts](src/resolver/defs/prompts/) if you have enabled that (i.e., added your OPENAI_API_KEY to .env).

You'll also probably want to install the duckdb cli using `brew` or similar if you want to poke around in the raw data.  This project uses a medallion system (albeit somewhat loosely) so if you are poking around in the data, the main schemas are `bronze` for raw, `silver` for cleaned and (eventually) `gold` for our final outputs.  We also have an `er` schema for entity resolution working tables.

### Complete the Setup

Sync your virtual env...

```sh
uv sync
```

Then create a dagster home and add a dagster.yaml to it.  This will stop dagster from warning you every time you run it.
```sh
mkdir .dagster_home
touch .dagster_home/dagster.yaml
export DAGSTER_HOME=$PWD/.dagster_home
```

Now you can do a quick sanity check to make sure dagster is setup and working

```sh
uv run dg 
# or if you want to start the dev instance
uv run dg dev 
```

> NOTE: This is NOT a tutorial on how to use [Dagster](https://docs.dagster.io) but check out their docs - they are good.  The documentation AI Assistant is also quite helpful.

## The Entity Resoultion Pipeline

The broad strokes...

1. Ingest & Clean (bronze -> silver)
2. Blocking & Pairing (multi-strategy block generation, adaptive block capping)
3. Feature Engineering (numerical + categorical similarity metrics)
4. Weakly Labeled Model Training (scikit-learn pipeline, persisted with joblib)
5. Scoring (streaming inference → pair_scores)
6. Entity Resolution / Clustering (union-find → er.entities)
7. Resolved Canonical Companies

## Processing

First a quick orientation.

What do we have in the toolbox...

```sh
uv run dg list defs
```

If you have the UI running (`uv run dg dev`), you can click on the hamburger in the upper left to see a good overview.  Or you can click on the `View Lineage` link from the `Assets` page to get the big picture.


### Ingestion/Cleaning

Nothing out of the ordinary here.  Mainly normalization so we have a good foundation to build upon.

Assuming you have your .env setup to point at the zip file containing companies data you grabbed from kaggle, you can materialize the raw data and the clean version either from the web ui or from the cli...

```sh
uv run dg launch --module-name resolver.definitions --assets '+key:"clean_companies"'
```

After that finishes you will have a couple of new tables.
Check out what you have...

```sh
duckdb /path/to/duckdatabase.duckdb
```

```sql
SELECT
    table_schema || '.' || table_name AS full_table_name,
    table_type
FROM
    information_schema.tables
ORDER BY
    full_table_name;
```

### Blocking

You could probably block and pair in a single step but that might cause issues if you try it with our dataset and DuckDB defaults (assuming you are running on a fairly average dev machine). Also, we want to be able to tweak our blocking strategy independant of producing pairs.  So we run blocking and pairing as separate processes.  Blocking is done in multiple stages in an attempt to arrive at our optimal block sizes.

But what exactly _is_ blocking and why do we need an optimal size?  If you are a data engineer, you would probably say that the goal of blocking is to "maximize recall while minimizing the number of comparisons".  If you are NOT a data engineer that may sound like one of those things you can almost get a handle on but is still just abstract enough to keep slipping away.  

So to clear it up, we'll say instead that we need blocking because of the O(n²) problem.  Simply put, that means that without blocking, we have an **order of growth** that is quadratic...meaning every record (`n`) needs to be compared to **every other record**

```text
Comparisons = n x (n - 1) / 2

- Each record compares to (n − 1) others.
- Divide by 2 so we don’t count both A–to-B and B–to-A as separate comparisons.

n = 1,000     →  (1000 × 999) / 2  = 499,500
n = 10,000    →  (10000 × 9999) / 2 = 49,995,000
n = 1,000,000 →  (1000000 × 999999) / 2 ≈ 499,999,500,000
```

So the goal is to try to break all of those comparisons into blocks of a more manageable size.

A **good** blocking strategy finds most of the real matches while keeping each block small enough to process comfortably - that is the "optimal size" we are going for.

A **bad** blocking strategy either:

- Creates a few giant blocks (too much work, memory blow-ups, cpu melts, etc), or
- Creates too many tiny, fragmented blocks (misses matches spread across them).

We run our blocking using a job so we can parallelize the work...
> Note if you want to get AI's opinion on the blocking strategy, add your `OPENAI_API_KEY` to .env

```sh
# Create the blocks
uv run dg launch --job blocking_job
# Create the pairs
uv run dg launch --job pairing_job
```

If you have AI enabled, you'll see it suggests we do some more work on our blocking...we'll put that on the future refinements TODO and move on.

You can do a quick sanity check if you like...
```sh
duckdb /path/to/db.duckdb -f artifacts/block_pair_sanity.sql
```

### Feature Engineering

After blocking and pairing, we have a big list of candidate record pairs — now we need to describe each pair in a way that helps the model tell if they refer to the same entity.
That’s what feature engineering is all about.

We build features that measure how similar two records are across key attributes (name, country, domain, employee counts, etc).

Some are numerical (differences or ratios), others are categorical (exact match, prefix match, one-hot encoded country or city).

We generate these features in parallel shards so that large datasets can be processed efficiently on normal hardware.
The result is a table of pairwise feature vectors — each row describes one candidate pair.

```sh
uv run dg launch --job features_job
```

### Weakly Labeled Model Training

Once we have features, we need a way to classify pairs as “match” or “non-match.”
Ideally, we’d have a big set of human-labeled examples — but that’s rarely realistic.
So we start with weak labeling: we generate approximate labels based on simple heuristics.

For example:
- If domain_exact == 1 or (country_exact == 1 and name_prefix6_eq == 1), call it a positive.
- If country_exact == 0 and names differ strongly, call it a negative.

These rough labels won’t be perfect, but they’re usually good enough for a model like Logistic Regression or Random Forest to learn useful boundaries.

see the [notebook](exp/modelling.ipynb) for what we are doing.  we save the model so we can use it in our future processing steps.

### Scoring

Scoring takes the trained model and applies it to every candidate pair to estimate a match probability.

Each pair gets a model_score between 0 and 1:
- Close to 1 → likely the same entity
- Close to 0 → probably different

Because this can involve hundreds of millions of pairs, we run our scored features through the model in batches.

```sh
uv run dg launch --job scoring_job

```

### Entity Resolution / Clustering

Now comes the actual resolution step — turning all those scored pairs into clusters of entities.

We treat each “match” as a connection between two nodes in a graph:
- Each company_id is a node.
- Each match edge links two companies that the model believes are the same entity.

We then use a Union-Find (Disjoint Set) algorithm to find connected components — every connected subgraph becomes one resolved entity.

```sh
 uv run dg launch --job matching_job
```

>__er.entities__

| company_id | entity_id |
|-----------:|----------:|
| 1178881    | 7         |
| 6823024    | 7         |
| 1567113    | 38180     |
| 1559241    | 16567     |
| 3567617    | 36201     |


### Resolved Canonical Companies

After entity resolution has done its job — figuring out which company records represent the same real-world organization — we still need to decide what the final version of each company actually looks like.

At this stage, each entity_id in er.entities represents a cluster of records that have been matched together.

For example, three records that all point to the same entity might look like this:

| company_id  | name                        | domain        | city        | country |
|-------------|-----------------------------|---------------|-------------|----------|
| 1178881     | IBM                         | ibm.com       | New York    | USA      |
| 6823024     | International Business Mach | ibm.co.uk     | London      | UK       |
| 1567113     | IBM Corp.                   | ibm.com       | Armonk      | USA      |

Entity resolution tells us these (likely) belong together — but it doesn’t decide which name, domain, or location is the "truth".

Canonicalization takes all the clustered duplicates and produces one clean, standardized “gold record” for each entity.
That record becomes the official version you’ll use downstream for analytics, deduped exports, or CRM enrichment.  The goal is to strike a balance between precision and completeness — each canonical record should be representative of its underlying group, not just the first one that happened to match.

