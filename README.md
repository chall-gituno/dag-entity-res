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

1. Ingest & Clean (bronze → silver)
2. Blocking & Pairing (multi-strategy block generation, adaptive block capping)
3. Feature Engineering (numerical + categorical similarity metrics)
4. Weakly Labeled Model Training (scikit-learn pipeline, persisted with joblib)
5. Scoring (parallel or streaming inference → pair_scores)
6. Entity Resolution / Clustering (union-find → er.entities)
7. Sanity Checks + Canonicalization (detect mega-clusters, derive gold record views)

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
uv run dg launch --job blocking_job
```

If you have AI enabled, you'll see it suggests we do some more work on our blocking...we'll put that on the future refinements TODO and move on.

### Pairing

### Feature Extraction (SQL)

### Feature Extraction (Python)

