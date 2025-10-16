# Entity Resolution

The ongoing and nagging problem of many data systems...
"Is this thing (company, customer, widget) the same as this other thing from this other dataset?"

This project is an example of one approach to answering the above question using a logistic regression model.  This is more about how to approach the problem rather than providing a definitive solution.  The data we use for this experiment is a companies dataset from from kaggle. see the [companies](src/resolver/defs/assets/companies.py) asset for more info.  It only has a handful of fields so the features we can get from it are limited.

Like most things in github, consider this a WIP.  I'll try to keep it up to date as time permits.

## Overview

The broad strokes...

1. Ingest & Clean (bronze → silver)
2. Blocking & Pairing (multi-strategy block generation, adaptive block capping)
3. Feature Engineering (numerical + categorical similarity metrics)
4. Weakly Labeled Model Training (scikit-learn pipeline, persisted with joblib)
5. Scoring (parallel or streaming inference → pair_scores)
6. Entity Resolution / Clustering (union-find → er.entities)
7. Sanity Checks + Canonicalization (detect mega-clusters, derive gold record views)

### Technology

We are using Duckdb as our working database and Dagster as our orchestration platform.  For the most part, processing is handled via SQL so we can offload heavy lifting to the database. There is a primitive templating system that uses jinja for some of our more complex SQL scripts...sort of a rudimentary `dbt`.

> NOTE: This is NOT a tutorial on how to use [Dagster](https://docs.dagster.io) but check out their docs - they are good.  So is the docs AI assistant.

### Getting Started

**Step Number One** - [install uv](https://docs.astral.sh/uv/getting-started/installation)

Then you can sync your virtual env and sort your dependencies...

```sh
uv sync
```

> You'll also probably want to install duckdb using `brew` or similar if you want to poke around in the raw data.  This project uses a medallion system (albeit somewhat loosely) so if you are poking around in the data, the main schemas are `bronze` for raw, `silver` for cleaned and (eventually) `gold` for our final outputs.  We also have an `er` schema for entity resolution work tables.

Next, copy the dotenv.example to .env and adjust as needed.  Most importantly, you need to point it at your companies data file.

Now you can do a quick sanity check

```sh
uv run dg 
# or if you want to start the dev instance
uv run dg dev 
```

## Processing

It might be helpful to see what we have in our orchestration definitions...

```sh
uv run dg list defs
```

If you have the UI running, you can click on the hamburger in the upper left to see a good overview.  Or you can click on the `View Lineage` link from the `Assets` page to get the big picture.

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

In smaller datasets you could probably block and pair in a single step (query). But that might cause issues if you try it with our dataset and DuckDB defaults (assuming you are running on a fairly average dev machine). Also, we want to be able to tweak our blocking strategy independant of producing pairs.  So we run blocking and pairing as separate processes.  Blocking is done in multiple stages in an attempt to arrive at our optimal block sizes.

But what exactly _is_ blocking and why do we need an optimal size?  If you are a data engineer, you would probably say that the goal of blocking is to "maximize recall while minimizing the number of comparisons".  If you are NOT a data engineer that may sound like one of those things you can _almost_ get a handle on but is still just abstract enough to keep slipping away.  

So to clear it up, we'll say instead that we need blocking because of the O(n²) problem.  Simply put, that means that without blocking, we have an order of growth that is quadratic...meaning every record (`n`) needs to be compared to **every other record**

```text
Comparisons = n x (n - 1) / 2

- Each record compares to (n − 1) others.
- Divide by 2 so we don’t count both A–to-B and B–to-A as separate comparisons.

n = 1,000     →  (1000 × 999) / 2  = 499,500
n = 10,000    →  (10000 × 9999) / 2 = 49,995,000
n = 1,000,000 →  (1000000 × 999999) / 2 ≈ 499,999,500,000
```

A **good** blocking strategy finds most of the real matches while keeping each block small enough to process comfortably - that is the "optimal size" we are going for.

A **bad** blocking strategy either:

- Creates a few giant blocks (too much work, memory blow-ups, cpu melts, etc), or
- Creates too many tiny, fragmented blocks (misses matches spread across them).


### Pairing

### Feature Extraction (SQL)

### Feature Extraction (Python)

