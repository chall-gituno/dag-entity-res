# Entity Resolution

The ongoing and nagging problem of all data systems - Is this thing the same as this other thing from this other dataset?

This project is an example of one approach (probably most common) to answering the above question.

## Overview

The overall approach is to:

1. ingest raw data
2. clean it
3. block it (several stages were used here to try to keep the block sizes usable)
4. build pairs from our blocks
5. do initial feature extraction (in sql)
6. do deeper feature extraction (in python)
7. build the model

The dataset used in this experiment was deliberately large(ish) ~7M records.  The reason being that every example you see anywhere on the internet will use tiny toy-sized datasets that fit easily into memory and can be handled using pandas. But that will not likely ever be the case in reality.  So part of the goal of this project is to demonstrate how things would need to be done in the real world.

## Technology

We are using Duckdb as our working database and Dagster as our orchestration platform.  For the most part, processing is handled via SQL so we can offload heavy lifting to the database (In a real application the db would probably be something like Snowflake but we work with what we have). We have built a small templating system that uses jinja for our SQL scripts.  In heavier system we could use `dbt` and migration to it shouldn't be too painful.

## Processing

### Ingestion/Cleaning

Nothing out of the ordinary here.

### Blocking

In smaller datasets you could probably block and pair in a single step (query). But that will cause OOM errors if you try it with our dataset and DuckDB defaults (assuming you are running on a fairly average dev machine). So we run blocking and pairing as separate processes.  Furthermore, blocking is done in multiple stages in an attempt to arrive at our optimal block sizes. (TODO: explain / illustrate what is a 'bad' vs 'good' block size and what leads to it)

### Pairing

### Feature Extraction (SQL)

### Feature Extraction (Python)

