# Entity Resolution

The ongoing and nagging problem of many data systems...
Is this thing (company, customer, widget) the same as this other thing from this other dataset?

This project is an example of one approach to answering the above question.

## Overview

The overall approach is to:

1. ingest raw data
2. clean it
3. block it (several stages were used here to try to keep the block sizes usable)
4. build pairs from our blocks
5. do initial feature extraction (in sql)
6. do deeper feature extraction (in python)
7. build the model
8. run our entities through the model

The dataset used in this experiment was a larger than a toy-sized one (~7M recs).  Reason being that part of the goal of this project is to demonstrate how things could be done in more realistic settings...that is to say where it probably doesn't make sense to try to load everything into a pandas dataframe.

## Technology

We are using Duckdb as our working database and Dagster as our orchestration platform.  For the most part, processing is handled via SQL so we can offload heavy lifting to the database (in a real application the db would probably be something like Snowflake but we work with what we have). We have built a small templating system that uses jinja for our SQL scripts.  In heavier system we'd proabably want to use `dbt`.

## Processing

### Ingestion/Cleaning

Nothing out of the ordinary here.

### Blocking

In smaller datasets you could probably block and pair in a single step (query). But that will cause OOM errors if you try it with our dataset and DuckDB defaults (assuming you are running on a fairly average dev machine). So we run blocking and pairing as separate processes.  Furthermore, blocking is done in multiple stages in an attempt to arrive at our optimal block sizes. (TODO: explain / illustrate what is a 'bad' vs 'good' block size and what leads to it)

### Pairing

### Feature Extraction (SQL)

### Feature Extraction (Python)

