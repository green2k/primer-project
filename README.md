# Primer Project
This is an example implementation of the `Data Engineering Challenge` @ `primer-io`.
## Overview
The core of this project is to implement a standalone pipeline for:
- Reading input WAL (`wal.json` in our case)
- Calculating metrics over the input data
- Store the metrics in output database (SQLite database stored in `metrics.db` in our case)
## Installation
- Python ~3.11 is required to run this pipeline.
- No additional libraries are needed. No virtual environment like Pipenv or Poetry are not needed as well.

## Run pipeline
Script for pipeline execution: `run_pipeline.sh`

## Run tests
Script for executing tests: `run_tests.sh`
