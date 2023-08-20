import dataclasses
import functools
import logging
import os
import sqlite3
from primer_project.consts import CONN_OUTPUT_DATABASE
from primer_project.transformers.commonmetric import extract_common_metrics

from primer_project.utils import create_table, derive_table_definition, fill_table, get_wal_increment, group_records_by_table, merge_wal_record_to_schema


logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))


if __name__ == "__main__":
    # Get WAL records
    logger.info("Getting records from WAL")
    records = get_wal_increment("./wal.json")

    # Generate schema of all tables
    logger.info("Generating database schema from WAL")
    schema = functools.reduce(merge_wal_record_to_schema, records, {})

    # Group records by table
    logger.info("Grouping WAL records by table")
    rows_by_table = group_records_by_table(records)

    # Temporary database for intermediate transformations (stored in memory)
    with sqlite3.connect(":memory:") as db:
        cur = db.cursor()

        # Create all temporary tables (and fill as well)
        logger.info("Filling temporary tables")
        for table in schema.values():
            create_table(cur, table)
            fill_table(cur, table, rows_by_table[table.get_key()])

        # Extract CommonMetric(s)
        logger.info("Generating common metrics")
        metrics = extract_common_metrics(cur)

    # Data check
    if not metrics:
        raise ValueError("Nothing to write!")

    # Connect to output database
    with sqlite3.connect(CONN_OUTPUT_DATABASE) as db:
        cur = db.cursor()

        # Build final table
        table = derive_table_definition(metrics[0].__class__)
        rows = list(map(dataclasses.asdict, metrics))
        create_table(cur, table)
        fill_table(cur, table, rows)

        # Commit changes
        db.commit()

    logger.info("New metrics loaded to output database: %s", CONN_OUTPUT_DATABASE)
