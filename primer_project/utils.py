import json
import logging
import os
import re
from collections import defaultdict
from sqlite3 import Cursor
from typing import Any, List, Union, get_type_hints, Dict

from primer_project.consts import PYTHON_TYPE_TO_SQL_TYPE
from primer_project.types import TableColumnDefinition, TableDefinition, TableRow, WalBatch, WalRecordInsert


logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))


def merge_wal_record_to_schema(schema: Dict[str, TableDefinition], record: WalRecordInsert) -> Dict[str, TableDefinition]:
    """
    Merges the schema of input WalRecordInsert into the schema accumulator.
    Returns a modified schema accumulator.
    """
    table_new = TableDefinition.from_record(record)
    table_old = schema.get(table_new.get_key())
    
    # Return the input schema if the specific table is already a part of it
    # (Not merging columns here)
    if table_old:
        return schema

    # Return a new schema with added new table definition
    return {
        **schema,
        table_new.get_key(): table_new,
    }


def sql_check_entity_name(entity_name: str) -> None:
    """
    Allow only some specific table/column names.
    """
    if not re.match("^[a-zA-Z0-9_]+$", entity_name):
        raise ValueError(f"Invalid SQL entity name: {entity_name}")    


def ddl_create_table(table: TableDefinition) -> str:
    """
    Constructs a DDL for the input TableDefinition.

    Manages some simple prevention of SQL inection.
    Could be switched to SQLAlchemy bound parameters in order to make it more robust.
    """
    sql_check_entity_name(table.table_name)
    for column in table.columns:
        sql_check_entity_name(column.name)

    sql_columns = ",".join([f"\"{c.name}\" \"{c.type}\"" for c in table.columns])
    return f"CREATE TABLE \"{table.table_name}\" ({sql_columns})"


def derive_table_definition(cls: type) -> TableDefinition:
    """
    Derives TableDefinition from an input type (class).
    """
    return TableDefinition(
        schema_name="public",
        table_name=cls.__name__.lower(),
        columns=[
            TableColumnDefinition(name=k, type=PYTHON_TYPE_TO_SQL_TYPE[v.__name__])
            for k, v
            in get_type_hints(cls).items()
        ],
    )


def get_wal_increment(wal_path: str) -> List[WalRecordInsert]:
    """
    Reads, parses and returns the WAL increment.
    """
    with open(wal_path, "r") as f:
        wal_batches: List[WalBatch] = json.loads(f.read())
        return [
            WalRecordInsert(**record)
            for batch in wal_batches for record in batch["change"]
        ]


def group_records_by_table(records: WalRecordInsert) -> Dict[str, List[TableRow]]:
    """
    Groups all input records by table.
    Transform the WalRecordInsert insteances to TableRow instances.
    """
    # Accumulator
    rows_by_table: Dict[str, List[TableRow]] = defaultdict(list)

    # Process record by record
    for record in records:
        rows_by_table[
            TableDefinition.from_record(record).get_key()
        ].append(TableRow.from_record(record))

    # Returns accumulated collection
    return rows_by_table


def table_exists(cursor: Cursor, table: TableDefinition) -> bool:
    """
    Returns True in case the input table already exists in the database.
    """
    return (
        len(
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                [table.table_name]
            ).fetchall()
        ) > 0
    )


def create_table(cursor: Cursor, table: TableDefinition) -> None:
    """
    Creates table in the database according to the input TableDefinition.
    """
    if table_exists(cursor, table):
        logger.warn(f"Table {table.table_name} already exists.")
        return

    logger.info(f"Creating table {table.table_name}")
    cursor.execute(ddl_create_table(table))


def fill_table(cursor: Cursor, table: TableDefinition, rows: List[Union[Dict, TableRow]]) -> None:
    """
    Inserts `rows` into the database table specified in the `table` input argument.
    """
    logger.info("Filling table %s", table.table_name)
    columns = ", ".join([c.name for c in table.columns])
    values_parameters = ", ".join([":" + c.name for c in table.columns])

    sql = f"INSERT INTO \"{table.table_name}\" ({columns}) VALUES ({values_parameters})"
    for row in rows:
        cursor.execute(sql, row.data if isinstance(row, TableRow) else row)
