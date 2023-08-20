from dataclasses import dataclass, field
from typing import Any, Dict, List, Self, TypedDict, Union

class WalBatch(TypedDict):
    """
    Schema of a single WAL batch in an input stream.
    """
    change: List[Dict[str, Any]]

@dataclass
class WalRecordInsert():
    """
    Schema of a single WAL INSERT record record (contained a WAL batch of records)
    """
    kind: str
    schema: str
    table: str
    columnnames: List[str]
    columntypes: List[str]
    columnvalues: List[Union[bool, float, int, str]]

    def __post_init__(self):
        # TODO: Check also datatypes
        if self.kind != "insert":
            raise ValueError(f"Only INSERT WAL records are supported (found: {self.kind})")

@dataclass
class TableRow():
    """
    A single row to be inserted to the database.
    """
    schema_name: str
    table_name: str
    data: Dict[str, Union[bool, float, int, str]]

    @staticmethod
    def from_record(record: WalRecordInsert) -> Self:
        """
        Constructs TableRow from a WalRecordInsert instance.
        """
        return TableRow(
            schema_name=record.schema,
            table_name=record.table,
            data={
                cname: cvalue
                for cname, cvalue
                in zip(record.columnnames, record.columnvalues)
            },
        )
        

@dataclass
class TableColumnDefinition():
    """
    Simple type definition of a table-column.
    """
    name: str
    type: str

@dataclass
class TableDefinition():
    """
    Table details definition, that includes:
        - Schema name
        - Table name
        - All columns including their type
    """
    schema_name: str
    table_name: str
    columns: Dict[str, TableColumnDefinition] = field(default_factory=list)

    def get_key(self) -> str:
        """
        Returns a deterministic key for this TableDefinition instance.
        """
        return f"{self.schema_name}.{self.table_name}"

    @staticmethod
    def from_record(record: WalRecordInsert) -> Self:
        """
        Constructs TableDefinition from WalRecordInsert
        """
        return TableDefinition(
            schema_name=record.schema,
            table_name=record.table,
            columns=[
                TableColumnDefinition(name=cname, type=ctype)
                for cname, ctype in zip(record.columnnames, record.columntypes)
            ],
        )
