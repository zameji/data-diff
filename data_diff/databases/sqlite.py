from typing import Any, Dict, List

import attrs

from data_diff.abcs.database_types import (
    ColType,
    DbPath,
    DbTime,
    Float,
    Decimal,
    Integer,
    Text,
    TemporalType,
    FractionalType,
    UnknownColType,
)
from data_diff.databases.base import (
    ThreadedDatabase,
    import_helper,
    BaseDialect,
)
from data_diff.databases.base import (
    TIMESTAMP_PRECISION_POS,
)


@import_helper("sqlite")
def import_sqlite3():
    import sqlite3

    return sqlite3


@attrs.define(frozen=False)
class Dialect(BaseDialect):
    name = "MySQL"
    ROUNDS_ON_PREC_LOSS = False
    SUPPORTS_PRIMARY_KEY = True
    SUPPORTS_INDEXES = True
    TYPE_CLASSES = {
        "INT": Integer,
        "INTEGER": Integer,
        "TINYINT": Integer,
        "SMALLINT": Integer,
        "MEDIUMINT": Integer,
        "BIGINT": Integer,
        "UNSIGNED BIG INT": Integer,
        "INT1": Integer,
        "INT2": Integer,
        "INT3": Integer,
        "INT4": Integer,
        "INT6": Integer,
        "INT8": Integer,
        "CHARACTER": Text,
        "VARCHAR": Text,
        "VARYING CHARACTER": Text,
        "NCHAR": Text,
        "NATIVE CHARACTER": Text,
        "NVARCHAR": Text,
        "TEXT": Text,
        "CLOB": Text,
        "REAL": Float,
        "DOUBLE": Float,
        "DOUBLE PRECISION": Float,
        "FLOAT": Float,
        "NUMERIC": Float,
        "DECIMAL": Decimal,
        "BOOLEAN": Integer,
        # Unsupported types
        "DATE": UnknownColType,
        "DATETIME": UnknownColType,
        "NULL": UnknownColType,
        "BLOB": UnknownColType,
    }

    def concat(self, items: List[str]) -> str:
        "Provide SQL for concatenating a bunch of columns into a string"
        assert len(items) > 1
        return " || ".join(items)

    def to_comparable(self, value: str, coltype: ColType) -> str:
        """Ensure that the expression is comparable in ``IS DISTINCT FROM``."""
        return value

    def is_distinct_from(self, a: str, b: str) -> str:
        "Provide SQL for a comparison where NULL = NULL is true"
        return f"{a} is not {b}"

    def timestamp_value(self, t: DbTime) -> str:
        "Provide SQL for the given timestamp value"
        return f"'{t.isoformat()}'"

    def random(self) -> str:
        "Provide SQL for generating a random number betweein 0..1"
        return "random()"

    def current_timestamp(self) -> str:
        "Provide SQL for returning the current timestamp, aka now"
        return "current_timestamp()"

    def current_database(self) -> str:
        "Provide SQL for returning the current default database."
        return "current_database()"

    def current_schema(self) -> str:
        "Provide SQL for returning the current default schema."
        return "current_schema()"

    def explain_as_text(self, query: str) -> str:
        "Provide SQL for explaining a query, returned as table(varchar)"
        return f"EXPLAIN QUERY PLAN {query}"

    def quote(self, s: str):
        "Quote SQL name"
        return f'"{s}"'

    def to_string(self, s: str) -> str:
        "Provide SQL for casting a column to string"
        return f"cast({s} as text)"

    def set_timezone_to_utc(self) -> str:
        "Provide SQL for setting the session timezone to UTC"
        return ""

    def md5_as_int(self, s: str) -> str:
        "Provide SQL for computing md5 and returning an int"
        return f"data_diff__md5_int({s})"

    def md5_as_hex(self, s: str) -> str:
        "Provide SQL for computing md5 and returning an int"
        return f"data_diff__md5_hex({s})"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized timestamp.

        The returned expression must accept any SQL datetime/timestamp, and return a string.

        Date format: ``YYYY-MM-DD HH:mm:SS.FFFFFF``

        Precision of dates should be rounded up/down according to coltype.rounds
        """
        if coltype.rounds:
            return f"substr(strftime({value}, '%Y-%m-%d %H:%M:%f'), 1, {TIMESTAMP_PRECISION_POS+coltype.precision})"

        return f"strftime({value}, '%Y-%m-%d %H:%M:%f')"

    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized number.

        The returned expression must accept any SQL int/numeric/float, and return a string.

        Floats/Decimals are expected in the format
        "I.P"

        Where I is the integer part of the number (as many digits as necessary),
        and must be at least one digit (0).
        P is the fractional digits, the amount of which is specified with
        coltype.precision. Trailing zeroes may be necessary.
        If P is 0, the dot is omitted.

        Note: We use 'precision' differently than most databases. For decimals,
        it's the same as ``numeric_scale``, and for floats, who use binary precision,
        it can be calculated as ``log10(2**numeric_precision)``.
        """
        return f"format('%.{coltype.precision}f', {value})"


@attrs.define(frozen=False, init=False, kw_only=True)
class SQLite(ThreadedDatabase):
    dialect = Dialect()
    SUPPORTS_ALPHANUMS = False
    SUPPORTS_UNIQUE_CONSTAINT = True
    CONNECT_URI_HELP = "sqlite://db_path"
    CONNECT_URI_PARAMS = []

    _args: Dict[str, Any]

    def __init__(self, *, thread_count, **kw):
        super().__init__(thread_count=thread_count)
        self._args = kw
        # In sqlite schema and database are synonymous
        self.default_schema = ""

    def create_connection(self):
        sqlite3 = import_sqlite3()

        import hashlib

        def data_diff__md5_int(t):
            return int(hashlib.md5(t).hexdigest(), base=16)

        def data_diff__md5_hex(t):
            return hashlib.md5(t).hexdigest()

        print(self._args)
        con = sqlite3.connect(**self._args)
        con.create_function("data_diff__md5_int", 1, data_diff__md5_int)
        con.create_function("data_diff__md5_hex", 1, data_diff__md5_hex)
        return con

    def close(self):
        sqlite3 = import_sqlite3()
        con = sqlite3.connect(**self._args)
        con.create_function("data_diff__md5_int", 1, None)
        con.create_function("data_diff__md5_hex", 1, None)

        super().close()

    def select_table_schema(self, path: DbPath) -> str:
        schema, table = self._normalize_table_path(path)

        return f"SELECT name as column_name, type as data_type, 3 as datetime_precision, 15 as numeric_precision, NULL as numeric_scale FROM pragma_table_info('{table}')"
