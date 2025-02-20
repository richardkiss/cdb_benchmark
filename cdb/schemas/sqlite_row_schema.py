import pathlib

from cdb.schemas.hash_db_schema import BaseDBSchema


from cdb.hashdb.sqlite3_row_array_storage import SQLite3RowStorage


PATH = pathlib.Path("./sqlite_row_schema_db")
if PATH.exists():
    raise FileExistsError(f"{PATH} already exists")

REPLAY = BaseDBSchema(PATH, SQLite3RowStorage)
