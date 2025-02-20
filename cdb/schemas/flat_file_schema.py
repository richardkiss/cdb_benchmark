import pathlib

from cdb.hashdb.flat_file_array_storage import FlatFileArrayStorage
from cdb.schemas.hash_db_schema import BaseDBSchema

PATH = pathlib.Path("./flat_file_schema_db")
if PATH.exists():
    raise FileExistsError(f"{PATH} already exists")

REPLAY = BaseDBSchema(PATH, FlatFileArrayStorage)
