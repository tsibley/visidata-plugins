import visidata
import dbm
import os
from functools import partial

__version__ = '2020.09.18'


DBM_ENCODING = os.environ.get("DBM_ENCODING", "utf-8")

decode = partial(bytes.decode, encoding = DBM_ENCODING, errors = "replace")


def open_dbm(path):
    return DBMSheet(path.name, source=path)


class DBMSheet(visidata.Sheet):
    rowtype = "keys" # rowdef: (key, value) tuple

    columns = [
        visidata.ColumnItem("key", 0, type=str),
        visidata.ColumnItem("value", 1, type=str),
    ]

    def __init__(self, name, source):
        super().__init__(name=name, source=source)
        self.db = dbm.open(str(self.source), "c")

    def reload(self):
        self.rows = []

        for key in self.db.keys():
            k, v = key, self.db[key]

            if isinstance(k, bytes):
                k = decode(k)

            if isinstance(v, bytes):
                v = decode(v)

            self.rows.append((k, v))


visidata.addGlobals({
    "open_dbm": open_dbm,
    "DBMSheet": DBMSheet,
})
