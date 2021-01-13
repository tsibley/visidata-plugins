import visidata
import dbm

__version__ = '2020.09.18'


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
            self.rows.append((key, self.db[key]))


visidata.addGlobals({
    "open_dbm": open_dbm,
    "DBMSheet": DBMSheet,
})
