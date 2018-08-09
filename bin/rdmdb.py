
import copy
import json
import os
from pathlib import Path
import sqlite3

ergastsqlite = os.path.join(
    os.path.dirname(__file__),
    "../ergast_f1db.sqlite3db"
)
uri = 'file:' + ergastsqlite + '?mode=ro'

db = sqlite3.connect(uri, uri=True)
db.isolation_level = None


def save_to_json(data, json_file):
    path = os.path.join(
        os.path.dirname(__file__),
        "../../racing-data-munging/data/ergast/",
         json_file
    )
#    print('saving ' + path)
    with open(path, 'w') as outfile:
        json.dump(data, outfile)


def exec_query(query, args=[], json_file=None):
    c = db.cursor()
    c.execute(query, args)
    names = [d[0] for d in c.description]
    rows = [dict(zip(names, row)) for row in c.fetchall()]

    if json_file:
        save_to_json(rows, json_file)
    return rows
