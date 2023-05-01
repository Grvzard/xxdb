import json

from xxdb.engine import DbMeta


def test():
    meta_obj = {
        "disk": {"page_size": 1024, "index_key_size": 8},
        "comment": "test",
        "data_schema": [
            {"name": "ts", "num": 0, "type": "fixed32"},
            {"name": "uid", "num": 1, "type": "uint64"},
            {"name": "text", "num": 2, "type": "string"},
        ],
    }

    DbMeta.parse_raw(json.dumps(meta_obj).encode())
