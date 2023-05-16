import json

from xxdb.engine import DbMeta


def test():
    meta_obj = {
        "disk": {"page_size": 1024},
        "index": {"typ": "hashtable", "key_size": 8},
        "comment": "test",
        "schemas": [
            {
                "name": "test_schema1",
                "code": 1,
                "columns": [
                    {"name": "ts", "num": 0, "typ": "fixed32"},
                    {"name": "uid", "num": 1, "typ": "uint64"},
                    {"name": "text", "num": 2, "typ": "string"},
                ],
            },
            {
                "name": "test_schema2",
                "code": 2,
                "columns": [
                    {"name": "ts", "num": 0, "typ": "fixed32"},
                    {"name": "uid", "num": 1, "typ": "uint64"},
                    {"name": "text", "num": 2, "typ": "string"},
                ],
            },
        ],
    }

    DbMeta.parse_raw(json.dumps(meta_obj).encode())
