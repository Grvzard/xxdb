from xxdb.engine.schema import Schema, SchemasConfig


def test():
    data_schemas = [
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
    ]

    schema = Schema(SchemasConfig.parse_obj(data_schemas))

    doc1_in = {"ts": 1, "uid": 1, "text": "hello", "other_field": "other_value"}
    data = schema.pack(doc1_in, "test_schema1")
    doc1_out = {"ts": 1, "uid": 1, "text": "hello", "schema_": "test_schema1"}
    assert schema.unpack(data) == doc1_out
