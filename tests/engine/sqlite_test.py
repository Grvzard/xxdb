from xxdb.engine.index import getIndex, IndexConfig


def test(tmp_path):
    idx = getIndex("test", tmp_path, IndexConfig(**{"typ": "sqlite", "key_size": 8, "value_size": 8}))

    idx[1] = 1
    assert idx[1] == 1
    assert idx[2] is None
    assert 1 in idx
