from xxdb.engine.hashtable import HashTable
from xxdb.engine.config import IndexSettings


def test_ht(tmp_path):
    meta = IndexSettings(key_size=8)
    idx_path = tmp_path / "test.idx.xxdb"

    ht = HashTable(idx_path, meta)
    ht[10] = 10
    assert ht[10] == 10
    assert ht[12] is None

    ht.close()

    ht = HashTable(idx_path, meta)
    assert ht[10] == 10

    for _ in range(100, 100000):
        ht[_] = _

    assert ht[9999] == 9999
