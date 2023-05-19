from xxdb.engine.hashtable import HashTable


def test_ht(tmp_path):
    idx_path = tmp_path / "test.idx.xxdb"

    ht = HashTable(idx_path, 8, 4)
    ht[10] = 10
    assert ht[10] == 10
    assert ht[12] is None

    ht.close()

    ht = HashTable(idx_path, 8, 4)
    assert ht[10] == 10

    for _ in range(100, 100000):
        ht[_] = _

    assert ht[9999] == 9999
