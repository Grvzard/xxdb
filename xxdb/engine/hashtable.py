# Referenced by: DB


class HashTable:
    def __init__(self, keys):
        self.keys = keys
        self.ht = dict(self.keys)
        self.len = len(self.keys)
        # self.indices = _resize_indices()

    def _resize_indices(self):
        ...

    def __contains__(self, key):
        return key in self.ht

    def __getitem__(self, key):
        return self.ht[key]

    def __setitem__(self, key, value):
        assert key not in self.ht

        self.keys.append((key, value))
        self.ht[key] = value
        self.len += 1
