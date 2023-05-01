from pb_encoding import getEncoder, getDecoder

from xxdb.engine.config import DbSchema


class Schema:
    def __init__(self, schema: DbSchema) -> None:
        self._columns = sorted(schema, key=lambda col: col.num)

    def unpack(self, data_bytes: bytes) -> dict:
        pos = 0
        data = {}
        for col in self._columns:
            result, pos = getDecoder(col.type)(data_bytes, pos)
            data[col.name] = result

        return data

    def pack(self, raw_data: dict) -> bytes:
        data = bytearray()
        for col in self._columns:
            getEncoder(col.type)(data.__iadd__, raw_data[col.name])

        return data
