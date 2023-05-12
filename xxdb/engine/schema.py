# Referenced in: engine.DB, http.ws_client
from pb_encoding import getEncoder, getDecoder

from xxdb.engine.config import SchemaConfig as SchemaConfig
from xxdb.engine.config import ColumnConfig

__all__ = ("Schema", "SchemaConfig")


class Schema:
    def __init__(self, schema: SchemaConfig) -> None:
        self._columns: list[ColumnConfig] = sorted(schema.columns, key=lambda col: col.num)

    def unpack(self, data_bytes: bytes) -> dict:
        pos = 0
        data = {}
        for col in self._columns:
            result, pos = getDecoder(col.typ)(data_bytes, pos)
            data[col.name] = result

        return data

    def pack(self, raw_data: dict) -> bytes:
        data = bytearray()
        for col in self._columns:
            getEncoder(col.typ)(data.__iadd__, raw_data[col.name])

        return bytes(data)
