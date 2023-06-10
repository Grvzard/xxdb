# Referenced in: engine.DB, http.ws_client
from collections import namedtuple

from pb_encoding import getEncoder, getDecoder

from xxdb.engine.config import SchemasConfig as SchemasConfig

__all__ = ("Schema", "SchemasConfig")


SchemaColumn = namedtuple("SchemaColumn", ["name", "encode", "decode"])


# TODO: add validation for schemas
class Schema:
    def __init__(self, schemas: SchemasConfig) -> None:
        # make sure no duplicate schema code
        _validate = len(schemas) == len({s.code for s in schemas})
        if _validate is False:
            raise Exception("duplicate schema code")
        self._schema_code_map = {s.name: s.code for s in schemas}
        self._schema_name_map = {s.code: s.name for s in schemas}
        self._cols: dict[int, list[SchemaColumn]] = {
            s.code: [
                SchemaColumn(col.name, getEncoder(col.typ), getDecoder(col.typ))
                for col in sorted(s.columns, key=lambda col: col.num)
            ]
            for s in schemas
        }

    def unpack(self, data_bytes: bytes) -> dict:
        schema_code = data_bytes[0]
        pos = 1  # skip schema code
        data = {}

        try:
            for schema_col in self._cols[schema_code]:
                result, pos = schema_col.decode(data_bytes, pos)
                data[schema_col.name] = result
            data["schema_"] = self._schema_name_map[schema_code]
        except KeyError:
            raise Exception(f"skip unknown schema code: {schema_code}")

        return data

    def pack(self, raw_data: dict, schema: str) -> bytes:
        try:
            schema_code = self._schema_code_map[schema]
        except KeyError:
            raise Exception(f"schema {schema} not found")

        data = bytearray()
        data += schema_code.to_bytes(1, "little")

        for schema_col in self._cols[schema_code]:
            try:
                schema_col.encode(data.__iadd__, raw_data[schema_col.name])
            except KeyError:
                raise Exception(f"column {schema_col.name} not found")

        return bytes(data)
