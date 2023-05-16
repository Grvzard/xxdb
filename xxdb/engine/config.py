from typing import Literal

from pydantic import BaseModel
from pb_encoding import SupportedType as ColumnType


class PageConfig(BaseModel):
    size_cost: int = 4  # do not change this
    id_cost: int = 0  # unused


class DiskConfig(BaseModel):
    page_size: int = 2048
    page: PageConfig = PageConfig()


class IndexSettings(BaseModel):
    typ: Literal['hashtable'] = 'hashtable'
    key_size: Literal[4, 8] = 8


class _ColumnConfig(BaseModel):
    name: str
    typ: ColumnType
    num: int


class _SchemaConfig(BaseModel):
    columns: list[_ColumnConfig]
    name: str  # there is a reserved column named "schema_"
    code: int


class SchemasConfig(BaseModel):
    __root__: list[_SchemaConfig]

    def __iter__(self):
        return iter(self.__root__)


class DbMeta(BaseModel):
    disk: DiskConfig = DiskConfig()
    index: IndexSettings = IndexSettings()
    comment: str = ''
    schemas: None | SchemasConfig = None


class BufferPoolConfig(BaseModel):
    max_size: int = 128 * 1024 * 1024
    replacer: str = "fifo"


class PrometheusSettings(BaseModel):
    enable: bool = True


class InstanceSettings(BaseModel):
    with_schema: bool = True
    buffer_pool: BufferPoolConfig = BufferPoolConfig()
    prometheus: PrometheusSettings = PrometheusSettings()
