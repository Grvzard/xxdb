from typing import Literal

from pydantic import BaseModel
from pb_encoding import SupportedType as ColumnType


class PageConfig(BaseModel):
    size_cost: int
    id_cost: int


class DiskConfig(BaseModel):
    page_size: int = 2048
    page: PageConfig = PageConfig(size_cost=4, id_cost=8)


class IndexSettings(BaseModel):
    typ: Literal['hashtable'] = 'hashtable'
    key_size: Literal[4, 8] = 8


class ColumnConfig(BaseModel):
    name: str
    typ: ColumnType
    num: int


class SchemaConfig(BaseModel):
    columns: list[ColumnConfig]


class DbMeta(BaseModel):
    disk: DiskConfig = DiskConfig()
    index: IndexSettings = IndexSettings()
    comment: str = ''
    data_schema: None | SchemaConfig = None


class BufferPoolConfig(BaseModel):
    max_size: int = 128 * 1024 * 1024
    replacer: str = "fifo"


class PrometheusSettings(BaseModel):
    enable: bool = True


class InstanceSettings(BaseModel):
    with_schema: bool = True
    buffer_pool: BufferPoolConfig = BufferPoolConfig()
    prometheus: PrometheusSettings = PrometheusSettings()
