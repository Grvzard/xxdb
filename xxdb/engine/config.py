from typing import Literal

from pydantic import BaseModel
from pb_encoding import SupportedType


class DiskSettings(BaseModel):
    page_size: int = 2048
    index_key_size: Literal[4, 8] = 8


class DbColumn(BaseModel):
    num: int
    name: str
    type: SupportedType


class DbSchema(BaseModel):
    __root__: list[DbColumn]

    def __iter__(self):
        return iter(self.__root__)


class DbMeta(BaseModel):
    disk: DiskSettings = DiskSettings()
    comment: str = ''
    data_schema: None | DbSchema = None


class BufferPoolSettings(BaseModel):
    max_size: int = 128 * 1024 * 1024
    replacer: str = "fifo"


class PrometheusSettings(BaseModel):
    enable: bool = True


class InstanceSettings(BaseModel):
    with_schema: bool = True
    buffer_pool: BufferPoolSettings = BufferPoolSettings()
    prometheus: PrometheusSettings = PrometheusSettings()
