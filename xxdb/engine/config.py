from typing import Optional

from pydantic import BaseModel


class DiskSettings(BaseModel):
    page_size: int = 2048
    comment: str = ''
    index_format: str = '<QL'
    data_schema: Optional[dict] = None

    class Config:
        case_sensitive = True


class BufferPoolSettings(BaseModel):
    max_page_num: int = 100000
    replacer: str = "fifo"


class PrometheusSettings(BaseModel):
    ...


class InstanceSettings(BaseModel):
    buffer_pool: BufferPoolSettings = BufferPoolSettings()
    prometheus: PrometheusSettings = PrometheusSettings()
