from typing import Literal, Optional, Any

from pydantic import BaseModel, FilePath
from pb_encoding import SupportedType as ColumnType

from xxdb import __version__


class DiskConfig(BaseModel):
    typ: Literal['singlefile', 'multifile'] = 'singlefile'
    # TODO：validate the page_size is a multiple of 512
    page_size: int = 2048
    key_size: Literal[4, 8] = 8

    pageid_size: int = 0  # will be auto filled based on typ

    # which will be passed to the disk class
    # multifile needs a block_size, sized in MB
    params: dict[str, Any] = {}

    def __init__(self, **data):
        super().__init__(**data)
        self.pageid_size = {
            'singlefile': 4,
            'multifile': 4,
        }[self.typ]


class IndexConfig(BaseModel):
    typ: Literal['sqlite', 'hashtable'] = 'hashtable'

    key_size: int = 0  # will be auto filled by DbMeta
    value_size: int = 0  # will be auto filled by DbMeta

    # which will be passed to the index class
    params: dict[str, Any] = {}


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

    def __len__(self):
        return len(self.__root__)


class DbMeta(BaseModel):
    version_: str = __version__
    disk: DiskConfig = DiskConfig()
    index: IndexConfig = IndexConfig()
    comment: str = ''
    schemas: Optional[SchemasConfig] = None

    def __init__(self, **data):
        super().__init__(**data)
        self.index.key_size = self.disk.key_size
        self.index.value_size = self.disk.pageid_size


class BufferPoolConfig(BaseModel):
    max_pages: int = 300_000
    replacer: str = "fifo"


# class SingleFileDiskConfig(BaseModel):
#     typ: Literal['singlefile'] = 'singlefile'
#     datadir: FilePath = FilePath('.')


# class MultiFileDiskConfig(BaseModel):
#     typ: Literal['multifile'] = 'multifile'
#     datadir: str


# class DiskConfig(BaseModel):
#     typ: Literal['singlefile', 'multifile'] = 'singlefile'
#     config: Union[SingleFileDiskConfig, MultiFileDiskConfig] = Field(SingleFileDiskConfig(), discriminator='typ')


class PrometheusSettings(BaseModel):
    enable: bool = True


class InstanceSettings(BaseModel):
    with_schema: bool = True
    meta_path: Optional[FilePath] = None
    buffer_pool: BufferPoolConfig = BufferPoolConfig()
    prometheus: PrometheusSettings = PrometheusSettings()
