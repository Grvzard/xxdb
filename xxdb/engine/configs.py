from pydantic import BaseSettings, BaseModel

# import orjson


class DiskSettings(BaseModel):
    page_size: int = 2048
    comment: str = ''
    index_format: str = '<QL'

    class Config:
        case_sensitive = False
        # json_loads = orjson.loads
        # json_dumps = orjson.dumps


class BufferPoolSettings(BaseModel):
    max_page_num: int = 100000
    replacer: str = "fifo"


class Settings(BaseSettings):
    # disk: DiskSettings = DiskSettings()
    buffer_pool: BufferPoolSettings = BufferPoolSettings()

    class Config:
        # @classmethod
        # def customise_sources(
        #     cls,
        #     init_settings,
        #     env_settings,
        #     file_secret_settings,
        # ):
        #     _ = init_settings
        #     return env_settings, file_secret_settings

        case_sensitive = False
        env_nested_delimiter = '_'
        # env_file = '.env'
