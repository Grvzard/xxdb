from pydantic import BaseModel

from xxdb.engine.config import InstanceSettings


class DbSettings(BaseModel):
    path: str
    name: str
    settings: InstanceSettings = InstanceSettings()
    flush_period: int = 5  # in seconds


class AppConfig(BaseModel):
    databases: list[DbSettings]
    cors_origins: list[str] = []