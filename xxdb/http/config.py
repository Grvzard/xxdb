from pydantic import BaseModel

from xxdb.engine.config import InstanceSettings


class DbSettings(BaseModel):
    path: str
    name: str
    cfg: str = ''
    settings: InstanceSettings = InstanceSettings()
    flush_period: int = 5  # in seconds


class AppConfig(BaseModel):
    databases: list[DbSettings]
    cors_origins: list[str] = ["*"]
    allowed_hosts: list[str] = ["*"]
    auto_create: bool = True
