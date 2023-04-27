from pydantic import BaseModel


class DbSettings(BaseModel):
    path: str
    name: str
    settings: dict = {}
    flush_period: int = 5  # in seconds
    # route: str


class ApiConfig(BaseModel):
    endpoints: list[DbSettings]
