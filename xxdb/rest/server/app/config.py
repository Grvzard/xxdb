from pydantic import BaseModel


class DbSettings(BaseModel):
    path: str
    name: str
    settings: dict = {}
    # route: str


class ApiConfig(BaseModel):
    endpoints: list[DbSettings]
