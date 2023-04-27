from pathlib import Path
from typing import Coroutine

import toml
import uvicorn

from .app import create_app, ApiConfig


class Server:
    def __init__(self, config_path: str):
        try:
            self.config = toml.load(Path(config_path).open('r'))
        except Exception as e:
            raise (e)

        _app = create_app(ApiConfig(**self.config['xxdb']))
        _uvicorn_config = uvicorn.Config(_app, **self.config['server'])
        self._server = uvicorn.Server(_uvicorn_config)

    def serve(self) -> Coroutine:
        return self._server.serve()
