from pathlib import Path
from typing import Coroutine

import toml
import uvicorn

from .app import create_app, AppConfig


class Server:
    def __init__(self, config_path: str):
        self.config = toml.load(Path(config_path).open('r'))
        _app = create_app(AppConfig(**self.config['app']))
        if "host" not in self.config['server']:
            self.config['server']['host'] = "0.0.0.0"
        _uvicorn_config = uvicorn.Config(_app, **self.config['server'])
        self._server = uvicorn.Server(_uvicorn_config)

    def serve(self) -> Coroutine:
        return self._server.serve()
