from pathlib import Path
from typing import Coroutine
from functools import partial

import toml
import uvicorn
import hypercorn
from hypercorn.asyncio import serve

from .app import create_app, AppConfig


class Server:
    def __init__(self, config_path: str):
        self.config = toml.load(Path(config_path).open('r'))
        _app = create_app(AppConfig(**self.config['app']))

        self.config['server'].setdefault('host', "0.0.0.0")
        self.config['server'].setdefault('port', 7791)

        server_type = self.config['server'].pop('type', "uvicorn")

        if server_type == 'uvicorn':
            _config = uvicorn.Config(_app, **self.config['server'])
            self._serve = uvicorn.Server(_config).serve

        elif server_type == 'hypercorn':
            _config = hypercorn.Config.from_mapping(self.config['server'])
            _config.bind = [f"{_config.host}:{_config.port}"]
            self._serve = partial(serve, _app, _config)

        else:
            raise Exception(f"unknown server type: {server_type}")

        self.debug = self.config['server'].get('reload', False)

    def serve(self) -> Coroutine:
        # return self._server.serve()
        return self._serve()
