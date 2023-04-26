import asyncio
import uvicorn
import typer
from pathlib import Path
import toml

from .app import create_app, ApiConfig


cli = typer.Typer()


@cli.command()
def main(config_path: str = 'xxdb.toml'):
    ...
    config = toml.load(Path(config_path).open('r'))
    _app = create_app(ApiConfig(**config['xxdb']))
    _uvicorn_config = uvicorn.Config(_app, **config['server'])
    server = uvicorn.Server(_uvicorn_config)
    asyncio.run(server.serve())
