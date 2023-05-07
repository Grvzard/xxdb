import asyncio

import typer

from .server import Server


cli = typer.Typer()


@cli.command()
def main(config_path: str = 'xxdb.toml'):
    server = Server(config_path)
    asyncio.run(server.serve())
