import asyncio

import typer

from .server import Server


cli = typer.Typer()


@cli.command()
def main(config: str = 'xxdb.toml'):
    server = Server(config)
    asyncio.run(server.serve())
