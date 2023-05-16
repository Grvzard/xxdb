import asyncio
from typing import Optional

import typer

from xxdb.http.server import Server
from xxdb.engine.db import create as db_create, DbMeta


cli = typer.Typer()


@cli.command()
def serve(config: str = 'xxdb.toml'):
    server = Server(config)
    asyncio.run(server.serve())


@cli.command()
def create(
    name: str = typer.Argument(...),
    config: Optional[str] = typer.Option('', '--config', '-c'),
    datadir: Optional[str] = typer.Option('', '--datadir', '-d'),
):
    if not config:
        config = f'{name}.xxmeta.json'
    params = {
        "name": name,
        "meta": DbMeta.parse_file(config, encoding="utf-8"),
        "exists_ok": False,
    }
    if datadir:
        params["datadir"] = datadir
    try:
        db_create(**params)
    except Exception as e:
        typer.secho(str(e), fg='yellow')
    else:
        typer.secho("db created", fg='green')
