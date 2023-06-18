import asyncio
from typing import Optional

import typer

from xxdb.http.server import Server
from xxdb.engine.db import create as db_create, DbMeta


cli = typer.Typer()


@cli.command()
def serve(config: str = 'xxdb.toml'):
    server = Server(config)
    if server.debug:
        import tracemalloc

        def print_tracemalloc():
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics("lineno")

            with open("tracemalloc.txt", "w") as fp:
                fp.write("[ Top 30 ]\n")
                for stat in top_stats[:30]:
                    fp.write(str(stat) + "\n")

        tracemalloc.start()

        import cProfile

        pr = cProfile.Profile()
        pr.enable()

        asyncio.run(server.serve())

        print_tracemalloc()
        pr.disable()
        pr.dump_stats('xxdb.prof')
    else:
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
