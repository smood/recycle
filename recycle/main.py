import logging
from pathlib import Path
from typing import Optional

import dataconf
from furl import furl
from recycle import config
from recycle.providers import gcs
from recycle.providers import mongo as mongodb
from recycle.providers import postgres as postgresql
from recycle.utils import shell
from typer import echo
from typer import Exit
from typer import Option
from typer import Typer

cli = Typer()


@cli.command()
def mongo(
    source: str,
    sink: str,
    transform: Optional[str] = Option(None),
    collection: Optional[str] = Option(None),
    jobs: int = Option(4),
    keep_previous: bool = Option(False),
    json: bool = Option(False),
    verbose: bool = Option(False),
):
    try:
        source_uri = furl(source)
        sink_uri = furl(sink)
        source_folder = str(source_uri.path).lstrip("/")
        sink_folder = str(sink_uri.path).lstrip("/")
        db_name = ""

        if source_uri.scheme == "mongodb" or source_uri.scheme == "mongodb+srv":
            source_folder = "dump"
        if source_uri.scheme == "gs":
            db_name = source_folder.split("/")[0]
            source_folder = "gcs"
            shell(f"mkdir -p {source_folder}")

        if sink_uri.scheme == "mongodb" or sink_uri.scheme == "mongodb+srv":
            sink_folder = f"dump/{sink_folder}"

        logging.info(f"source folder: {source_folder}")
        logging.info(f"sink folder: {sink_folder}")

        if (
            source_uri.scheme not in ["gs", "mongodb", "mongodb+srv"]
            and not Path(source_folder).is_dir()
        ):
            echo(f"source {source_uri} must be a directory")
            raise Exit(-1)

        if (
            sink_uri.scheme not in ["gs", "mongodb", "mongodb+srv"]
            and Path(sink_folder).exists()
            and any(Path(sink_folder).iterdir())
        ):
            echo(f"sink {sink_uri} must be an empty directory")
            raise Exit(-1)

        if source_uri.scheme == "gs":
            gcs.pull(source_uri.url, folder=source_folder)
            shell(f"mkdir -p {source_folder}/{db_name}")
            shell(
                f"find {source_folder} -type f -exec mv {'{}'} {source_folder}/{db_name} ;"
            )

        elif source_uri.scheme == "mongodb" or source_uri.scheme == "mongodb+srv":
            mongodb.dump_cli(
                source_uri.url,
                collection,
                folder=source_folder,
                n_parallel=jobs,
                jsonarray=json,
            )

        if transform is not None:
            logging.info("Starting transform")
            loader = dataconf.load if Path(transform).exists() else dataconf.loads
            ops = loader(transform, config.Ops)
            mongodb.transform(ops, source_folder, verbose=verbose, jsonarray=json)

        shell(f"mkdir -p {sink_folder}")
        shell(f"find {source_folder} -type f -exec mv {'{}'} {sink_folder} ;")

        if sink_uri.scheme == "gs":
            gcs.push(sink_uri.url, folder=sink_folder)

        elif sink_uri.scheme == "mongodb" or sink_uri.scheme == "mongodb+srv":
            mongodb.restore_cli(
                sink_uri.url,
                collection,
                folder="dump",
                n_parallel=jobs,
                keep_previous=keep_previous,
            )

        if sink_folder != source_folder:
            shell(f"rm -rf {source_folder}")

    except Exit as e:
        raise e

    except Exception as e:
        logging.exception(e)
        echo(f"An error has occured: {e}", err=True)
        raise Exit(1)


@cli.command()
def postgres(
    source: str,
    sink: str,
    transform: Optional[str] = Option(None),
    table: Optional[str] = Option(None),
    jobs: int = Option(4),
    keep_previous: bool = Option(False),
    verbose: bool = Option(False),
):
    try:
        source_uri = furl(source)
        sink_uri = furl(sink)
        source_folder = str(source_uri.path).lstrip("/")
        sink_folder = str(sink_uri.path).lstrip("/")

        logging.info(f"source folder: {source_folder}")
        logging.info(f"sink folder: {sink_folder}")

        if (
            source_uri.scheme not in ["gs", "postgresql"]
            and not Path(source_folder).is_dir()
        ):
            echo(f"source {source_uri} must be a directory")
            raise Exit(-1)

        if (
            sink_uri.scheme not in ["gs", "postgresql"]
            and Path(sink_folder).exists()
            and any(Path(sink_folder).iterdir())
        ):
            echo(f"sink {source_uri} must be an empty directory")
            raise Exit(-1)

        if source_uri.scheme == "gs":
            gcs.pull(source_uri.url, folder=source_folder)

        elif source_uri.scheme == "postgresql":
            postgresql.dump_cli(
                source_uri.url, table, folder=source_folder, n_parallel=jobs
            )

        if transform is not None:
            raise NotImplementedError

        shell(f"mkdir -p {sink_folder}")
        shell(f"find {source_folder} -type f -exec mv {'{}'} {sink_folder} ;")

        if sink_uri.scheme == "gs":
            gcs.push(sink_uri.url, folder=sink_folder)

        elif sink_uri.scheme == "postgresql":
            postgresql.restore_cli(
                sink_uri.url,
                table,
                folder=sink_folder,
                n_parallel=jobs,
                keep_previous=keep_previous,
            )

        if sink_folder != source_folder:
            shell(f"rm -rf {source_folder}")

    except Exit as e:
        raise e

    except Exception as e:
        logging.exception(e)
        echo(f"An error has occured: {e}", err=True)
        raise Exit(1)


@cli.command()
def doctor():
    shell("pg_dump --version")
    shell("pg_restore --version")
    shell("mongodump --version")
    shell("mongorestore --version")
    shell("gsutil --version")


@cli.callback()
def main():
    """
    Smood data mover.
    """
