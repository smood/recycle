import logging

from datacycle import config
from datacycle.utils import shell
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import sqlparse


def get_database(uri):
    return create_engine(
        uri.replace("postgresql://", "postgresql+pg8000://"), poolclass=NullPool
    ).execution_options(isolation_level="AUTOCOMMIT")


def log_formatter(log):
    return ":".join(log.split(":")[1:])


def dump_cli(uri, table=None, folder="dump", n_parallel=10):
    cmd = f"pg_dump --verbose --clean --file={folder} --format=d --jobs={n_parallel}"

    if table is not None:
        cmd += f" --table={table}"

    cmd += f" {uri}"
    shell(cmd, log_formatter=log_formatter)


def restore_cli(uri, table=None, folder="dump", n_parallel=10, keep_previous=False):

    if table is None:
        toc = shell(f"pg_restore --list --format=d --dbname={uri} {folder}")
        tables = [
            entry.split()[5]
            for entry in toc
            if not entry.startswith(";")
            and "TABLE" in entry
            and "TABLE DATA" not in entry
        ]
    else:
        tables = [table]

    if not keep_previous:
        logging.info(f"dropping {', '.join(tables)}")
        cmd = f"psql {uri} -c ".split() + [
            "; ".join([f"DROP TABLE IF EXISTS {t}" for t in tables])
        ]
        shell(cmd, log_formatter=log_formatter)

    cmd = f"pg_restore --verbose --format=d --jobs={n_parallel} --dbname={uri} {folder}"
    shell(cmd, log_formatter=log_formatter)


def transform(ops: config.Ops, folder="dump", **kwargs):
    sqlparse.split("")
