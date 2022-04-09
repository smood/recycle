from os import environ
from pathlib import Path
import shutil

from datacycle.providers import mongo
from datacycle.providers import postgres
import pytest

test_mongo_host = environ.get(
    "TEST_MONGO_HOST", "mongodb://user:password@localhost:27017"
)
test_postgres_host = environ.get(
    "TEST_POSTGRES_HOST", "postgresql://user:password@localhost:5432"
)

mongo_test1 = f"{test_mongo_host}/test1?authSource=admin"
mongo_test2 = f"{test_mongo_host}/test2?authSource=admin"
gcs_test1 = "gs://datacycle/test1"
gcs_test2 = "gs://datacycle/test2"
local_test1 = "./test1"
local_test2 = "./test2"
postgres_test = f"{test_postgres_host}/test"
postgres_test1 = f"{test_postgres_host}/test1"
postgres_test2 = f"{test_postgres_host}/test2"


@pytest.fixture()
def clean_mongo():
    mongo.get_database(mongo_test1).command("dropDatabase")
    mongo.get_database(mongo_test2).command("dropDatabase")
    yield


@pytest.fixture()
def clean_postgres():
    db = postgres.get_database(postgres_test)

    for table in ["test1", "test2"]:
        for sql in [f"DROP DATABASE IF EXISTS {table}", f"CREATE DATABASE {table}"]:
            db.execute(sql)

    yield


@pytest.fixture()
def clean_fs():
    for file in [local_test1, local_test2, "dump"]:
        if Path(file).exists():
            shutil.rmtree(file)
    yield


def require_env_var(name):
    return pytest.mark.skipif(
        name not in environ,
        reason=f"{name} not found in environ variables"
    )
