from datetime import datetime
from datetime import timedelta
import logging
from pathlib import Path
import random

import bson
from datacycle.main import cli
from datacycle.providers import gcs
from datacycle.providers import mongo
import pytest
from tests.conftest import gcs_test1, require_env_var
from tests.conftest import gcs_test2
from tests.conftest import local_test1
from tests.conftest import local_test2
from tests.conftest import mongo_test1
from tests.conftest import mongo_test2
from tests.utils import seed_mongo
from typer.testing import CliRunner


@pytest.fixture()
def snapshot():
    return f"static-{random.randint(1, 100_000)}"


runner = CliRunner()


def run(cmd):
    print(" ".join(cmd))
    res = runner.invoke(cli, cmd)
    assert res.exit_code == 0


class TestCli:
    def test_mongo_mongo(self, caplog, clean_mongo, clean_fs):

        seed_mongo(mongo_test1, "collection", 50)
        db1 = mongo.get_database(mongo_test1)["collection"]
        db2 = mongo.get_database(mongo_test2)["collection"]

        assert db1.count_documents({}) == 50
        assert db2.count_documents({}) == 0

        cmd = f"mongo {mongo_test1} {mongo_test2}".split()
        run(cmd)

        assert db1.count_documents({}) == 50
        assert db2.count_documents({}) == 50

    @require_env_var(gcs.SA_ENV)
    def test_mongo_gcs(self, caplog, clean_mongo, clean_fs, snapshot):

        seed_mongo(mongo_test1, "collection", 50)

        cmd = f"mongo {mongo_test1} {gcs_test1}/{snapshot}".split()
        run(cmd)

        remote_fs = gcs.list(f"{gcs_test1}/{snapshot}")
        assert sorted(remote_fs) == [
            f"{gcs_test1}/{snapshot}/:",
            f"{gcs_test1}/{snapshot}/collection.bson.gz",
            f"{gcs_test1}/{snapshot}/collection.metadata.json.gz",
        ]

    def test_mongo_fs(self, caplog, clean_mongo, clean_fs, snapshot):

        seed_mongo(mongo_test1, "collection", 50)

        cmd = f"mongo {mongo_test1} {local_test1}/{snapshot}".split()
        run(cmd)

        local_fs = [str(p) for p in Path(f"{local_test1}/{snapshot}").glob("**/*")]
        assert sorted(local_fs) == [
            f"{local_test1.lstrip('./')}/{snapshot}/collection.bson.gz",
            f"{local_test1.lstrip('./')}/{snapshot}/collection.metadata.json.gz",
        ]

    @require_env_var(gcs.SA_ENV)
    def test_gcs_mongo(self, caplog, clean_mongo, clean_fs, snapshot):

        self.test_mongo_gcs(caplog, clean_mongo, clean_fs, snapshot)

        db2 = mongo.get_database(mongo_test2)["collection"]
        assert db2.count_documents({}) == 0

        cmd = f"mongo {gcs_test1}/{snapshot} {mongo_test2}".split()
        run(cmd)

        assert db2.count_documents({}) == 50

    @require_env_var(gcs.SA_ENV)
    def test_gcs_gcs(self, caplog, clean_mongo, clean_fs, snapshot):

        self.test_mongo_gcs(caplog, clean_mongo, clean_fs, snapshot)

        cmd = f"mongo {gcs_test1}/{snapshot} {gcs_test2}/{snapshot}".split()
        run(cmd)

        remote_fs = gcs.list(f"{gcs_test2}/{snapshot}")
        assert sorted(remote_fs) == [
            f"{gcs_test2}/{snapshot}/:",
            f"{gcs_test2}/{snapshot}/collection.bson.gz",
            f"{gcs_test2}/{snapshot}/collection.metadata.json.gz",
        ]

    @require_env_var(gcs.SA_ENV)
    def test_gcs_fs(self, caplog, clean_mongo, clean_fs, snapshot):

        self.test_mongo_gcs(caplog, clean_mongo, clean_fs, snapshot)

        cmd = f"mongo {gcs_test1}/{snapshot} {local_test2}/{snapshot}".split()
        run(cmd)

        local_fs = [str(p) for p in Path(f"{local_test2}/{snapshot}").glob("**/*")]
        assert sorted(local_fs) == [
            f"{local_test2.lstrip('./')}/{snapshot}/collection.bson.gz",
            f"{local_test2.lstrip('./')}/{snapshot}/collection.metadata.json.gz",
        ]

    def test_fs_mongo(self, caplog, clean_mongo, clean_fs, snapshot):

        self.test_mongo_fs(caplog, clean_mongo, clean_fs, snapshot)
        db2 = mongo.get_database(mongo_test2)["collection"]
        assert db2.count_documents({}) == 0

        cmd = f"mongo {local_test1}/{snapshot} {mongo_test2}".split()
        run(cmd)

        assert db2.count_documents({}) == 50

    @require_env_var(gcs.SA_ENV)
    def test_fs_gcs(self, caplog, clean_mongo, clean_fs, snapshot):

        self.test_mongo_fs(caplog, clean_mongo, clean_fs, snapshot)

        cmd = f"mongo {local_test1}/{snapshot} {gcs_test2}/{snapshot}".split()
        run(cmd)

        remote_fs = gcs.list(f"{gcs_test2}/{snapshot}")
        assert sorted(remote_fs) == [
            f"{gcs_test2}/{snapshot}/:",
            f"{gcs_test2}/{snapshot}/collection.bson.gz",
            f"{gcs_test2}/{snapshot}/collection.metadata.json.gz",
        ]

    def test_fs_fs(self, caplog, clean_mongo, clean_fs, snapshot):
        self.test_mongo_fs(caplog, clean_mongo, clean_fs, snapshot)

        cmd = f"mongo {local_test1}/{snapshot} {local_test2}/{snapshot}".split()
        run(cmd)

        local_fs = [str(p) for p in Path(f"{local_test2}/{snapshot}").glob("**/*")]
        assert sorted(local_fs) == [
            f"{local_test2.lstrip('./')}/{snapshot}/collection.bson.gz",
            f"{local_test2.lstrip('./')}/{snapshot}/collection.metadata.json.gz",
        ]

    def test_mongo_transform_mongo(self, caplog, clean_mongo, clean_fs):

        ops1 = """
        transforms {
            test1 {
                data {
                    keep = 5d
                    anonymize {
                        to_anon = str
                    }
                }
            }
        }
        """
        db1 = mongo.get_database(mongo_test1)["data"]
        db2 = mongo.get_database(mongo_test2)["data"]

        now = datetime.utcnow()
        db1.insert_many(
            [
                dict(
                    to_anon=str(i),
                    original=str(i),
                    _id=bson.ObjectId.from_datetime(now - timedelta(days=i)),
                )
                for i in range(10)
            ]
        )
        assert db1.count_documents({}) == 10

        with caplog.at_level(logging.INFO):
            cmd = f"mongo {mongo_test1} {mongo_test2} --transform".split() + [ops1]
            res = runner.invoke(cli, cmd)

        assert res.exit_code == 0
        assert db1.count_documents({}) == 10
        assert db2.count_documents({}) == 5

        processed = list(db2.find())
        for p in processed:
            assert p["to_anon"] != p["original"]
