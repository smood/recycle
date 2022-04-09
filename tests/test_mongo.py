from datetime import datetime
from datetime import timezone
import logging
from pathlib import Path
import shutil

import bson
from datacycle.providers import mongo
from dictdiffer import diff
from tests.conftest import mongo_test1
from tests.utils import seed_mongo

n = 10


class TestMongo:
    def test_seeds(self, clean_mongo):
        seed_mongo(mongo_test1, "profiles", n)

    def test_dump_cli_restore_cli(self, caplog, clean_mongo):
        db = mongo.get_database(mongo_test1)

        path = Path("dump")
        if path.exists():
            shutil.rmtree(path.absolute())

        seed_mongo(mongo_test1, "profiles", n)
        before = list(db["profiles"].find())

        with caplog.at_level(logging.INFO):
            mongo.dump_cli(mongo_test1, folder=path.absolute())
        assert path.exists()

        db["profiles"].drop()
        assert db["profiles"].count_documents({}) == 0

        with caplog.at_level(logging.INFO):
            mongo.restore_cli(mongo_test1, folder=path.absolute())
        assert db["profiles"].count_documents({}) == n

        after = list(db["profiles"].find())
        assert before == after

    def test_dump_cli_restore_py(self, caplog, clean_mongo):
        db = mongo.get_database(mongo_test1)

        path = Path("dump")
        if path.exists():
            shutil.rmtree(path.absolute())

        seed_mongo(mongo_test1, "profiles", n)
        before = list(db["profiles"].find())

        with caplog.at_level(logging.INFO):
            mongo.dump_cli(mongo_test1, folder=path.absolute())
        assert path.exists()

        db["profiles"].drop()
        assert db["profiles"].count_documents({}) == 0

        mongo.restore_py(mongo_test1, folder=path.absolute())
        assert db["profiles"].count_documents({}) == n

        after = list(db["profiles"].find())
        assert before == after

    def test_dump_cli_filter_restore_cli(self, clean_mongo):
        db = mongo.get_database(mongo_test1)

        path = Path("dump")
        if path.exists():
            shutil.rmtree(path.absolute())

        seed_mongo(mongo_test1, "profiles", n)
        before = list(db["profiles"].find())

        mongo.dump_cli(mongo_test1, folder=path.absolute())
        assert path.exists()

        def remove_username(doc):
            del doc["username"]
            return True

        mongo.filtr(path / db.name / "profiles.bson.gz", remove_username)

        db["profiles"].drop()
        assert db["profiles"].count_documents({}) == 0

        mongo.restore_cli(mongo_test1, folder=path.absolute())
        assert db["profiles"].count_documents({}) == n

        after = list(db["profiles"].find())

        for op, loc, values in diff(before, after):
            assert op == "remove"
            assert values[0][0] == "username"

    def test_filter(self, clean_mongo):
        db = mongo.get_database(mongo_test1)

        collection = "test"
        path = Path("dump")
        if path.exists():
            shutil.rmtree(path.absolute())

        db[collection].insert_many(
            [
                {
                    "_id": bson.ObjectId("60ddd22446ec2a173c0a2ced"),
                    "value": "1",
                    "createdAt": datetime(2021, 6, 30),
                },
                {"value": "2", "createdAt": datetime(2021, 7, 1)},
                {"value": "3", "createdAt": datetime(2021, 7, 8)},
            ]
        )
        mongo.dump_cli(mongo_test1, folder=path.absolute())
        assert db[collection].count_documents({}) == 3

        def keep_all(doc):
            return True

        mongo.filtr(path / db.name / f"{collection}.bson.gz", keep_all)

        mongo.restore_cli(mongo_test1, folder=path.absolute())
        assert db[collection].count_documents({}) == 3

        def keep_date(doc):
            return doc["createdAt"] <= datetime(2021, 7, 1)

        mongo.filtr(path / db.name / f"{collection}.bson.gz", keep_date)

        mongo.restore_cli(mongo_test1, folder=path.absolute())
        assert db[collection].count_documents({}) == 2
        assert [d["value"] for d in db[collection].find({}, {"value": 1})] == ["1", "2"]

        def keep_id(doc):
            return doc["_id"].generation_time < datetime(
                2021, 7, 1, 14, 36, tzinfo=timezone.utc
            )

        mongo.filtr(path / db.name / f"{collection}.bson.gz", keep_id)

        mongo.restore_cli(mongo_test1, folder=path.absolute())
        assert db[collection].count_documents({}) == 1
        assert [d["value"] for d in db[collection].find({}, {"value": 1})] == ["1"]

        def keep_none(doc):
            return False

        mongo.filtr(path / db.name / f"{collection}.bson.gz", keep_none)

        mongo.restore_cli(mongo_test1, folder=path.absolute())
        assert db[collection].count_documents({}) == 0
