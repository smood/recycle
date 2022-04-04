from faker import Faker
from recycle.providers import mongo
from recycle.providers import postgres
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

faker = Faker()


def seed_mongo(uri, collection, n):
    records = [faker.profile(["job", "username", "sex"]) for i in range(n)]
    db = mongo.get_database(uri)
    db[collection].drop()
    db[collection].insert_many(records)
    assert db[collection].count_documents({}) == n


def seed_postgres(uri, table, n):
    records = [
        {
            "string": faker.pystr(),
            "datetime": faker.date_time(),
            "integer": faker.pyint(),
            "float": faker.pyfloat(),
            "boolean": faker.pybool(),
            "text": faker.json(),
            "json": faker.json(),
        }
        for i in range(n)
    ]

    db = postgres.get_database(uri)
    session = scoped_session(sessionmaker(bind=db))()
    users = Table(
        table,
        MetaData(bind=db),
        Column("string", String(100)),
        Column("datetime", DateTime()),
        Column("integer", Integer()),
        Column("float", Float()),
        Column("boolean", Boolean()),
        Column("text", Text()),
        Column("json", JSON(100)),
    )

    users.drop(checkfirst=True)
    users.create()
    session.execute(users.insert(), records)

    assert next(session.execute(f"SELECT COUNT(*) FROM {table}"))[0] == n
    session.close()
