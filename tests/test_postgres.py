import logging
from pathlib import Path
import shutil

from recycle.providers import postgres
from tests.conftest import postgres_test1
from tests.utils import seed_postgres

n = 10


class TestPostgres:
    def test_seeds(self, clean_postgres):
        seed_postgres(postgres_test1, "profiles", n)

    def test_dump_cli_restore_cli(self, caplog, clean_postgres):
        db = postgres.get_database(postgres_test1)

        path = Path("dump")
        if path.exists():
            shutil.rmtree(path.absolute())

        seed_postgres(postgres_test1, "profiles", n)
        before = list(db.execute("SELECT * FROM profiles"))

        with caplog.at_level(logging.INFO):
            postgres.dump_cli(postgres_test1, folder=path.absolute())
        assert path.exists()

        db.execute("DROP TABLE profiles")
        assert (
            len(
                list(
                    db.execute(
                        "SELECT 1 FROM information_schema.tables WHERE table_name = 'profiles'"
                    )
                )
            )
            == 0
        )

        with caplog.at_level(logging.INFO):
            postgres.restore_cli(postgres_test1, folder=path.absolute())
        assert next(db.execute("SELECT COUNT(*) FROM profiles"))[0] == n

        after = list(db.execute("SELECT * FROM profiles"))
        assert before == after
