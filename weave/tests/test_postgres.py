import sqlalchemy as sqla

import weave


def test_validate_no_metadata_file():
    engine = sqla.create_engine(sqla.engine.url.URL(
        drivername="postgresql",
        username="postgres",
        password="postgres",
        host="localhost",
        database="postgres",
        query={},
        port="5432",
    ))

    conn = engine.connect()
    