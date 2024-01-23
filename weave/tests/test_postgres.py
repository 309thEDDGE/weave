import sqlalchemy as sqla


def test_validate_no_metadata_file(test_validate):
    engine = sqla.create_engine(sqla.engine.url.URL(
        drivername="postgresql",
        username="postgres",
        password="postgres",
        host="postgres",
        database="postgres",
        query={},
        port="5432",
    ))

    conn = engine.connect()