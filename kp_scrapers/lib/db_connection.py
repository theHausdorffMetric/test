from contextlib import contextmanager

import sqlalchemy as db
from sqlalchemy.orm import sessionmaker


@contextmanager
def db_conn_orm(db_url):
    engine = db.create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    engine.dispose()
