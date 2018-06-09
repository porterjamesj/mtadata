from sqlalchemy import Column, Integer, String, Numeric, MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.dialects.postgresql import UUID, insert

def start_db(conn_str):
    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)
    MTABase.prepare(engine)
    return engine, Session

# TODO nicer way to do this, this is a bit gross
# items are dictionaries
def bulk_upsert(Model, session, items):
    if not items:
        return
    session.execute(
        insert(Model.__table__)
        .values(items)
        .on_conflict_do_nothing()
    )


Base = declarative_base(metadata=MetaData(schema="mta"))

class MTABase(Base, DeferredReflection):
    __abstract__ = True


class StopTime(MTABase):
    __tablename__ = 'stop_times'

class Trip(MTABase):
    __tablename__ = 'trips'

class Stop(MTABase):
    __tablename__ = 'stops'
