from sqlalchemy import Column, String, Integer
from config.database.Sqlalchemy import Sqlalchemy


class LiveAccountsTable(Sqlalchemy.base):
    __tablename__ = 'LIVE_ACCOUNTS'
    __table_args__ = {'extend_existing': True}

    live_strategy_id = Column(Integer, primary_key=True)
    cluster_id = Column('CLUSTER_ID', String)
    is_active = Column('IS_ACTIVE', Integer, unique=False, default=1)