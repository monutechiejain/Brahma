from sqlalchemy import Column, String, Text
from config.database.Sqlalchemy import Sqlalchemy


class TrackerTable(Sqlalchemy.base):
    __tablename__ = 'TRACKER'
    __table_args__ = {'extend_existing': True}

    symbol = Column('SYMBOL', String, primary_key=True)
    account = Column('ACCOUNT', String)
    qty_sell = Column('QTY_SELL', String)
    sp_sell = Column('SP_SELL', String)
    current_underlying = Column('CURRENT_UNDERLYING', String)
    net_pnl = Column('NET_PNL', String)
    net_pnl_pct = Column('NET_PNL_PCT', String)
    margin_used = Column('MARGIN_USED', String)
    positions_matched = Column('POSITIONS_MATCHED', String)
    positions_closed = Column('POSITIONS_CLOSED', String)
    positions_count_db = Column('POSITIONS_COUNT_DB', String)
    positions_count_broker = Column('POSITIONS_COUNT_BROKER', String)
    qty_count_db = Column('QTY_COUNT_DB', String)
    qty_count_broker = Column('QTY_COUNT_BROKER', String)
    rag = Column('RAG', String)
    refresh_interval = Column('REFRESH_INTERVAL', String)
    last_updated_time = Column('LAST_UPDATED_TIME', String)
    margin_used_broker = Column('MARGIN_USED_BROKER', String)
    execution_iv = Column('EXECUTION_IV', String)
    current_iv = Column('CURRENT_IV', String)
    stoploss = Column('STOPLOSS', String)
    target = Column('TARGET', String)
    params = Column('PARAMS', Text)