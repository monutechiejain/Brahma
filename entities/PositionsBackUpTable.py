from sqlalchemy import Column, Integer, String, Text, Boolean, Enum, JSON
from config.database.Sqlalchemy import Sqlalchemy
from entities.Enums import InstrumentTypeEnum, TransactionTypeEnum, OrderTypeEnum

class PositionsBackUpTable(Sqlalchemy.base):
    __tablename__ = 'POSITIONS_BACK_UP'
    __table_args__ = {'extend_existing': True}

    id = Column('ID', Integer, primary_key=True)
    signal_id = Column('SIGNAL_ID', String)
    signal_group_id = Column('SIGNAL_GROUP_ID', String)
    position_id = Column('POSITION_ID', String)
    position_group_id = Column('POSITION_GROUP_ID', String)
    moneyness = Column('MONEYNESS', String)
    symbol = Column('SYMBOL', String)
    expiry_date = Column('EXPIRY_DATE', String)
    strike_price = Column('STRIKE_PRICE', Integer)
    instrument_type = Column('INSTRUMENT_TYPE', Enum(InstrumentTypeEnum))
    transaction_type = Column('TRANSACTION_TYPE', Enum(TransactionTypeEnum))
    order_type = Column('ORDER_TYPE', Enum(OrderTypeEnum))
    contract_type = Column('CONTRACT_TYPE', String)
    num_lots = Column('NUM_LOTS', Integer)
    lot_size = Column('LOT_SIZE', Integer)
    quantity = Column('QUANTITY', Integer)
    is_active = Column('IS_ACTIVE', Boolean, unique=False, default=True)
    margin_overall = Column('MARGIN_OVERALL', Integer)
    entry_price = Column('ENTRY_PRICE', Integer)
    exit_price = Column('EXIT_PRICE', Integer)
    params = Column('PARAMS', JSON)
    realized_pnl = Column('REALIZED_PNL', Integer)
    realized_pnl_group = Column('REALIZED_PNL_GROUP', Integer)
    realized_pnl_overall = Column('REALIZED_PNL_OVERALL', Integer)

