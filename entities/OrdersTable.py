from sqlalchemy import Column, Integer, String, Text, Boolean, Enum, Numeric, JSON
from config.database.Sqlalchemy import Sqlalchemy
from entities.Enums import InstrumentTypeEnum, TransactionTypeEnum, OrderTypeEnum

class OrdersTable(Sqlalchemy.base):
    __tablename__ = 'ORDERS'
    __table_args__ = {'extend_existing': True}

    id = Column('ID', Integer, primary_key=True)
    order_id = Column('ORDER_ID', String)
    order_group_id = Column('ORDER_GROUP_ID', String)
    position_id = Column('POSITION_ID', String)
    position_group_id = Column('POSITION_GROUP_ID', String)
    broker_order_id = Column('BROKER_ORDER_ID', String)
    broker_order_status = Column('BROKER_ORDER_STATUS', String)
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
    is_success = Column('IS_SUCCESS', Boolean, unique=False, default=True)
    margin_overall = Column('MARGIN_OVERALL', Numeric)
    execution_price = Column('EXECUTION_PRICE', Numeric)
    params = Column('PARAMS', JSON)

