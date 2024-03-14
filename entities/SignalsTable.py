from sqlalchemy import Column, String, Integer, Text, Enum
from config.database.Sqlalchemy import Sqlalchemy
from entities.Enums import InstrumentTypeEnum, TransactionTypeEnum


class SignalsTable(Sqlalchemy.base):
    __tablename__ = 'SIGNALS'
    __table_args__ = {'extend_existing': True}

    id = Column('ID', Integer, primary_key=True)
    signal_id = Column('CONFIG_KEY', String)
    signal_group_id = Column('CONFIG_KEY', String)
    symbol = Column('CONFIG_KEY', String)
    expiry_date = Column('CONFIG_KEY', String)
    strike_price = Column('CONFIG_KEY', Integer)
    contract_type = Column('CONFIG_KEY', String)
    instrument_type = Column('INSTRUMENT_TYPE', Enum(InstrumentTypeEnum))
    transaction_type = Column('TRANSACTION_TYPE', Enum(TransactionTypeEnum))
    execution_price = Column('CONFIG_KEY', Integer)
    params = Column('PARAMS', Text)