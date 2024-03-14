from sqlalchemy import Column, String, Integer
from config.database.Sqlalchemy import Sqlalchemy


class ClientConfigurationTable(Sqlalchemy.base):
    __tablename__ = 'CONFIG'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)
    key = Column('CONFIG_KEY', String)
    value = Column('CONFIG_VALUE', String)