import datetime
from sqlalchemy import Column, String, Integer, TIMESTAMP
from config.database.Sqlalchemy import Sqlalchemy
import pytz

class GlobalConfigurationTable(Sqlalchemy.base):
    __tablename__ = 'CONFIG'
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)
    key = Column('CONFIG_KEY', String)
    value = Column('CONFIG_VALUE', String)
    created_date_time = Column('CREATED_DATE_TIME', TIMESTAMP, default=datetime.datetime.now(pytz.timezone('Asia/Kolkata')))