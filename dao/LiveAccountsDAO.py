
from config.database.Sqlalchemy import Sqlalchemy
from entities.LiveAccountsTable import LiveAccountsTable
from common.constants import PROFILE_PROPERTIES
import pandas as pd

def fetchCustomers():
    '''
    GET ACTIVE CUSTOMERS
    :param key:
    :return:
    '''

    session = Sqlalchemy(PROFILE_PROPERTIES['SCHEMA_NAME'])
    live_strategy_result = session.query(LiveAccountsTable).filter(LiveAccountsTable.is_active == 1)
    df_live_strategy_result = pd.read_sql(live_strategy_result.statement, session.bind)
    session.close()
    return df_live_strategy_result