
from config.database.Sqlalchemy import Sqlalchemy
from entities.PositionsTrackingBackUpTable import PositionsTrackingBackUpTable
from datetime import datetime
import pytz
import pandas as pd
from sqlalchemy import func
import numpy as np


def insert(schema_name, row):
    '''
    Insert Positions in Positions Table
    Get Trans ID/Order Id Sequence Next Value and Populate in Positions Table
    :return:
    '''
    session = Sqlalchemy(schema_name)

    positions = PositionsTrackingBackUpTable(signal_id=row['signal_id'],
                                       signal_group_id=row['signal_group_id'],
                                       position_id=row['position_id'],
                                       position_group_id=row['position_group_id'],
                                       position_tracking_id=row['position_tracking_id'],
                                       position_tracking_group_id=row['position_tracking_group_id'],
                                       moneyness=row['moneyness'],
                                       symbol=row['symbol'],
                                       expiry_date=row['expiry_date'],
                                       strike_price=row['strike_price'],
                                       instrument_type=row['instrument_type'],
                                       transaction_type=row['transaction_type'],
                                       order_type=row['order_type'],
                                       contract_type=row['contract_type'],
                                       num_lots=row['num_lots'],
                                       lot_size=row['lot_size'],
                                       quantity=row['quantity'],
                                       is_active=row['is_active'],
                                       is_square_off=row['is_square_off'],
                                       margin_overall=row['margin_overall'],
                                       entry_price=row['entry_price'],
                                       exit_price=row['exit_price'],
                                       current_price=row['current_price'],
                                       execution_price=row['execution_price'],
                                       params=row['params'],
                                       unrealized_pnl=row['unrealized_pnl'],
                                       realized_pnl=row['realized_pnl'],
                                       unrealized_pnl_group=row['unrealized_pnl_group'],
                                       realized_pnl_group=row['realized_pnl_group'],
                                       net_pnl_overall=row['net_pnl_overall'],
                                       realized_pnl_overall=row['realized_pnl_overall'],
                                       entry_delta=row['entry_delta'],
                                       entry_net_delta=row['entry_net_delta'],
                                       entry_net_delta_overall=row['entry_net_delta_overall'],
                                       current_delta=row['current_delta'],
                                       current_net_delta=row['current_net_delta'],
                                       current_net_delta_overall=row['current_net_delta_overall'],
                                       entry_gamma=row['entry_gamma'],
                                       entry_net_gamma=row['entry_net_gamma'],
                                       current_gamma=row['current_gamma'],
                                       current_net_gamma=row['current_net_gamma'],
                                       entry_iv=row['entry_iv'],
                                       entry_iv_diff_pct=row['entry_iv_diff_pct'],
                                       current_iv=row['current_iv'],
                                       current_iv_diff_pct=row['current_iv_diff_pct'],
                                       net_delta_threshold=row['net_delta_threshold'],
                                       entry_theta=row['entry_theta'],
                                       current_theta=row['current_theta'],
                                       entry_net_theta=row['entry_net_theta'],
                                       current_net_theta=row['current_net_theta'],
                                       entry_vega=row['entry_vega'],
                                       current_vega=row['current_vega'],
                                       entry_net_vega=row['entry_net_vega'],
                                       current_net_vega=row['current_net_vega'],
                                       contract_expiry_date=row['contract_expiry_date'],
                                       entry_time_to_expiry=row['entry_time_to_expiry'],
                                       current_time_to_expiry=row['current_time_to_expiry'],
                                       order_manifest=row['order_manifest'],
                                       time_value_options=row['time_value_options'],
                                       entry_underlying=row['entry_underlying'],
                                       current_underlying=row['current_underlying'],
                                       expected_theta_pnl_pending=row['expected_theta_pnl_pending'],
                                       current_theta_pnl_pending=row['current_theta_pnl_pending'],
                                       net_pnl_threshold=row['net_pnl_threshold'],
                                       entry_atm_put_price=row['entry_atm_put_price'],
                                       entry_atm_call_price=row['entry_atm_call_price'],
                                       entry_atm_avg_price=row['entry_atm_avg_price'],
                                       entry_atm_price_diff=row['entry_atm_price_diff'],
                                       entry_vix=row['entry_vix'],
                                       current_price_pnl_pct=row['current_price_pnl_pct']
                                       )
    session.add(positions)
    session.commit()
    session.close()

def getActivePositionsByLatestPositionTrackingGroupIdAndSymbol(schema_name, position_group_id, symbol):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    max_position_tracking_group_id = session.query(func.max(PositionsTrackingBackUpTable.position_tracking_group_id)).scalar()
    positions_tracking_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                  PositionsTrackingBackUpTable.symbol == symbol,
                                                                  PositionsTrackingBackUpTable.position_tracking_group_id == max_position_tracking_group_id)
    df_positions_tracking_result = pd.read_sql(positions_tracking_result.statement, session.bind)
    session.close()
    return df_positions_tracking_result

def getPositionsByLatestPositionTrackingGroupIdAndSymbol(schema_name, symbol):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    max_position_tracking_group_id = session.query(func.max(PositionsTrackingBackUpTable.position_tracking_group_id)).scalar()
    positions_tracking_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.symbol == symbol,
                                                                  PositionsTrackingBackUpTable.position_tracking_group_id == max_position_tracking_group_id)
    df_positions_tracking_result = pd.read_sql(positions_tracking_result.statement, session.bind)
    session.close()
    return df_positions_tracking_result

def getLatestPositionsTrackingBackUpRows(schema_name):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    max_position_tracking_group_id = session.query(func.max(PositionsTrackingBackUpTable.position_tracking_group_id)).scalar()
    positions_tracking_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_tracking_group_id == max_position_tracking_group_id)
    df_positions_tracking_result = pd.read_sql(positions_tracking_result.statement, session.bind)
    session.close()
    df_positions_tracking_result.sort_values(by=['POSITION_TRACKING_ID'], inplace=True, ascending=False)
    df_positions_tracking_result = df_positions_tracking_result.replace({np.nan: None})
    return df_positions_tracking_result

def getActivePositionsByTransIdAndSymbol(schema_name, position_group_id, symbol):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    ia_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                  PositionsTrackingBackUpTable.symbol == symbol,
                                                                  PositionsTrackingBackUpTable.is_active == 'Y')
    df_positions_result = pd.read_sql(ia_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getActivePositionsByTransIdSymbolAndOrderType(schema_name, position_group_id, symbol, orderType):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    ia_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                  PositionsTrackingBackUpTable.symbol == symbol,
                                                                  PositionsTrackingBackUpTable.order_type == orderType,
                                                                  PositionsTrackingBackUpTable.is_active == 'Y')
    df_positions_result = pd.read_sql(ia_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getActivePositionsByTransIdSymbolAndExpiry(schema_name, position_group_id, symbol, expiryDate):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                  PositionsTrackingBackUpTable.symbol == symbol,
                                                                  PositionsTrackingBackUpTable.contract_expiry_date == expiryDate,
                                                                  PositionsTrackingBackUpTable.is_active == 'Y')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getActivePositionsByTransId(schema_name, position_group_id):
    '''
    Get Positions By Trans ID ONLY ACTIVE POSITIONS
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                  PositionsTrackingBackUpTable.is_active == 'Y')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getActivePositionsByTransIdAll(schema_name, position_group_id):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id)
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getNonActivePositionsAll(schema_name):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.is_active == 'N')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getPositionsAll(schema_name):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    positions_result = session.query(PositionsTrackingBackUpTable)
    df_positions_result = pd.read_sql(positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getNonActivePositionsByTransIdAndInstrumentType(schema_name, position_group_id, instrument_type):
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                   PositionsTrackingBackUpTable.instrument_type == instrument_type,
                                                                   PositionsTrackingBackUpTable.is_active == 'N')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getNonActivePositionsByTransIdSymbolAndInstrumentType(schema_name, position_group_id, symbol, instrument_type):
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                   PositionsTrackingBackUpTable.instrument_type == instrument_type,
                                                                   PositionsTrackingBackUpTable.symbol == symbol,
                                                                   PositionsTrackingBackUpTable.is_active == 'N')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getActivePositionsByTransIdSymbolAndInstrumentType(schema_name, position_group_id, symbol, instrument_type):
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                   PositionsTrackingBackUpTable.instrument_type == instrument_type,
                                                                   PositionsTrackingBackUpTable.symbol == symbol,
                                                                   PositionsTrackingBackUpTable.is_active == 'Y')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result


def getActivePositionsByTransIdAndSymbolAll(schema_name, position_group_id, symbol):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                                  PositionsTrackingBackUpTable.symbol == symbol)
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getActivePositionsBySymbol(schema_name, symbol):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    ia_positions_result = session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.symbol == symbol,
                                                                  PositionsTrackingBackUpTable.is_active == 'Y')
    df_positions_result = pd.read_sql(ia_positions_result.statement, session.bind)
    session.close()
    return df_positions_result


def updatePositionsPostSquareOffJob(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.strike_price == row_position['strike_price'],
                                              PositionsTrackingBackUpTable.instrument_type == row_position['instrument_type'],
                                              PositionsTrackingBackUpTable.contract_expiry_date == row_position['contract_expiry_date'],
                                              PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'current_price': row_position['current_price_actual'],
                'squareoff_price': row_position['current_price'],
                'squareoff_price_actual': row_position['current_price_actual'],
                'broker_order_id_squareoff' : row_position['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row_position['broker_order_status_squareoff'],
                'updated_date': current_date,
                'unrealized_pnl': row_position['unrealized_pnl'],
                'realized_pnl': row_position['realized_pnl'],
                'unrealized_pnl_txn': row_position['unrealized_pnl_txn'],
                'realized_pnl_txn': row_position['realized_pnl_txn'],
                'realized_pnl_overall': row_position['realized_pnl_overall'],
                'realized_pnl_futures': row_position['realized_pnl_futures'],
                'realized_pnl_options': row_position['realized_pnl_options'],
                'unrealized_pnl_overall': row_position['unrealized_pnl_overall'],
                'net_pnl_overall': row_position['net_pnl_overall'],
                'margin_txn' : row_position['margin_txn'],
                'quantity' : row_position['quantity'],
                'multi_factor': row_position['multi_factor'],
                'is_active': row_position['is_active'],
                'current_net_delta': row_position['current_net_delta'],
                'current_net_delta_overall': row_position['current_net_delta_overall'],
                'is_trailing_active': row_position['is_trailing_active'],
                'trailing_pnl': row_position['trailing_pnl'],
                'brokerage': row_position['brokerage'],
                'realized_pnl_overall_brokerage': row_position['realized_pnl_overall_brokerage'],
                'expected_theta_pnl_pending': row_position['expected_theta_pnl_pending'],
                'squareoff_params': row_position['squareoff_params']
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobWithOrderType(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.strike_price == row_position['strike_price'],
                                              PositionsTrackingBackUpTable.instrument_type == row_position['instrument_type'],
                                              PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'current_price': row_position['current_price_actual'],
                'squareoff_price': row_position['current_price'],
                'squareoff_price_actual': row_position['current_price_actual'],
                'broker_order_id_squareoff' : row_position['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row_position['broker_order_status_squareoff'],
                'updated_date': current_date,
                'unrealized_pnl': row_position['unrealized_pnl'],
                'realized_pnl': row_position['realized_pnl'],
                'unrealized_pnl_txn': row_position['unrealized_pnl_txn'],
                'realized_pnl_txn': row_position['realized_pnl_txn'],
                'realized_pnl_overall': row_position['realized_pnl_overall'],
                'realized_pnl_options': row_position['realized_pnl_options'],
                'realized_pnl_futures': row_position['realized_pnl_futures'],
                'unrealized_pnl_overall': row_position['unrealized_pnl_overall'],
                'margin_txn' : row_position['margin_txn'],
                'quantity' : row_position['quantity'],
                'multi_factor': row_position['multi_factor'],
                'is_active': row_position['is_active'],
                'current_net_delta': row_position['current_net_delta'],
                'current_net_delta_overall': row_position['current_net_delta_overall'],
                'execution_params': row_position['execution_params'],
                'order_type': row_position['order_type']
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobOptions(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.strike_price == row_position['strike_price'],
                                              PositionsTrackingBackUpTable.instrument_type == row_position['instrument_type'],
                                              PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'current_price': row_position['current_price_actual'],
                'squareoff_price': row_position['current_price'],
                'squareoff_price_actual': row_position['current_price_actual'],
                'broker_order_id_squareoff' : row_position['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row_position['broker_order_status_squareoff'],
                'updated_date': current_date,
                'unrealized_pnl': row_position['unrealized_pnl'],
                'realized_pnl': row_position['realized_pnl'],
                'unrealized_pnl_txn': row_position['unrealized_pnl_txn'],
                'realized_pnl_txn': row_position['realized_pnl_txn'],
                'realized_pnl_overall': row_position['realized_pnl_overall'],
                'realized_pnl_options': row_position['realized_pnl_options'],
                'realized_pnl_futures': row_position['realized_pnl_futures'],
                'unrealized_pnl_overall': row_position['unrealized_pnl_overall'],
                'quantity' : row_position['quantity'],
                'multi_factor': row_position['multi_factor'],
                'is_active': row_position['is_active'],
                'current_net_delta': row_position['current_net_delta'],
                'current_net_delta_overall': row_position['current_net_delta_overall'],
                'execution_params': row_position['execution_params']
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobOptionsMarginFreeUp(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.strike_price == row_position['strike_price'],
                                              PositionsTrackingBackUpTable.instrument_type == row_position['instrument_type'],
                                              PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'current_price': row_position['current_price_actual'],
                'squareoff_price': row_position['current_price'],
                'squareoff_price_actual': row_position['current_price_actual'],
                'broker_order_id_squareoff' : row_position['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row_position['broker_order_status_squareoff'],
                'updated_date': current_date,
                'realized_pnl_txn': row_position['realized_pnl_txn'],
                'realized_pnl_overall': row_position['realized_pnl_overall'],
                'realized_pnl_options': row_position['realized_pnl_options'],
                'realized_pnl_futures': row_position['realized_pnl_futures'],
                'quantity' : row_position['quantity'],
                'multi_factor': row_position['multi_factor']
                })
    session.commit()
    session.close()

def updatePositionsMergeValues(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.contract_expiry_date == row_position['contract_expiry_date'],
                                              PositionsTrackingBackUpTable.strike_price_put == row_position['strike_price_put'],
                                              PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'execution_price_call': row_position['execution_price_call'],
                'execution_price_call_actual': row_position['execution_price_call_actual'],
                'execution_price_put': row_position['execution_price_put'],
                'execution_price_put_actual': row_position['execution_price_put_actual'],
                'execution_price_futures': row_position['execution_price_futures'],
                'execution_price_futures_actual': row_position['execution_price_futures_actual'],
                'quantity': row_position['quantity'],
                'multi_factor': row_position['multi_factor'],
                'margin_txn': row_position['margin_txn'],
                'execution_params': row_position['execution_params'],
                'updated_date': current_date,
                 })
    session.commit()
    session.close()

def updatePositionsPnlPostSquareOffJob(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.oops_order_id == row_position['oops_order_id']).\
        update({'unrealized_pnl_overall': row_position['unrealized_pnl_overall'],
                'realized_pnl_overall': row_position['realized_pnl_overall'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobAll(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.is_active == 'Y'). \
        update({'unrealized_pnl_overall': row_position['unrealized_pnl_overall'],
                'realized_pnl_overall': row_position['realized_pnl_overall'],
                'realized_pnl_options': row_position['realized_pnl_options'],
                'realized_pnl_futures': row_position['realized_pnl_futures'],
                'current_net_delta_overall': row_position['current_net_delta_overall'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsPnlSquareOffException(schema_name, position_group_id, symbol, is_active, net_pnl, brokerage, squareoff_params):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                              PositionsTrackingBackUpTable.symbol == symbol,
                                              PositionsTrackingBackUpTable.is_active == 'Y'). \
        update({'realized_pnl_overall_brokerage': net_pnl,
                'realized_pnl_overall': net_pnl,
                'is_active': is_active,
                'brokerage': brokerage,
                'updated_date': current_date,
                'squareoff_params': squareoff_params
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobAllMarginFreeUp(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.is_active == 'Y'). \
        update({'realized_pnl_overall': row_position['realized_pnl_overall'],
                'realized_pnl_options': row_position['realized_pnl_options'],
                'realized_pnl_futures': row_position['realized_pnl_futures'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsDeltaAll(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.is_active == 'Y'). \
        update({'current_net_delta_overall': row_position['current_net_delta_overall'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsRealizedPnlOverall(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.is_active == 'Y'). \
        update({'realized_pnl_overall': row_position['realized_pnl_overall'],
                'realized_pnl_options': row_position['realized_pnl_options'],
                'realized_pnl_futures': row_position['realized_pnl_futures'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsPostFreshAddition(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.strike_price == row_position['strike_price'],
                                              PositionsTrackingBackUpTable.instrument_type == row_position['instrument_type'],
                                              PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'execution_price': row_position['execution_price'],
                'execution_price_actual': row_position['execution_price_actual'],
                'broker_order_id_fresh' : row_position['broker_order_id_fresh'],
                'broker_order_status_fresh' : row_position['broker_order_status_fresh'],
                'updated_date': current_date,
                'margin_txn' : row_position['margin_txn'],
                'quantity' : row_position['quantity'],
                'multi_factor': row_position['multi_factor'],
                'is_active': row_position['is_active'],
                'current_net_delta': row_position['current_net_delta'],
                'current_net_delta_overall': row_position['current_net_delta_overall'],
                'order_type': row_position['order_type'],
                'execution_params' :row_position['execution_params']
                })
    session.commit()
    session.close()

def updatePositionsExistingOptions(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                              PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                              PositionsTrackingBackUpTable.strike_price == row_position['strike_price'],
                                              PositionsTrackingBackUpTable.instrument_type == row_position['instrument_type'],
                                              PositionsTrackingBackUpTable.order_type == row_position['order_type'],
                                              PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'execution_price': row_position['execution_price'],
                'execution_price_actual': row_position['execution_price_actual'],
                'broker_order_id_fresh' : row_position['broker_order_id_fresh'],
                'broker_order_status_fresh' : row_position['broker_order_status_fresh'],
                'updated_date': current_date,
                'margin_txn' : row_position['margin_txn'],
                'quantity' : row_position['quantity'],
                'multi_factor': row_position['multi_factor'],
                'is_active': row_position['is_active'],
                'current_net_delta': row_position['current_net_delta'],
                'current_net_delta_overall': row_position['current_net_delta_overall'],
                'order_type': row_position['order_type'],
                'execution_params' :row_position['execution_params']
                })
    session.commit()
    session.close()

def updatePositionsExistingOrderGenJob(schema_name, row_position, symbol):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == row_position['position_group_id'],
                                                  PositionsTrackingBackUpTable.symbol == row_position['symbol'],
                                                  PositionsTrackingBackUpTable.symbol == symbol,
                                                  PositionsTrackingBackUpTable.ia_options_pair_id == row_position['ia_options_pair_id'],
                                                  PositionsTrackingBackUpTable.is_active == 'Y').\
        update({'execution_price_call': row_position['execution_price_call'],
                'execution_price_call_actual': row_position['execution_price_call_actual'],
                'execution_price_put' : row_position['execution_price_put'],
                'execution_price_put_actual': row_position['execution_price_put_actual'],
                'updated_date': current_date,
                'premium_paid_call': row_position['premium_paid_call'],
                'premium_paid_put': row_position['premium_paid_put'],
                'premium_paid_index': row_position['premium_paid_index'],
                'premium_paid_overall': row_position['premium_paid_overall'],
                'multi_factor_ia': row_position['multi_factor_ia'],
                'quantity_call' : row_position['quantity_call'],
                'quantity_put' : row_position['quantity_put']
               })
    session.commit()
    session.close()

def updatePositionsQtyMF(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on oops_positions_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.oops_positions_id == row_position['oops_positions_id']).\
        update({'updated_date': current_date,
                'quantity' : row_position['quantity'],
                'multi_factor': row_position['multi_factor']
                })
    session.commit()
    session.close()

def updatePositionsMarkInactive(schema_name, row_position):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.oops_positions_id == row_position['oops_positions_id']).\
        update({'updated_date': current_date,
                'is_active': row_position['is_active']
                })
    session.commit()
    session.close()

def deletePositionsByTransIdAndSymbol(schema_name, position_group_id, symbol):
    '''
    Delete from Positions Table using TransId
    :param symbol:
    :param frequency:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id,
                                                  PositionsTrackingBackUpTable.symbol == symbol).delete()

    session.commit()
    session.close()


def deletePositionsByTransId(schema_name, position_group_id):
    '''
    Delete from Positions Table using TransId
    :param symbol:
    :param frequency:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.position_group_id == position_group_id).delete()

    session.commit()
    session.close()

def deletePositionsByPositionIds(schema_name, position_id_list):
    '''
    Delete from Positions Table using TransId
    :param symbol:
    :param frequency:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    for position_id in position_id_list:
        session.query(PositionsTrackingBackUpTable).filter(PositionsTrackingBackUpTable.oops_positions_id == position_id).delete()

    session.commit()
    session.close()

def deletePositionsTrackingBackUpAll(schema_name):
    '''
    Delete from Positions Table using TransId
    :param symbol:
    :param frequency:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    session.query(PositionsTrackingBackUpTable).delete()

    session.commit()
    session.close()