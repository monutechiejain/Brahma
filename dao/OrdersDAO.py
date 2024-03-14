
from config.database.Sqlalchemy import Sqlalchemy
from entities.OrdersTable import OrdersTable
from datetime import datetime
import pytz
import pandas as pd



def insert(row, schema_name):
    '''
    Insert Positions in Positions Table
    Get Trans ID/Order Id Sequence Next Value and Populate in Positions Table
    :return:
    '''
    session = Sqlalchemy(schema_name)

    positions = OrdersTable(order_id = row['order_id'],
                                order_group_id=row['order_group_id'],
                                position_id = row['position_id'],
                                position_group_id = row['position_group_id'],
                                broker_order_id = row['broker_order_id'],
                                broker_order_status = row['broker_order_status'],
                                symbol = row['symbol'],
                                expiry_date = row['expiry_date'],
                                strike_price = row['strike_price'],
                                instrument_type = row['instrument_type'],
                                transaction_type = row['transaction_type'],
                                order_type = row['order_type'],
                                contract_type=row['contract_type'],
                                num_lots = row['num_lots'],
                                lot_size = row['lot_size'],
                                quantity = row['quantity'],
                                is_success = row['is_success'],
                                margin_overall = row['margin_overall'],
                                execution_price = row['execution_price'],
                                params = row['params'])
    session.add(positions)
    session.commit()
    session.close()


def getActivePositionsByTransIdAndSymbol(schema_name, position_group_id, symbol):
    '''
    Get Positions By Trans ID
    :param position_group_id:
    :return:
    '''
    session = Sqlalchemy(schema_name)
    ia_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                  OrdersTable.symbol == symbol,
                                                                  OrdersTable.is_active == 'Y')
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
    ia_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                  OrdersTable.symbol == symbol,
                                                                  OrdersTable.order_type == orderType,
                                                                  OrdersTable.is_active == 'Y')
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
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                  OrdersTable.symbol == symbol,
                                                                  OrdersTable.contract_expiry_date == expiryDate,
                                                                  OrdersTable.is_active == 'Y')
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
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                  OrdersTable.is_active == 'Y')
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
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id)
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
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.is_active == 'N')
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
    positions_result = session.query(OrdersTable)
    df_positions_result = pd.read_sql(positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getNonActivePositionsByTransIdAndInstrumentType(schema_name, position_group_id, instrument_type):
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                   OrdersTable.instrument_type == instrument_type,
                                                                   OrdersTable.is_active == 'N')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getNonActivePositionsByTransIdSymbolAndInstrumentType(schema_name, position_group_id, symbol, instrument_type):
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                   OrdersTable.instrument_type == instrument_type,
                                                                   OrdersTable.symbol == symbol,
                                                                   OrdersTable.is_active == 'N')
    df_positions_result = pd.read_sql(oops_positions_result.statement, session.bind)
    session.close()
    return df_positions_result

def getActivePositionsByTransIdSymbolAndInstrumentType(schema_name, position_group_id, symbol, instrument_type):
    session = Sqlalchemy(schema_name)
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                   OrdersTable.instrument_type == instrument_type,
                                                                   OrdersTable.symbol == symbol,
                                                                   OrdersTable.is_active == 'Y')
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
    oops_positions_result = session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                                  OrdersTable.symbol == symbol)
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
    ia_positions_result = session.query(OrdersTable).filter(OrdersTable.symbol == symbol,
                                                                  OrdersTable.is_active == 'Y')
    df_positions_result = pd.read_sql(ia_positions_result.statement, session.bind)
    session.close()
    return df_positions_result


def updatePositionsPostSquareOffJob(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.strike_price == row['strike_price'],
                                              OrdersTable.instrument_type == row['instrument_type'],
                                              OrdersTable.contract_expiry_date == row['contract_expiry_date'],
                                              OrdersTable.is_active == 'Y').\
        update({'current_price': row['current_price_actual'],
                'squareoff_price': row['current_price'],
                'squareoff_price_actual': row['current_price_actual'],
                'broker_order_id_squareoff' : row['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row['broker_order_status_squareoff'],
                'updated_date': current_date,
                'unrealized_pnl': row['unrealized_pnl'],
                'realized_pnl': row['realized_pnl'],
                'unrealized_pnl_txn': row['unrealized_pnl_txn'],
                'realized_pnl_txn': row['realized_pnl_txn'],
                'realized_pnl_overall': row['realized_pnl_overall'],
                'realized_pnl_futures': row['realized_pnl_futures'],
                'realized_pnl_options': row['realized_pnl_options'],
                'unrealized_pnl_overall': row['unrealized_pnl_overall'],
                'net_pnl_overall': row['net_pnl_overall'],
                'margin_txn' : row['margin_txn'],
                'quantity' : row['quantity'],
                'multi_factor': row['multi_factor'],
                'is_active': row['is_active'],
                'current_net_delta': row['current_net_delta'],
                'current_net_delta_overall': row['current_net_delta_overall'],
                'is_trailing_active': row['is_trailing_active'],
                'trailing_pnl': row['trailing_pnl'],
                'brokerage': row['brokerage'],
                'realized_pnl_overall_brokerage': row['realized_pnl_overall_brokerage'],
                'expected_theta_pnl_pending': row['expected_theta_pnl_pending'],
                'squareoff_params': row['squareoff_params']
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobWithOrderType(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.strike_price == row['strike_price'],
                                              OrdersTable.instrument_type == row['instrument_type'],
                                              OrdersTable.is_active == 'Y').\
        update({'current_price': row['current_price_actual'],
                'squareoff_price': row['current_price'],
                'squareoff_price_actual': row['current_price_actual'],
                'broker_order_id_squareoff' : row['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row['broker_order_status_squareoff'],
                'updated_date': current_date,
                'unrealized_pnl': row['unrealized_pnl'],
                'realized_pnl': row['realized_pnl'],
                'unrealized_pnl_txn': row['unrealized_pnl_txn'],
                'realized_pnl_txn': row['realized_pnl_txn'],
                'realized_pnl_overall': row['realized_pnl_overall'],
                'realized_pnl_options': row['realized_pnl_options'],
                'realized_pnl_futures': row['realized_pnl_futures'],
                'unrealized_pnl_overall': row['unrealized_pnl_overall'],
                'margin_txn' : row['margin_txn'],
                'quantity' : row['quantity'],
                'multi_factor': row['multi_factor'],
                'is_active': row['is_active'],
                'current_net_delta': row['current_net_delta'],
                'current_net_delta_overall': row['current_net_delta_overall'],
                'execution_params': row['execution_params'],
                'order_type': row['order_type']
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobOptions(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.strike_price == row['strike_price'],
                                              OrdersTable.instrument_type == row['instrument_type'],
                                              OrdersTable.is_active == 'Y').\
        update({'current_price': row['current_price_actual'],
                'squareoff_price': row['current_price'],
                'squareoff_price_actual': row['current_price_actual'],
                'broker_order_id_squareoff' : row['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row['broker_order_status_squareoff'],
                'updated_date': current_date,
                'unrealized_pnl': row['unrealized_pnl'],
                'realized_pnl': row['realized_pnl'],
                'unrealized_pnl_txn': row['unrealized_pnl_txn'],
                'realized_pnl_txn': row['realized_pnl_txn'],
                'realized_pnl_overall': row['realized_pnl_overall'],
                'realized_pnl_options': row['realized_pnl_options'],
                'realized_pnl_futures': row['realized_pnl_futures'],
                'unrealized_pnl_overall': row['unrealized_pnl_overall'],
                'quantity' : row['quantity'],
                'multi_factor': row['multi_factor'],
                'is_active': row['is_active'],
                'current_net_delta': row['current_net_delta'],
                'current_net_delta_overall': row['current_net_delta_overall'],
                'execution_params': row['execution_params']
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobOptionsMarginFreeUp(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.strike_price == row['strike_price'],
                                              OrdersTable.instrument_type == row['instrument_type'],
                                              OrdersTable.is_active == 'Y').\
        update({'current_price': row['current_price_actual'],
                'squareoff_price': row['current_price'],
                'squareoff_price_actual': row['current_price_actual'],
                'broker_order_id_squareoff' : row['broker_order_id_squareoff'],
                'broker_order_status_squareoff' : row['broker_order_status_squareoff'],
                'updated_date': current_date,
                'realized_pnl_txn': row['realized_pnl_txn'],
                'realized_pnl_overall': row['realized_pnl_overall'],
                'realized_pnl_options': row['realized_pnl_options'],
                'realized_pnl_futures': row['realized_pnl_futures'],
                'quantity' : row['quantity'],
                'multi_factor': row['multi_factor']
                })
    session.commit()
    session.close()

def updatePositionsMergeValues(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.contract_expiry_date == row['contract_expiry_date'],
                                              OrdersTable.strike_price_put == row['strike_price_put'],
                                              OrdersTable.is_active == 'Y').\
        update({'execution_price_call': row['execution_price_call'],
                'execution_price_call_actual': row['execution_price_call_actual'],
                'execution_price_put': row['execution_price_put'],
                'execution_price_put_actual': row['execution_price_put_actual'],
                'execution_price_futures': row['execution_price_futures'],
                'execution_price_futures_actual': row['execution_price_futures_actual'],
                'quantity': row['quantity'],
                'multi_factor': row['multi_factor'],
                'margin_txn': row['margin_txn'],
                'execution_params': row['execution_params'],
                'updated_date': current_date,
                 })
    session.commit()
    session.close()

def updatePositionsPnlPostSquareOffJob(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.oops_order_id == row['oops_order_id']).\
        update({'unrealized_pnl_overall': row['unrealized_pnl_overall'],
                'realized_pnl_overall': row['realized_pnl_overall'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobAll(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.is_active == 'Y'). \
        update({'unrealized_pnl_overall': row['unrealized_pnl_overall'],
                'realized_pnl_overall': row['realized_pnl_overall'],
                'realized_pnl_options': row['realized_pnl_options'],
                'realized_pnl_futures': row['realized_pnl_futures'],
                'current_net_delta_overall': row['current_net_delta_overall'],
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
    session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                              OrdersTable.symbol == symbol,
                                              OrdersTable.is_active == 'Y'). \
        update({'realized_pnl_overall_brokerage': net_pnl,
                'realized_pnl_overall': net_pnl,
                'is_active': is_active,
                'brokerage': brokerage,
                'updated_date': current_date,
                'squareoff_params': squareoff_params
                })
    session.commit()
    session.close()

def updatePositionsPostSquareOffJobAllMarginFreeUp(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.is_active == 'Y'). \
        update({'realized_pnl_overall': row['realized_pnl_overall'],
                'realized_pnl_options': row['realized_pnl_options'],
                'realized_pnl_futures': row['realized_pnl_futures'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsDeltaAll(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.is_active == 'Y'). \
        update({'current_net_delta_overall': row['current_net_delta_overall'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsRealizedPnlOverall(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.is_active == 'Y'). \
        update({'realized_pnl_overall': row['realized_pnl_overall'],
                'realized_pnl_options': row['realized_pnl_options'],
                'realized_pnl_futures': row['realized_pnl_futures'],
                'updated_date': current_date
                })
    session.commit()
    session.close()

def updatePositionsPostFreshAddition(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.strike_price == row['strike_price'],
                                              OrdersTable.instrument_type == row['instrument_type'],
                                              OrdersTable.is_active == 'Y').\
        update({'execution_price': row['execution_price'],
                'execution_price_actual': row['execution_price_actual'],
                'broker_order_id_fresh' : row['broker_order_id_fresh'],
                'broker_order_status_fresh' : row['broker_order_status_fresh'],
                'updated_date': current_date,
                'margin_txn' : row['margin_txn'],
                'quantity' : row['quantity'],
                'multi_factor': row['multi_factor'],
                'is_active': row['is_active'],
                'current_net_delta': row['current_net_delta'],
                'current_net_delta_overall': row['current_net_delta_overall'],
                'order_type': row['order_type'],
                'execution_params' :row['execution_params']
                })
    session.commit()
    session.close()

def updatePositionsExistingOptions(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                              OrdersTable.symbol == row['symbol'],
                                              OrdersTable.strike_price == row['strike_price'],
                                              OrdersTable.instrument_type == row['instrument_type'],
                                              OrdersTable.order_type == row['order_type'],
                                              OrdersTable.is_active == 'Y').\
        update({'execution_price': row['execution_price'],
                'execution_price_actual': row['execution_price_actual'],
                'broker_order_id_fresh' : row['broker_order_id_fresh'],
                'broker_order_status_fresh' : row['broker_order_status_fresh'],
                'updated_date': current_date,
                'margin_txn' : row['margin_txn'],
                'quantity' : row['quantity'],
                'multi_factor': row['multi_factor'],
                'is_active': row['is_active'],
                'current_net_delta': row['current_net_delta'],
                'current_net_delta_overall': row['current_net_delta_overall'],
                'order_type': row['order_type'],
                'execution_params' :row['execution_params']
                })
    session.commit()
    session.close()

def updatePositionsExistingOrderGenJob(schema_name, row, symbol):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.position_group_id == row['position_group_id'],
                                                  OrdersTable.symbol == row['symbol'],
                                                  OrdersTable.symbol == symbol,
                                                  OrdersTable.ia_options_pair_id == row['ia_options_pair_id'],
                                                  OrdersTable.is_active == 'Y').\
        update({'execution_price_call': row['execution_price_call'],
                'execution_price_call_actual': row['execution_price_call_actual'],
                'execution_price_put' : row['execution_price_put'],
                'execution_price_put_actual': row['execution_price_put_actual'],
                'updated_date': current_date,
                'premium_paid_call': row['premium_paid_call'],
                'premium_paid_put': row['premium_paid_put'],
                'premium_paid_index': row['premium_paid_index'],
                'premium_paid_overall': row['premium_paid_overall'],
                'multi_factor_ia': row['multi_factor_ia'],
                'quantity_call' : row['quantity_call'],
                'quantity_put' : row['quantity_put']
               })
    session.commit()
    session.close()

def updatePositionsQtyMF(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on oops_positions_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.oops_positions_id == row['oops_positions_id']).\
        update({'updated_date': current_date,
                'quantity' : row['quantity'],
                'multi_factor': row['multi_factor']
                })
    session.commit()
    session.close()

def updatePositionsMarkInactive(schema_name, row):
    '''
    Update Positions in Positions Table
    UPdate Current Prices by Square Off Job based on position_group_id and symbol
    :return:
    '''
    session = Sqlalchemy(schema_name)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    session.query(OrdersTable).filter(OrdersTable.oops_positions_id == row['oops_positions_id']).\
        update({'updated_date': current_date,
                'is_active': row['is_active']
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
    session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id,
                                                  OrdersTable.symbol == symbol).delete()

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
    session.query(OrdersTable).filter(OrdersTable.position_group_id == position_group_id).delete()

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
        session.query(OrdersTable).filter(OrdersTable.oops_positions_id == position_id).delete()

    session.commit()
    session.close()