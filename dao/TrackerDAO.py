
from entities.TrackerTable import TrackerTable
from config.database.Sqlalchemy import Sqlalchemy
import logging
import pandas as pd

def insertOrUpdateTrackerPositions(schema_name, row_tracking_position, symbol):
    '''
    Insert Tracker Positions
    :return:
    '''

    session = Sqlalchemy(schema_name)
    tracker_row = session.query(TrackerTable).filter(TrackerTable.symbol == symbol).first()
    if not tracker_row:
        trackingPosition = TrackerTable(account=row_tracking_position['ACCOUNT'],
                                        symbol=row_tracking_position['SYMBOL'],
                                        qty_sell=row_tracking_position['QTY_SELL'],
                                        sp_sell=row_tracking_position['SP_SELL'],
                                        current_underlying=row_tracking_position['CURRENT_UNDERLYING'],
                                        net_pnl=row_tracking_position['NET_PNL'],
                                        net_pnl_pct=row_tracking_position['NET_PNL_PCT'],
                                        margin_used=row_tracking_position['MARGIN_USED'],
                                        positions_matched=row_tracking_position['POSITIONS_MATCHED'],
                                        positions_closed=row_tracking_position['POSITIONS_CLOSED'],
                                        positions_count_db=row_tracking_position['POSITIONS_COUNT_DB'],
                                        positions_count_broker=row_tracking_position['POSITIONS_COUNT_BROKER'],
                                        qty_count_db=row_tracking_position['QTY_COUNT_DB'],
                                        qty_count_broker=row_tracking_position['QTY_COUNT_BROKER'],
                                        rag=row_tracking_position['RAG'],
                                        refresh_interval=row_tracking_position['REFRESH_INTERVAL'],
                                        last_updated_time=row_tracking_position['LAST_UPDATED_TIME'],
                                        margin_used_broker=row_tracking_position['MARGIN_USED_BROKER'],
                                        execution_iv=row_tracking_position['EXECUTION_IV'],
                                        current_iv=row_tracking_position['CURRENT_IV'],
                                        target=row_tracking_position['TARGET'],
                                        stoploss=row_tracking_position['STOPLOSS'],
                                        params=row_tracking_position['PARAMS']
                                        )
        session.add(trackingPosition)
    else:
        session.query(TrackerTable).filter(TrackerTable.symbol == symbol). \
            update({'account':row_tracking_position['ACCOUNT'],
                    'symbol':row_tracking_position['SYMBOL'],
                    'qty_sell':row_tracking_position['QTY_SELL'],
                    'sp_sell':row_tracking_position['SP_SELL'],
                    'current_underlying':row_tracking_position['CURRENT_UNDERLYING'],
                    'net_pnl':row_tracking_position['NET_PNL'],
                    'net_pnl_pct':row_tracking_position['NET_PNL_PCT'],
                    'margin_used':row_tracking_position['MARGIN_USED'],
                    'positions_matched':row_tracking_position['POSITIONS_MATCHED'],
                    'positions_closed':row_tracking_position['POSITIONS_CLOSED'],
                    'positions_count_db':row_tracking_position['POSITIONS_COUNT_DB'],
                    'positions_count_broker':row_tracking_position['POSITIONS_COUNT_BROKER'],
                    'qty_count_db':row_tracking_position['QTY_COUNT_DB'],
                    'qty_count_broker':row_tracking_position['QTY_COUNT_BROKER'],
                    'rag':row_tracking_position['RAG'],
                    'refresh_interval':row_tracking_position['REFRESH_INTERVAL'],
                    'last_updated_time':row_tracking_position['LAST_UPDATED_TIME'],
                    'margin_used_broker': row_tracking_position['MARGIN_USED_BROKER'],
                    'execution_iv': row_tracking_position['EXECUTION_IV'],
                    'current_iv': row_tracking_position['CURRENT_IV'],
                    'target': row_tracking_position['TARGET'],
                    'stoploss': row_tracking_position['STOPLOSS'],
                    'params': row_tracking_position['PARAMS']
                    })


    session.commit()
    session.close()

def insertOrUpdateTrackerPositionsWithoutTime(schema_name, row_tracking_position, symbol):
    '''
    Insert Tracker Positions
    :return:
    '''

    session = Sqlalchemy(schema_name)
    tracker_row = session.query(TrackerTable).filter(TrackerTable.symbol == symbol).first()
    if not tracker_row:
        trackingPosition = TrackerTable(account=row_tracking_position['ACCOUNT'],
                                        symbol=row_tracking_position['SYMBOL'],
                                        qty_sell=row_tracking_position['QTY_SELL'],
                                        sp_sell=row_tracking_position['SP_SELL'],
                                        current_underlying=row_tracking_position['CURRENT_UNDERLYING'],
                                        net_pnl=row_tracking_position['NET_PNL'],
                                        net_pnl_pct=row_tracking_position['NET_PNL_PCT'],
                                        margin_used=row_tracking_position['MARGIN_USED'],
                                        positions_matched=row_tracking_position['POSITIONS_MATCHED'],
                                        positions_closed=row_tracking_position['POSITIONS_CLOSED'],
                                        positions_count_db=row_tracking_position['POSITIONS_COUNT_DB'],
                                        positions_count_broker=row_tracking_position['POSITIONS_COUNT_BROKER'],
                                        qty_count_db=row_tracking_position['QTY_COUNT_DB'],
                                        qty_count_broker=row_tracking_position['QTY_COUNT_BROKER'],
                                        rag=row_tracking_position['RAG'],
                                        refresh_interval=row_tracking_position['REFRESH_INTERVAL'],
                                        margin_used_broker=row_tracking_position['MARGIN_USED_BROKER'],
                                        execution_iv=row_tracking_position['EXECUTION_IV'],
                                        current_iv=row_tracking_position['CURRENT_IV'],
                                        target=row_tracking_position['TARGET'],
                                        stoploss=row_tracking_position['STOPLOSS'],
                                        params=row_tracking_position['PARAMS'])
        session.add(trackingPosition)
    else:
        session.query(TrackerTable).filter(TrackerTable.symbol == symbol). \
            update({'account':row_tracking_position['ACCOUNT'],
                    'symbol':row_tracking_position['SYMBOL'],
                    'qty_sell':row_tracking_position['QTY_SELL'],
                    'sp_sell': row_tracking_position['SP_SELL'],
                    'current_underlying': row_tracking_position['CURRENT_UNDERLYING'],
                    'net_pnl':row_tracking_position['NET_PNL'],
                    'net_pnl_pct':row_tracking_position['NET_PNL_PCT'],
                    'margin_used':row_tracking_position['MARGIN_USED'],
                    'positions_matched':row_tracking_position['POSITIONS_MATCHED'],
                    'positions_closed':row_tracking_position['POSITIONS_CLOSED'],
                    'positions_count_db':row_tracking_position['POSITIONS_COUNT_DB'],
                    'positions_count_broker':row_tracking_position['POSITIONS_COUNT_BROKER'],
                    'qty_count_db':row_tracking_position['QTY_COUNT_DB'],
                    'qty_count_broker':row_tracking_position['QTY_COUNT_BROKER'],
                    'rag':row_tracking_position['RAG'],
                    'refresh_interval':row_tracking_position['REFRESH_INTERVAL'],
                    'margin_used_broker': row_tracking_position['MARGIN_USED_BROKER'],
                    'execution_iv': row_tracking_position['EXECUTION_IV'],
                    'current_iv': row_tracking_position['CURRENT_IV'],
                    'target': row_tracking_position['TARGET'],
                    'stoploss': row_tracking_position['STOPLOSS'],
                    'params': row_tracking_position['PARAMS']
                    })


    session.commit()
    session.close()

def updateTrackerPostSquareOffException(schema_name, row_tracking_position, symbol):
    '''
    Insert Tracker Positions
    :return:
    '''

    session = Sqlalchemy(schema_name)
    session.query(TrackerTable).filter(TrackerTable.symbol == symbol). \
        update({'qty_count_broker':row_tracking_position['QTY_COUNT_BROKER'],
                'positions_count_broker':row_tracking_position['POSITIONS_COUNT_BROKER'],
                'qty_count_db': row_tracking_position['QTY_COUNT_DB'],
                'positions_count_db': row_tracking_position['POSITIONS_COUNT_DB'],
                'positions_closed': row_tracking_position['POSITIONS_CLOSED'],
                'positions_matched': row_tracking_position['POSITIONS_MATCHED'],
                'rag': row_tracking_position['RAG'],
                'net_pnl': row_tracking_position['NET_PNL'],
                'net_pnl_pct': row_tracking_position['NET_PNL_PCT'],
                'last_updated_time': row_tracking_position['LAST_UPDATED_TIME']
                })


    session.commit()
    session.close()

def getTrackerPositions(schema_name):
    '''
    GET TRACKER POSITIONS
    :return:
    '''
    session = Sqlalchemy(schema_name)
    tracker_result = session.query(TrackerTable).all()
    df_tracker_result = pd.read_sql(tracker_result.statement, session.bind)
    session.close()
    return df_tracker_result

def deleteTrackerPositions(schema_name):
    '''
    Delete Tracker Positions
    :return:
    '''
    session = Sqlalchemy(schema_name)

    session.query(TrackerTable).delete()
    session.commit()
    session.close()
    print("Deleted Tracker Positions.")
    logging.info("Deleted Tracker Positions.")

def updateTrackerPostStoplossTargetUpdate(schema_name, row_tracking_position, symbol):
    '''
    Insert Tracker Positions
    :return:
    '''

    session = Sqlalchemy(schema_name)
    session.query(TrackerTable).filter(TrackerTable.symbol == symbol). \
        update({'target': row_tracking_position['TARGET'],
                'stoploss': row_tracking_position['STOPLOSS'],
                'last_updated_time': row_tracking_position['LAST_UPDATED_TIME']
                })


    session.commit()
    session.close()


if __name__ == "__main__":

    # Insert in Configuration table
    #insertConfigurations()

    # getConfigurations()
    print(getTrackerPositions())
