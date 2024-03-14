import pandas as pd
import backtrader as bt
from adaptor.BollingerBreakout import BollingerBreakout
import logging

def fetchTechnicalIndicators(orderGen_dict, Configuration):
    df_historical_data = orderGen_dict['df_historical_data']
    LAST_NUM_CANDLES = int(Configuration['LAST_NUM_CANDLES'])
    df_historical_data = df_historical_data.tail(LAST_NUM_CANDLES)
    df_historical_data.insert(0, 'datetime', pd.to_datetime(df_historical_data['date']))
    df_historical_data.drop(columns=['date'])
    df_historical_data = df_historical_data.set_index('datetime')

    ############################################ FETCH BOLLINGER BANDWIDTH #############################################
    # MOCK DATA
    #orderGen_dict['BBW'] = 0.40

    cerebro = bt.Cerebro()
    cerebro.addstrategy(BollingerBreakout)
    data = bt.feeds.PandasData(dataname=df_historical_data)
    cerebro.adddata(data)
    strategies = cerebro.run()
    orderGen_dict['BBW'] = round(strategies[0].boll.lines.bbbandwidth[0]*100,2)
    logging.info("{}: BBW ::::::::::::::::::: {}".format(Configuration['SCHEMA_NAME'], orderGen_dict['BBW']))

    return orderGen_dict
