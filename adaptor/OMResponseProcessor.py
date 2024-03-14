import pandas as pd
import logging
from common import constants
from datetime import datetime
import json
import pytz

def transformOMResponse(Configuration, df_Put_Call_Order, df_Order, isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze):

    if isFresh:
        df_Put_Call_Order = transformResponseFresh(df_Put_Call_Order, df_Order)
    elif isHedging:
        df_Put_Call_Order = transformResponseHedged(df_Put_Call_Order, df_Order)
    elif isSquareOff and not isQtyFreeze:
        df_Put_Call_Order = transformResponseSquareoff(df_Put_Call_Order, df_Order)
    elif isMarginFreeUp:
        df_Put_Call_Order = transformResponseMarginFreeUp(df_Put_Call_Order, df_Order)
    elif isSquareOff and isQtyFreeze:
        df_Put_Call_Order = transformResponseSquareoffQtyFreeze(df_Put_Call_Order, df_Order)
    else:
        raise AttributeError("Need Manual Intervention :: Order can't be processed as all 3 attributes: "
                             "isFresh/isHedging/isSquareOff are False !!")

    return df_Put_Call_Order

def transformResponseFresh(df_Put_Call_Order, df_Order):
    # MERGE DF - OM DF AND INITIAL DF
    df_Order_Temp = df_Order[['TRADING_SYMBOL', 'average_price', 'status', 'zorderid']]
    df_Put_Call_Order = pd.merge(df_Put_Call_Order, df_Order_Temp, on='TRADING_SYMBOL', how='inner')

    if len(df_Put_Call_Order) == 0:
        raise ValueError("TRADING_SYMBOL mismatch between INITIAL DF AND OM DF. Check logs.")

    ############################################ UPDATE VALUES IN DF ###################################################
    for index, row in df_Put_Call_Order.iterrows():
        df_Put_Call_Order.at[index, 'ENTRY_PRICE_'+row['INSTRUMENT_TYPE']] = row['average_price']
        df_Put_Call_Order.at[index, 'broker_order_id_fresh_' + row['INSTRUMENT_TYPE']] = str(row['zorderid'])
        df_Put_Call_Order.at[index, 'broker_order_status_fresh_' + row['INSTRUMENT_TYPE']] = str(row['status'])
    ####################################################################################################################

    df_Put_Call_Order['broker_order_id_squareoff_put'] = "NA"
    df_Put_Call_Order['broker_order_id_squareoff_call'] = "NA"
    df_Put_Call_Order['broker_order_status_squareoff_put'] = "NA"
    df_Put_Call_Order['broker_order_status_squareoff_call'] = "NA"

    return df_Put_Call_Order

def transformResponseHedged(df_Put_Call_Order, df_Order):
    # MERGE DF - OM DF AND INITIAL DF
    df_Order_Temp = df_Order[['TRADING_SYMBOL', 'average_price', 'status', 'zorderid']]
    df_Put_Call_Order = pd.merge(df_Put_Call_Order, df_Order_Temp, on='TRADING_SYMBOL', how='inner')

    if len(df_Put_Call_Order) == 0:
        raise ValueError("TRADING_SYMBOL mismatch between INITIAL DF AND OM DF. Check logs.")

    ############################################ UPDATE VALUES IN DF ###################################################
    duplicate_cols = df_Put_Call_Order.columns[df_Put_Call_Order.columns.duplicated()]
    df_Put_Call_Order.drop(columns=duplicate_cols, inplace=True)
    for index, row in df_Put_Call_Order.iterrows():
        df_Put_Call_Order.at[index, 'ENTRY_PRICE'] = row['average_price']
        df_Put_Call_Order.at[index, 'CURRENT_PRICE'] = row['average_price']
        df_Put_Call_Order.at[index, 'BROKER_ORDER_ID'] = str(row['zorderid'])
        df_Put_Call_Order.at[index, 'BROKER_ORDER_STATUS'] = str(row['status'])
    ####################################################################################################################

    return df_Put_Call_Order

def transformResponseSquareoff(df_Put_Call_Order, df_Order):

    # MERGE DF - OM DF AND INITIAL DF
    df_Order_Temp = df_Order[['TRADING_SYMBOL', 'average_price','status','zorderid']]
    df_Put_Call_Order = pd.merge(df_Put_Call_Order, df_Order_Temp, on='TRADING_SYMBOL', how='inner')


    if len(df_Put_Call_Order) == 0:
        raise ValueError("TRADING_SYMBOL mismatch between INITIAL DF AND OM DF. Check logs.")

    ############################################ UPDATE VALUES IN DF ###################################################
    for index, row in df_Put_Call_Order.iterrows():
        df_Put_Call_Order.at[index, 'CURRENT_PRICE'] = row['average_price']
        df_Put_Call_Order.at[index, 'BROKER_ORDER_ID'] = str(row['zorderid'])
        df_Put_Call_Order.at[index, 'BROKER_ORDER_STATUS'] = str(row['status'])
    ####################################################################################################################

    return df_Put_Call_Order

def transformResponseSquareoffQtyFreeze(df_Put_Call_Order, df_Order):

    # MERGE DF - OM DF AND INITIAL DF
    df_Order_Temp = df_Order[['TRADING_SYMBOL', 'TAG','average_price','status','zorderid']]
    df_Put_Call_Order = pd.merge(df_Put_Call_Order, df_Order_Temp, on='TAG', how='inner')

    if len(df_Put_Call_Order) == 0:
        raise ValueError("TRADING_SYMBOL mismatch between INITIAL DF AND OM DF. Check logs.")

    ############################################ UPDATE VALUES IN DF ###################################################
    for index, row in df_Put_Call_Order.iterrows():

        df_Put_Call_Order.at[index, 'CURRENT_PRICE'] = row['average_price']
        df_Put_Call_Order.at[index, 'BROKER_ORDER_ID_SQUAREOFF'] = str(row['zorderid'])
        df_Put_Call_Order.at[index, 'BROKER_ORDER_STATUS_SQUAREOFF'] = str(row['status'])
        df_Put_Call_Order.at[index, 'BROKER_ORDER_ID_FRESH'] = "NA"
        df_Put_Call_Order.at[index, 'BROKER_ORDER_STATUS_FRESH'] = "NA"
    ####################################################################################################################

    return df_Put_Call_Order

def transformResponseMarginFreeUp(df_Put_Call_Order, df_Order):

    # MERGE DF - OM DF AND INITIAL DF
    df_Order_Temp = df_Order[['TRADING_SYMBOL', 'average_price','status','zorderid']]
    df_Put_Call_Order = pd.merge(df_Put_Call_Order, df_Order_Temp, on='TRADING_SYMBOL', how='inner')


    if len(df_Put_Call_Order) == 0:
        raise ValueError("TRADING_SYMBOL mismatch between INITIAL DF AND OM DF. Check logs.")

    ############################################ UPDATE VALUES IN DF ###################################################
    for index, row in df_Put_Call_Order.iterrows():

        df_Put_Call_Order.at[index, 'CURRENT_PRICE_'+row['INSTRUMENT_TYPE']] = row['average_price']
        df_Put_Call_Order.at[index, 'broker_order_id_fresh_'+row['INSTRUMENT_TYPE']] = str(row['zorderid'])
        df_Put_Call_Order.at[index, 'broker_order_status_fresh_'+row['INSTRUMENT_TYPE']] = str(row['status'])
    ####################################################################################################################

    df_Put_Call_Order['broker_order_id_squareoff_put'] = "NA"
    df_Put_Call_Order['broker_order_id_squareoff_call'] = "NA"
    df_Put_Call_Order['broker_order_status_squareoff_put'] = "NA"
    df_Put_Call_Order['broker_order_status_squareoff_call'] = "NA"

    return df_Put_Call_Order