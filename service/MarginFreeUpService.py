from helper import OrderGenOptionsHelper, MarginFreeUpHelper
from rules import MarginFreeUpRuleEngine
from entities.Enums import  TransactionTypeEnum
import logging
import pandas as pd

def marginFreeUp(**kwargs):
    Configuration = kwargs['Configuration']
    squareOff_dict = kwargs['squareOff_dict']
    isMarginFreeUpAllowed = False
    squareOff_dict['isMarginFreeUpAllowed'] = isMarginFreeUpAllowed
    squareOff_dict['live_balance'] = 0.0
    squareOff_dict['utilised'] = 0.0

    ########################################## ONLY FOR PROD ###########################################################
    if not (Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']):
        return squareOff_dict


    symbol = kwargs['symbol']
    contract_type = kwargs['contract_type']

    marginFreeUp_dict = {}
    marginFreeUp_dict['TOTAL_INITIAL_MARGIN'] = squareOff_dict['TOTAL_INITIAL_MARGIN']
    marginFreeUp_dict['MF_EXISTING'] = squareOff_dict['MF_EXISTING']
    marginFreeUp_dict['symbol'] = symbol
    position_group_id = kwargs['position_group_id']
    marginFreeUp_dict['position_group_id'] = position_group_id
    marginFreeUp_dict['isDealAllowed'] = False
    LOT_SIZE = float(Configuration['LOT_SIZE_' + symbol])


    spot_value = squareOff_dict['spot_value']
    marginFreeUp_dict['df_positions_tracking'] = squareOff_dict['df_positions_tracking']
    marginFreeUp_dict["spot_value"] = spot_value

    ######################### 1. CHECK BROKER MARGIN IS NEGATIVE OR LESS THAN 1 PCT #########################
    ######################### 2. PARTIAL SQUAREOFF SCENARIO #################################################
    marginFreeUp_dict = MarginFreeUpRuleEngine.decisonMaker(marginFreeUp_dict, Configuration, symbol)
    squareOff_dict['live_balance'] = marginFreeUp_dict['live_balance']
    squareOff_dict['utilised'] = marginFreeUp_dict['utilised']
    isMarginFreeUpAllowed = marginFreeUp_dict['isMarginFreeUpAllowed']
    squareOff_dict['isMarginFreeUpAllowed'] = isMarginFreeUpAllowed

    if not marginFreeUp_dict['isMarginFreeUpAllowed']:
        return squareOff_dict

    ####################################### 2. POPULATE MARGIN FREE UP DATAFRAME ########################################
    squareOff_dict['squareoff_type'] = 'MARGIN_FREE_UP'
    df_Margin_Free_Up = squareOff_dict['df_positions_tracking']
    # ADD SQUAREOFF TO ORDER MANIFEST
    df_Margin_Free_Up['order_manifest'] = df_Margin_Free_Up['order_manifest'] + "_" + squareOff_dict['squareoff_type']

    # POPULATE DETAILS LIKE MF, QTY, NUM_LOTS
    df_Margin_Free_Up = populateAdditionalFields(df_Margin_Free_Up, marginFreeUp_dict, LOT_SIZE, Configuration)
    df_Margin_Free_Up.columns = [x.upper() for x in df_Margin_Free_Up.columns]

    # IF NO POSITIONS LEFT, KINDLY DO MANUAL OVERRIDE
    QUANTITY_PENDING = df_Margin_Free_Up['QUANTITY_PENDING'].iloc[0]
    marginFreeUp_dict['QUANTITY_PENDING'] = QUANTITY_PENDING

    #################################### 3. PLACE DEAL AND CALCULATE PNL ###############################################
    MarginFreeUpHelper.squareoffPositions(df_Margin_Free_Up, marginFreeUp_dict, squareOff_dict, Configuration, symbol, squareOff_dict['expiry_date'], position_group_id,
                                    squareOff_dict['expiry_date_futures'], isPartialSquareOff=True)

    # CHANGING COLUMNS TO LOWERCASE FOR TRACKER UTIL
    df_Margin_Free_Up.columns = [x.lower() for x in df_Margin_Free_Up.columns]
    return squareOff_dict


def populateAdditionalFields(df_Put_Call, marginFreeUp_dict, LOT_SIZE, Configuration):
    # RATIO_SPREAD_MIN_MF_BUY, RATIO_SPREAD_MIN_MF_SELL = [int(i) for i in
    #                                                      Configuration['RATIO_SPREAD_MIN_MF'].split(":")]
    RATIO_SPREAD_MIN_MF_SELL, RATIO_SPREAD_MIN_MF_BUY = 1.0,1.0 # WILL BE CHANGED IF WE ARE MAINATING BUY TO SELL RATIO IN HEDGED POSITIONS
    for index, row in df_Put_Call.iterrows():
        if row['transaction_type'] == TransactionTypeEnum.SELL:
            df_Put_Call.at[index, 'quantity_pending'] = row['quantity'] - float(marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * LOT_SIZE)
            df_Put_Call.at[index, 'num_lots_pending'] = row['num_lots'] - float(marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * RATIO_SPREAD_MIN_MF_SELL)
            df_Put_Call.at[index, 'quantity'] = marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * LOT_SIZE * RATIO_SPREAD_MIN_MF_SELL
            df_Put_Call.at[index, 'num_lots'] = marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * RATIO_SPREAD_MIN_MF_SELL
        else:
            df_Put_Call.at[index, 'quantity_pending'] = row['quantity'] - float(marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * LOT_SIZE * RATIO_SPREAD_MIN_MF_BUY)
            df_Put_Call.at[index, 'num_lots_pending'] = row['num_lots'] - float(marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * RATIO_SPREAD_MIN_MF_BUY)
            df_Put_Call.at[index, 'quantity'] = marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * LOT_SIZE * RATIO_SPREAD_MIN_MF_BUY
            df_Put_Call.at[index, 'num_lots'] = marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] * RATIO_SPREAD_MIN_MF_BUY

    return df_Put_Call


def populateExecutionPricesWAP(row):
    row['EXECUTION_PRICE_CALL'] = row['BID_PRICE_CALL']
    row['EXECUTION_PRICE_CALL_ACTUAL'] = row['BID_PRICE_CALL']
    row['EXECUTION_PRICE_PUT'] = row['BID_PRICE_PUT']
    row['EXECUTION_PRICE_PUT_ACTUAL'] = row['BID_PRICE_PUT']

    row['CURRENT_PRICE_CALL'] = row['ASK_PRICE_CALL']
    row['CURRENT_PRICE_CALL_ACTUAL'] = row['ASK_PRICE_CALL']
    row['CURRENT_PRICE_PUT'] = row['ASK_PRICE_PUT']
    row['CURRENT_PRICE_PUT_ACTUAL'] = row['ASK_PRICE_PUT']

    return row

# def populateAdditionalFields(row, marginFreeUp_dict, LOT_SIZE):
#     row['MARGIN_TXN'] = 0.0
#     row['EXPIRY_DATE'] = marginFreeUp_dict['expiry_date']
#
#     row['MULTI_FACTOR_MARGIN_FREE_UP'] = marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP']
#     row['LOT_SIZE'] = LOT_SIZE
#
#     row['QUANTITY_CALL'] = row['MULTI_FACTOR_MARGIN_FREE_UP'] * float(LOT_SIZE)
#     row['QUANTITY_PUT'] = row['MULTI_FACTOR_MARGIN_FREE_UP'] * float(LOT_SIZE)
#     row['BID_PRICE_PUT'] = OrderGenOptionsHelper.calculateBidAskPriceByMF(row, 'PUT')[0]
#     row['ASK_PRICE_PUT'] = OrderGenOptionsHelper.calculateBidAskPriceByMF(row, 'PUT')[1]
#     row['BID_PRICE_CALL'] = OrderGenOptionsHelper.calculateBidAskPriceByMF(row, 'CALL')[0]
#     row['ASK_PRICE_CALL'] = OrderGenOptionsHelper.calculateBidAskPriceByMF(row, 'CALL')[1]
#
#     return row

def populateBuyDF(SP_BUY, df_Level):
    df_Put_Call = df_Level[df_Level['STRIKE_PRICE'] == float(SP_BUY)]
    df_Put_Call = df_Put_Call.append([df_Put_Call] * 1, ignore_index=True)
    df_Put_Call.reset_index(drop=True, inplace=True)

    # SET ORDER TYPE AND OPTION TYPE
    df_Put_Call['ORDER_TYPE'] = 'BUY'
    df_Put_Call = df_Put_Call.apply(lambda row: setOptionTypeBuy(row), axis=1)

    return df_Put_Call

def setOptionTypeBuy(row):
    row['INSTRUMENT_TYPE'] = None
    row['ORDER_TYPE_PUT'] = 'BUY'
    row['ORDER_TYPE_CALL'] = 'BUY'

    # SET OPTION TYPE BASED ON INDEX, 0th index Set Call
    if row.name == 0:
        row['INSTRUMENT_TYPE'] = 'CALL'
        row['ORDER_TYPE_CALL'] = 'BUY'

    if row.name == 1: # 1st index set PUT
        row['INSTRUMENT_TYPE'] = 'PUT'
        row['ORDER_TYPE_PUT'] = 'BUY'

    # ORDER MANIFEST
    row["ORDER_MANIFEST"] = str(row['STRIKE_PRICE'])+'_'+row['INSTRUMENT_TYPE']+'_'+row['ORDER_TYPE_'+row['INSTRUMENT_TYPE']]+'_MARGIN_FREE_UP'

    return row

def populateSellDF(row):
    # SET ORDER TYPE AND OPTION TYPE
    row['ORDER_TYPE'] = 'SELL'
    row['INSTRUMENT_TYPE'] = None
    row['ORDER_TYPE_PUT'] = 'SELL'
    row['ORDER_TYPE_CALL'] = 'SELL'
    # SET OPTION TYPE BASED ON INDEX, 0th index Set Call
    if row.name == 0:
        row['INSTRUMENT_TYPE'] = 'PUT'
        row['ORDER_TYPE_PUT'] = 'SELL'

    if row.name == 1:  # 1st index set PUT
        row['INSTRUMENT_TYPE'] = 'CALL'
        row['ORDER_TYPE_CALL'] = 'SELL'

    # ORDER MANIFEST
    row["ORDER_MANIFEST"] = str(row['STRIKE_PRICE']) + '_' + row['INSTRUMENT_TYPE'] + '_' + row[
        'ORDER_TYPE_' + row['INSTRUMENT_TYPE']] + '_MARGIN_FREE_UP'

    return row
