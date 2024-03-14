

import logging
import pandas as pd
import json
from datetime import datetime
from common.UniqueKeyGenerator import UniqueKeyGenerator
from dao import PositionsDAO, PositionsTrackingDAO, OrdersDAO, ClientConfigurationDAO
from adaptor.OMAdaptor import OMAdaptor
import numpy as np
from entities.Enums import TransactionTypeEnum

def placeDealsAndPopulateDBOptions(position_group_id, marginFreeUp_dict, Configuration, df_Put_Call_Margin_Free_Up,
                                                         symbol, expiry_date_options, spot_value, expiry_date_futures):
    marginFreeUp_dict['position_group_id'] = position_group_id
    # ADD TRANSACTION DETAILS
    df_Put_Call_Margin_Free_Up = df_Put_Call_Margin_Free_Up.apply(lambda row: addTxnDetails(row), axis=1)

    # 1. GET EXISTING POSITIONS, UPDATE VALUES LIKE WAP EXECUTION PRICES, MF, QUANTITY
    # 2. DIVIDE DF INTO 2 DFs, ONE FOR EXISTING SELL/BUY POSITIONS, OTHER FOR HEDGED BUY FRESH POSITIONS

    ################################# STEP 2. GET EXISTING HEDGED POSITIONS ############################################
    df_positions_existing = PositionsDAO.getActivePositionsByPositionGroupIdAndSymbol(Configuration['SCHEMA_NAME'], position_group_id, symbol)
    df_positions_existing.columns = [x.upper() for x in df_positions_existing.columns]
    marginFreeUp_dict['df_positions_existing'] = df_positions_existing
    df_positions_existing_ATM = filterExistingATMPositions(df_positions_existing)

    ################################## STEP 3 :  HEDGING SCENARIOS STARTED #############################################
    df_Put_Call_Margin_Free_up_Squareoff_Partial = pd.merge(df_Put_Call_Margin_Free_Up,
                                                    df_positions_existing_ATM, how='inner',
                                                    on=['INSTRUMENT_TYPE', 'STRIKE_PRICE'])
    squareoffPositions(df_Put_Call_Margin_Free_up_Squareoff_Partial, marginFreeUp_dict, Configuration, symbol, expiry_date_options, position_group_id,
                                    expiry_date_futures, isPartialSquareOff=True)


    ######################################### HEDGING SCENARIOS ENDED ##################################################

    #################################### LOGGING ORDER GENERATION #######################################################
    logging.info( "######################################################################################################")
    print("######################################################################################################")
    logging.info("ORDER GEN MARGIN FREE UP INITIATED for {}, STRIKE PRICE : {}".format(marginFreeUp_dict['symbol'], str(marginFreeUp_dict['SP_SELL'])))
    print("ORDER GEN MARGIN FREE UP INITIATED for {}, STRIKE PRICE : {}".format(marginFreeUp_dict['symbol'], str(marginFreeUp_dict['SP_SELL'])))
    print("######################################################################################################")
    logging.info("######################################################################################################")

    logging.info('Margin Free Up Position Taken: Inserted in OOPS_POSITIONS AND TXN Table for Symbol {} and TransID {}'.format(symbol, position_group_id))

    return marginFreeUp_dict


def filterExistingATMPositions(df_positions_existing):
    # ADD EXECUTION PRICES
    df_positions_existing_hedged = df_positions_existing[df_positions_existing['ORDER_TYPE'] == 'SELL']
    df_positions_existing_hedged = df_positions_existing_hedged[
        ['EXECUTION_PRICE', 'EXECUTION_PRICE_ACTUAL', 'QUANTITY', 'MULTI_FACTOR', "STRIKE_PRICE",
         "INSTRUMENT_TYPE", "ORDER_TYPE", "SYMBOL", "EXECUTION_PARAMS", "OOPS_TRANS_ID", "OOPS_ORDER_ID",
         "CONTRACT_EXPIRY_DATE", "CURRENT_NET_DELTA", "CURRENT_NET_DELTA_OVERALL"]]
    # RENAME COLUMNS
    df_positions_existing_hedged['STRIKE_PRICE'] = df_positions_existing_hedged['STRIKE_PRICE'].astype(float)
    df_positions_existing_hedged['QUANTITY'] = df_positions_existing_hedged['QUANTITY'].astype(float)
    df_positions_existing_hedged['MULTI_FACTOR'] = df_positions_existing_hedged['MULTI_FACTOR'].astype(float)
    df_positions_existing_hedged.rename(columns={'EXECUTION_PRICE': 'EXECUTION_PRICE_POSITION',
                                                'EXECUTION_PRICE_ACTUAL': 'EXECUTION_PRICE_ACTUAL_POSITION',
                                                'QUANTITY': 'QUANTITY_POSITION',
                                                'MULTI_FACTOR': 'MULTI_FACTOR_POSITION',
                                                'ORDER_TYPE': 'ORDER_TYPE_POSITION'
                                                }, inplace=True)
    return df_positions_existing_hedged


def squareoffPositions(df_Margin_Free_Up, marginFreeUp_dict, squareOff_dict, Configuration, symbol, expiry_date_options, position_group_id,
                                    expiry_date_futures, isPartialSquareOff=True):
    order_group_id = str(int(UniqueKeyGenerator().getUniqueKeyDateTime()))
    Configuration['order_group_id'] = order_group_id
    df_Margin_Free_Up['order_group_id'] = order_group_id
    ################################ STEP 1. PLACE HEDGED TRADES USING OM ###############################################
    df_Margin_Free_Up = placeOrder(Configuration, df_Margin_Free_Up, symbol, expiry_date_options, position_group_id,
                                          expiry_date_futures)
    ####################################################################################################################

    # UNREALIZED PNL FUTURES
    # CALCULATE REALIZED PNL
    # df_Put_Call_Hedged_Squareoff = df_Put_Call_Hedged_Squareoff.apply(lambda row:
    #                             calculatePnlFuturesSquareOff(row, optionsHedging_dict, isPartialSquareOff), axis=1)
    df_Margin_Free_Up = calculatePnlHedgingSquareOff(df_Margin_Free_Up, isPartialSquareOff, marginFreeUp_dict, squareOff_dict)
    df_Margin_Free_Up.columns = [x.lower() for x in df_Margin_Free_Up.columns]


    BROKERAGE = 0.0
    df_Margin_Free_Up['brokerage'] = BROKERAGE
    df_Margin_Free_Up['realized_pnl_overall_brokerage'] = df_Margin_Free_Up['realized_pnl_overall'] - BROKERAGE

    for index, row in df_Margin_Free_Up.iterrows():
        OrdersDAO.insert(row=row, schema_name=Configuration['SCHEMA_NAME'])
        print('Inserted in ORDERS Table')

    ######################################  UPDATE POSITIONS/TRACKING TABLE FOR ACTIVE POSITIONS #######################
    for index, row in df_Margin_Free_Up.iterrows():
        # INSERT IN POSITIONS_TRACKING TABLE
        PositionsTrackingDAO.insert(Configuration['SCHEMA_NAME'], row=row)

    # UPDATE POSITIONS TABLE WITH PENDING QUANTITY AND PENDING NUM LOTS
    df_Margin_Free_Up['quantity'] = df_Margin_Free_Up['quantity_pending']
    df_Margin_Free_Up['num_lots'] = df_Margin_Free_Up['num_lots_pending']

    if marginFreeUp_dict['QUANTITY_PENDING'] == 0:
        df_Margin_Free_Up['is_active'] = False
        ClientConfigurationDAO.updateConfiguration(squareOff_dict['schema_name'], 'SYMBOL_ACTIVE_' + symbol, 'N')
        logging.info("MARGIN FREE UP : SYMBOL ACTIVE DISABLED : {}".format(symbol))

    for index, row_position in df_Margin_Free_Up.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsPostSquareOffMarginFreeUp(Configuration['SCHEMA_NAME'], row_position)

        print("INSERTED ENTRIES IN POSITIONS TRACKING TABLE!!!")
    ############################################ END - MARGIN FREE UP SQUAREOFF ########################################


def calculatePnlHedgingSquareOff(df_Margin_Free_Up, isPartialSquareOff, marginFreeUp_dict, squareOff_dict):


    df_Margin_Free_Up['EXIT_PRICE'] = df_Margin_Free_Up['CURRENT_PRICE']
    ############################################  INDIVIDUAL PNL CALCULATION ###########################################
    # REALIZED PNL
    df_Margin_Free_Up['REALIZED_PNL'] = np.where((df_Margin_Free_Up['TRANSACTION_TYPE'] == TransactionTypeEnum.BUY),
                                             ((df_Margin_Free_Up['QUANTITY'].astype(float).mul(df_Margin_Free_Up['CURRENT_PRICE'].astype(float))).astype(float)
                                                 .sub((df_Margin_Free_Up['QUANTITY'].astype(float)
                                                 .mul(df_Margin_Free_Up['ENTRY_PRICE'].astype(float))).astype(float),axis=0)),
                                             ((df_Margin_Free_Up['QUANTITY'].astype(float).mul(df_Margin_Free_Up['ENTRY_PRICE'].astype(float))).astype( float)
                                                 .sub((df_Margin_Free_Up['QUANTITY'].astype(float)
                                                 .mul(df_Margin_Free_Up['CURRENT_PRICE'].astype(float))).astype(float),axis=0)))

    REALIZED_PNL_GROUP = np.sum(df_Margin_Free_Up['REALIZED_PNL'].astype(float))
    df_Margin_Free_Up['REALIZED_PNL_GROUP'] = REALIZED_PNL_GROUP

    ####################################### REALIZED PNL OVERALLLLLL ###############################################
    if squareOff_dict['REALIZED_PNL_OVERALL'] != None:
        REALIZED_PNL_OVERALL = squareOff_dict['REALIZED_PNL_OVERALL'] + REALIZED_PNL_GROUP
    else:
        REALIZED_PNL_OVERALL = REALIZED_PNL_GROUP

    # UPDATE REALIZED_PNL_OVERALL
    df_Margin_Free_Up['REALIZED_PNL_OVERALL'] = REALIZED_PNL_OVERALL
    squareOff_dict['REALIZED_PNL_OVERALL'] = REALIZED_PNL_OVERALL

    ####################################### UNREALIZED PNL #########################################################
    df_Margin_Free_Up['UNREALIZED_PNL'] = np.where((df_Margin_Free_Up['TRANSACTION_TYPE'] == TransactionTypeEnum.BUY),
                                                 ((df_Margin_Free_Up['QUANTITY_PENDING'].astype(float).mul(df_Margin_Free_Up['CURRENT_PRICE'].astype(float))).astype(float)
                                                     .sub((df_Margin_Free_Up['QUANTITY_PENDING'].astype(float)
                                                     .mul(df_Margin_Free_Up['ENTRY_PRICE'].astype(float))).astype(float),axis=0)),
                                                 ((df_Margin_Free_Up['QUANTITY_PENDING'].astype(float).mul(df_Margin_Free_Up['ENTRY_PRICE'].astype(float))).astype( float)
                                                     .sub((df_Margin_Free_Up['QUANTITY_PENDING'].astype(float)
                                                     .mul(df_Margin_Free_Up['CURRENT_PRICE'].astype(float))).astype(float),axis=0)))

    df_Margin_Free_Up['UNREALIZED_PNL_GROUP'] = np.sum(df_Margin_Free_Up['UNREALIZED_PNL'].astype(float))

    ########################################## NET_PNL_OVERALL #####################################################
    df_Margin_Free_Up = df_Margin_Free_Up.apply(lambda row: calculateNetPnl(row, squareOff_dict), axis=1)

    # LOGGING SQUARE OFF
    logging.info("######################################################################################################")
    print("######################################################################################################")
    logging.info("MARGIN FREE UP SQUAREOFF INITIATED for {}, TOTAL INITIAL MARGIN :"
                 " {}, QUANTITY : {} ".format(squareOff_dict['symbol'], squareOff_dict['TOTAL_INITIAL_MARGIN'], df_Margin_Free_Up['QUANTITY'].iloc[0]))
    print("MARGIN FREE UP SQUAREOFF INITIATED for {}, TOTAL INITIAL MARGIN : {},"
          " QUANTITY : {} ".format( squareOff_dict['symbol'],squareOff_dict['TOTAL_INITIAL_MARGIN'], df_Margin_Free_Up['QUANTITY'].iloc[0]))
    print("######################################################################################################")
    logging.info("######################################################################################################")

    return df_Margin_Free_Up

def calculateNetPnl(row, squareOff_dict):
    if squareOff_dict['REALIZED_PNL_OVERALL'] != None:
        row['NET_PNL_OVERALL'] = float(row['UNREALIZED_PNL_GROUP']) + float(squareOff_dict['REALIZED_PNL_OVERALL'])
    else:
        row['NET_PNL_OVERALL'] = float(row['UNREALIZED_PNL_GROUP'])

    return row


def updatePositionsPostSquareOffOptionsMarginFreeUp(df_positions_Squareoff_Margin_Free_Up):
    for index, row_position in df_positions_Squareoff_Margin_Free_Up.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsPostSquareOffJobOptionsMarginFreeUp(row_position)

def updatePositionsPostSquareOffAllMarginFreeUp(df_positions_Squareoff_Margin_Free_Up):
    for index, row_position in df_positions_Squareoff_Margin_Free_Up.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsPostSquareOffJobAllMarginFreeUp(row_position)


def updatePositionMergeValues(df_positions):
    for index, row_position in df_positions.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsMergeValues(row_position)

def updatePositionsExistingOptions(df_positions_Futures):
    for index, row_position in df_positions_Futures.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsExistingOptions(row_position)

def updatePositionsDeltaAll(df_positions_Futures):
    for index, row_position in df_positions_Futures.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsDeltaAll(row_position)

def addTxnDetails(row):
    row['QUANTITY_TXN_'+row['INSTRUMENT_TYPE']] = row['QUANTITY_'+row['INSTRUMENT_TYPE']]
    row['MULTI_FACTOR_TXN'] = row['MULTI_FACTOR_MARGIN_FREE_UP']
    return row

def placeOrder(Configuration, df_Put_Call_Margin_Free_up_Squareoff_Partial, symbol, expiry_date_options, position_group_id,
                                          expiry_date_futures):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']:
        omAdaptor = OMAdaptor()
        orderType = "MARGIN_FREE_UP"
        tradeType = Configuration['TRADE_TYPE']
        df_Put_Call_Margin_Free_up_Squareoff_Partial = omAdaptor.placeOrders(df_Put_Call_Margin_Free_up_Squareoff_Partial, Configuration, symbol, orderType, tradeType, expiry_date_options,
                                            position_group_id, expiry_date_futures, isFresh=False, isHedging=False,
                                            isSquareOff=True, isMarginFreeUp= False)

    else:  # Default Broker related fields
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_id_fresh_put'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_id_fresh_call'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_id_fresh_futures'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_status_fresh_put'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_status_fresh_call'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_status_fresh_futures'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_id_squareoff_put'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_id_squareoff_call'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_id_squareoff_futures'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_status_squareoff_put'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_status_squareoff_call'] = "NA"
        df_Put_Call_Margin_Free_up_Squareoff_Partial['broker_order_status_squareoff_futures'] = "NA"

    return df_Put_Call_Margin_Free_up_Squareoff_Partial




