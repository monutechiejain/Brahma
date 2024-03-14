import pandas as pd
import json
from common import utils as parent_utils
from common import utils
from dao import ClientConfigurationDAO
from dao import PositionsDAO, PositionsTrackingDAO, OrdersDAO, PositionsTrackingBackUpDAO
import logging
from adaptor.OMAdaptor import OMAdaptor
from adaptor import GreeksAdaptor
from helper import  QtyFreezeHelper
from rules import SquareOffRuleEngine
from entities.Enums import InstrumentTypeEnum, TransactionTypeEnum
from common.UniqueKeyGenerator import UniqueKeyGenerator
import numpy as np
from datetime import datetime
import pytz
import warnings
from concurrent.futures import ThreadPoolExecutor
from pandas.core.common import SettingWithCopyWarning


warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)

def filterBasedOnExistingPositions(df_Put_Call, squareOff_dict):
    # GET EXECUTION PARAMS LIKE STRIKE PRICE, ORDER_TYPE
    df_put_existing = squareOff_dict['df_put_existing']
    df_call_existing = squareOff_dict['df_call_existing']
    df_futures_existing = squareOff_dict['df_futures_existing']

    # GET STRIKE PRICE/ ORDER TYPES / QUANTITY / QUANTITY_FUTURES/ EXECUTION_PRICES / PNL
    STRIKE_PRICE = float(df_put_existing['STRIKE_PRICE'].iloc[0])
    ORDER_TYPE_PUT = df_put_existing['TRANSACTION_TYPE'].iloc[0]
    ORDER_TYPE_CALL = df_call_existing['TRANSACTION_TYPE'].iloc[0]
    QUANTITY = df_call_existing['QUANTITY'].iloc[0]

    ########################################## MERGE IN df_Put_Call ####################################################
    df_Put_Call = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == STRIKE_PRICE]
    df_Put_Call['ORDER_TYPE_PUT'] = ORDER_TYPE_PUT
    df_Put_Call['ORDER_TYPE_CALL'] = ORDER_TYPE_CALL
    df_Put_Call['EXECUTION_PRICE_PUT_ACTUAL'] = df_put_existing['ENTRY_PRICE'].iloc[0]
    df_Put_Call['EXECUTION_PRICE_CALL_ACTUAL'] = df_call_existing['ENTRY_PRICE'].iloc[0]
    df_Put_Call['MULTI_FACTOR_CALL'] = df_call_existing['NUM_LOTS'].iloc[0]
    df_Put_Call['MULTI_FACTOR_PUT'] = df_call_existing['NUM_LOTS'].iloc[0]
    df_Put_Call['QUANTITY_PUT'] = QUANTITY
    df_Put_Call['QUANTITY_CALL'] = QUANTITY
    df_Put_Call['NET_DELTA_FUTURES'] = 0.0

    ############################################## MERGE FUTURES VALUES ################################################
    if squareOff_dict['isFuturesTaken']:
        ORDER_TYPE_FUTURES = df_futures_existing['TRANSACTION_TYPE'].iloc[0]
        df_Put_Call['ORDER_TYPE_FUTURES'] = ORDER_TYPE_FUTURES
        df_Put_Call['QUANTITY_FUTURES'] = df_futures_existing['QUANTITY'].astype(float).iloc[0]
        #df_futures_existing = df_futures_existing.apply(lambda row: populateExecutionParams(row), axis=1)
        df_Put_Call['NET_DELTA_FUTURES'] = df_futures_existing['CURRENT_NET_DELTA'].astype(float).iloc[0]
        df_Put_Call['EXECUTION_PRICE_FUTURES_ACTUAL'] = df_futures_existing['ENTRY_PRICE'].iloc[0]
        df_Put_Call['MULTI_FACTOR_FUTURES'] = df_futures_existing['NUM_LOTS'].astype(float).iloc[0]

        ########################### INITIALIZE FUTURES VALUES #########################################################
        df_Put_Call['DELTA_FUTURES'] = "NA"
        df_Put_Call['GAMMA_FUTURES'] = "NA"
        df_Put_Call['IV_FUTURES'] = "NA"
        df_Put_Call['THETA_FUTURES'] = "NA"
        df_Put_Call['VEGA_FUTURES'] = "NA"

    df_Put_Call['REALIZED_PNL_OVERALL_POSITION'] = df_put_existing['REALIZED_PNL_OVERALL'].iloc[0]
    df_Put_Call['REALIZED_PNL_OPTIONS_POSITION'] = df_put_existing['REALIZED_PNL_OPTIONS'].iloc[0]
    df_Put_Call['REALIZED_PNL_FUTURES_POSITION'] = df_put_existing['REALIZED_PNL_FUTURES'].iloc[0]
    df_Put_Call['IS_TRAILING_ACTIVE_POSITION'] = df_put_existing['IS_TRAILING_ACTIVE'].iloc[0]
    df_Put_Call['TRAILING_PNL_POSITION'] = df_put_existing['TRAILING_PNL'].iloc[0]
    squareOff_dict['df_Put_Call'] = df_Put_Call



    # df_positions_existing = orderGen_dict['df_positions_existing']
    # symbol = orderGen_dict["symbol"]
    # df_positions_merge = df_positions_existing[['strike_price', 'lot_size', 'multi_factor',
    #                                             'contract_expiry_date', 'order_type_put', 'order_type_call',
    #                                             'order_type_futures', 'quantity','execution_price_put_actual',
    #                                             'execution_price_call_actual', 'execution_price_futures_actual',
    #                                             'position_group_id', 'oops_order_id', 'execution_params', 'margin_txn',
    #                                             'realized_pnl_overall', 'unrealized_pnl_overall']]
    #
    # df_positions_merge.rename(columns={'strike_price_put': 'STRIKE_PRICE'}, inplace=True)
    # df_positions_merge.columns = [x.upper() for x in df_positions_merge.columns]
    #
    # # TRANSFORMATION
    # df_positions_merge['STRIKE_PRICE'] = df_positions_merge['STRIKE_PRICE'].astype(float)
    # # PNL VALUES FOR EXISTING POSITION
    # df_positions_merge['UNREALIZED_PNL_OVERALL'] = df_positions_merge['UNREALIZED_PNL_OVERALL'].replace('NA','0.0',regex = True)
    # df_positions_merge['REALIZED_PNL_OVERALL'] = df_positions_merge['REALIZED_PNL_OVERALL'].replace('NA','0.0',regex = True)
    # df_positions_merge['UNREALIZED_PNL_OVERALL_POSITION'] = df_positions_merge['UNREALIZED_PNL_OVERALL'].astype(float)
    # df_positions_merge['REALIZED_PNL_OVERALL_POSITION'] = df_positions_merge['REALIZED_PNL_OVERALL'].astype(float)
    #
    # # MERGING
    # df_Put_Call = pd.merge(df_Put_Call, df_positions_merge, how='inner', on='STRIKE_PRICE')
    # df_Put_Call.reset_index(drop=True, inplace=True)
    #
    # # ADD EXECUTION DETAILS NEEDED FOR DECISION MAKING
    # df_Put_Call = df_Put_Call.apply(lambda row: populateExecutionParams(row, symbol), axis=1)

    return squareOff_dict

def populateExecutionParams(row):
    execution_params = json.loads(row['EXECUTION_PARAMS'])
    row['NET_DELTA_FUTURES'] = execution_params['execution_net_delta']
    return row


def initialTransformationPositions(squareoffOptions_dict):
    df_Put_Call = squareoffOptions_dict['df_Put_Call']
    Pair_Id_List = squareoffOptions_dict['Pair_Id_List']
    df_positions_options = squareoffOptions_dict['df_positions_options']
    spot_value = squareoffOptions_dict['spot_value']
    current_time_to_expiry = squareoffOptions_dict['current_time_to_expiry']
    print(df_positions_options)

    # Add columns to positions dataframe
    df_positions_options['current_delta_diff'] = None
    df_positions_options['current_delta_call'] = None
    df_positions_options['current_delta_put'] = None
    df_positions_options['current_iv_call'] = None
    df_positions_options['current_iv_put'] = None
    df_positions_options['current_underlying'] = spot_value
    df_positions_options['current_time_to_expiry'] = str(round(current_time_to_expiry,3))
    df_positions_options['execution_delta_call'] = None
    df_positions_options['execution_delta_put'] = None
    df_positions_options['execution_delta_diff'] = None
    df_positions_options['execution_delta_call_actual'] = None
    df_positions_options['execution_delta_put_actual'] = None
    df_positions_options['execution_delta_diff_actual'] = None
    df_positions_options['execution_iv_call'] = None
    df_positions_options['execution_iv_put'] = None
    df_positions_options['is_square_off'] = False
    df_positions_options['squareoff_params'] = None

    print(df_positions_options)


    # Populating df_positions columns from df_Put_Call(Option Chain)
    for pair_id in Pair_Id_List:
        sp_put, sp_call = pair_id.split('_')
        delta_put = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == float(sp_put)]['DELTA_PUT'].iloc[0]
        delta_call = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == float(sp_call)]['DELTA_CALL'].iloc[0]
        current_delta_diff = abs(delta_put+delta_call)
        iv_put = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == float(sp_put)]['IV_PUT'].iloc[0]
        iv_call = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == float(sp_call)]['IV_CALL'].iloc[0]
        current_price_put = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == float(sp_put)]['EXECUTION_PRICE_PUT'].iloc[0]
        current_price_call = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == float(sp_call)]['EXECUTION_PRICE_CALL'].iloc[0]

        loc = df_positions_options[df_positions_options['ia_options_pair_id'] == pair_id].index.tolist()[0]
        df_positions_options.at[loc, 'current_delta_diff'] = current_delta_diff
        df_positions_options.at[loc, 'current_delta_put'] = delta_put
        df_positions_options.at[loc, 'current_delta_call'] = delta_call
        df_positions_options.at[loc, 'current_iv_put'] = iv_put
        df_positions_options.at[loc, 'current_iv_call'] = iv_call
        df_positions_options.at[loc, 'current_price_put'] = current_price_put
        df_positions_options.at[loc, 'current_price_call'] = current_price_call

        # Populating Execution Params Json into Columns
        execution_params = json.loads(df_positions_options.iloc[loc]['execution_params'])
        print(execution_params)
        df_positions_options.at[loc, 'execution_delta_call'] = execution_params['execution_delta_call']
        df_positions_options.at[loc, 'execution_delta_put'] = execution_params['execution_delta_put']
        df_positions_options.at[loc, 'execution_delta_diff'] = execution_params['execution_delta_diff']
        df_positions_options.at[loc, 'execution_delta_call_actual'] = execution_params['execution_delta_call_actual']
        df_positions_options.at[loc, 'execution_delta_put_actual'] = execution_params['execution_delta_put_actual']
        df_positions_options.at[loc, 'execution_delta_diff_actual'] = execution_params['execution_delta_diff_actual']
        df_positions_options.at[loc, 'execution_iv_call'] = execution_params['execution_iv_call']
        df_positions_options.at[loc, 'execution_iv_put'] = execution_params['execution_iv_put']


    #print('initialTransformationPositions df_positions')
    #print(df_positions_options)
    squareoffOptions_dict['df_positions_options'] = df_positions_options

    return squareoffOptions_dict

def placeDealsAndCalculatePnl(squareOff_dict, Configuration, symbol,expiry_date_options, expiry_date_futures,
                                                                      position_group_id, expiry_date, spot_value):
    df_positions_existing = squareOff_dict['df_positions_existing']

    df_positions_existing = placeDealsAndCalculatePnlByRow(df_positions_existing, squareOff_dict, Configuration, symbol, expiry_date_options, expiry_date_futures,
                                     position_group_id, expiry_date, spot_value)

    ########################################### STOPLOSS PNL : START ###################################################
    # SQUAREOFF ALL IS FALSE
    # if not squareOff_dict['isSquareOff']:  # IF SQUAREOFF ALL TRUE, THEN NO POINT OF STOPLOSS CHECK
    #     df_positions_existing = SquareOffRuleEngine.checkStopLossTargetPnl(Configuration, df_positions_existing, squareOff_dict, symbol)
    #
    #     # IF SQUAREOFF BECOMES TRUE POST STOPLOSS RULES, CALCULATE REALIZED PNL
    #     if squareOff_dict['isSquareOff']:
    #         df_positions_existing = placeDealsAndCalculatePnlByRow(df_positions_existing, squareOff_dict,
    #                                   Configuration, symbol, expiry_date_options, expiry_date_futures,
    #                                   position_group_id, expiry_date, spot_value)
    ########################################### STOPLOSS PNL : END #####################################################

    ########################################### STOPLOSS PNL : START ###################################################
    # SQUAREOFF ALL IS FALSE
    if not squareOff_dict['isSquareOff']:  # IF SQUAREOFF ALL TRUE, THEN NO POINT OF STOPLOSS CHECK
        df_positions_existing = SquareOffRuleEngine.checkStopLossPortfolioTargetSLPctPnl(Configuration, df_positions_existing,
                                                                           squareOff_dict, symbol)

        # IF SQUAREOFF BECOMES TRUE POST STOPLOSS RULES, CALCULATE REALIZED PNL
        if squareOff_dict['isSquareOff']:
            df_positions_existing = placeDealsAndCalculatePnlByRow(df_positions_existing, squareOff_dict,
                                                                   Configuration, symbol, expiry_date_options,
                                                                   expiry_date_futures,
                                                                   position_group_id, expiry_date, spot_value)
    ########################################### STOPLOSS PNL : END #####################################################

    # ADD to SQUAREOFF DICT
    squareOff_dict['df_positions_existing'] = df_positions_existing
    return squareOff_dict


def placeDealsAndCalculatePnlByRow(df_positions_existing, squareOff_dict, Configuration, symbol, expiry_date_options, expiry_date_futures,
                                     position_group_id, expiry_date, spot_value):
    # MOCK DATA FOR TESTING PNL
    # if row['STRIKE_PRICE'] == 31100.0:
    #     row['is_square_off'] = True

    #QUANTITY_TXN = squareOff_dict['QUANTITY_TXN']
    isSquareOff = squareOff_dict['isSquareOff']

    if not isSquareOff:
        # UNREALIZED PNL
        df_positions_existing['UNREALIZED_PNL'] = np.where((df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.BUY),
                                                 ((df_positions_existing['QUANTITY'].astype(float).mul(df_positions_existing['CURRENT_PRICE'].astype(float))).astype(float)
                                                     .sub((df_positions_existing['QUANTITY'].astype(float)
                                                     .mul(df_positions_existing['ENTRY_PRICE'].astype(float))).astype(float),axis=0)),
                                                 ((df_positions_existing['QUANTITY'].astype(float).mul(df_positions_existing['ENTRY_PRICE'].astype(float))).astype( float)
                                                     .sub((df_positions_existing['QUANTITY'].astype(float)
                                                     .mul(df_positions_existing['CURRENT_PRICE'].astype(float))).astype(float),axis=0)))

        df_positions_existing['UNREALIZED_PNL_GROUP'] = np.sum(df_positions_existing['UNREALIZED_PNL'].astype(float))

        ########################################## REALIZED PNL OVERALL ################################################

        ########################################## NET_PNL_OVERALL #####################################################
        df_positions_existing = df_positions_existing.apply(lambda row: calculateNetPnl(row, squareOff_dict), axis=1)


        ###############################################################################################################
    else:

        order_group_id = str(int(UniqueKeyGenerator().getUniqueKeyDateTime()))
        Configuration['order_group_id'] = order_group_id
        squareOff_dict['order_group_id'] = order_group_id
        #squareOff_dict['TOTAL_INITIAL_MARGIN'] = TOTAL_AVAILABLE_MARGIN + squareOff_dict['MARGIN_TXN_CURRENT']
        ############################### PLACE SQUAREOFF TRADE USING OM ######################################################
        df_positions_existing = placeOrderSquareOff(Configuration, df_positions_existing, symbol, expiry_date, position_group_id,
                                    expiry_date_futures, squareOff_dict)
        df_positions_existing['EXIT_PRICE'] = df_positions_existing['CURRENT_PRICE']
        ############################################  INDIVIDUAL PNL CALCULATION ###########################################
        # REALIZED PNL
        df_positions_existing['REALIZED_PNL'] = np.where((df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.BUY),
                                                 ((df_positions_existing['QUANTITY'].astype(float).mul(df_positions_existing['CURRENT_PRICE'].astype(float))).astype(float)
                                                     .sub((df_positions_existing['QUANTITY'].astype(float)
                                                     .mul(df_positions_existing['ENTRY_PRICE'].astype(float))).astype(float),axis=0)),
                                                 ((df_positions_existing['QUANTITY'].astype(float).mul(df_positions_existing['ENTRY_PRICE'].astype(float))).astype( float)
                                                     .sub((df_positions_existing['QUANTITY'].astype(float)
                                                     .mul(df_positions_existing['CURRENT_PRICE'].astype(float))).astype(float),axis=0)))

        REALIZED_PNL_GROUP = np.sum(df_positions_existing['REALIZED_PNL'].astype(float))
        df_positions_existing['REALIZED_PNL_GROUP'] = REALIZED_PNL_GROUP

        ####################################### REALIZED PNL OVERALLLLLL ###############################################
        if squareOff_dict['REALIZED_PNL_OVERALL'] != None:
            REALIZED_PNL_OVERALL = squareOff_dict['REALIZED_PNL_OVERALL'] + REALIZED_PNL_GROUP
        else:
            REALIZED_PNL_OVERALL = REALIZED_PNL_GROUP

        # UPDATE REALIZED_PNL_OVERALL
        df_positions_existing['REALIZED_PNL_OVERALL'] = REALIZED_PNL_OVERALL
        squareOff_dict['REALIZED_PNL_OVERALL'] = REALIZED_PNL_OVERALL

        ####################################### UNREALIZED PNL #########################################################
        df_positions_existing['UNREALIZED_PNL_GROUP'] = 0.0
        df_positions_existing['UNREALIZED_PNL'] = 0.0

        # NET_PNL_OVERALL
        df_positions_existing['NET_PNL_OVERALL'] = df_positions_existing['REALIZED_PNL_OVERALL']

        # LOGGING SQUARE OFF
        logging.info("######################################################################################################")
        print("######################################################################################################")
        logging.info("SQUAREOFF INITIATED for {}, STRIKE PRICE : {}, TOTAL INITIAL MARGIN : {}, QUANTITY : {} ".format(squareOff_dict['symbol'], df_positions_existing['STRIKE_PRICE'].iloc[0], squareOff_dict['TOTAL_INITIAL_MARGIN'], df_positions_existing['QUANTITY'].iloc[0]))
        print("SQUAREOFF INITIATED for {}, STRIKE PRICE : {}, TOTAL INITIAL MARGIN : {}, QUANTITY : {} ".format( squareOff_dict['symbol'], df_positions_existing['STRIKE_PRICE'].iloc[0], squareOff_dict['TOTAL_INITIAL_MARGIN'], df_positions_existing['QUANTITY'].iloc[0]))
        print("######################################################################################################")
        logging.info("######################################################################################################")

    return df_positions_existing

def calculateNetPnl(row, squareOff_dict):
    if squareOff_dict['REALIZED_PNL_OVERALL'] != None:
        row['NET_PNL_OVERALL'] = float(row['UNREALIZED_PNL_GROUP']) + float(squareOff_dict['REALIZED_PNL_OVERALL'])
    else:
        row['NET_PNL_OVERALL'] = float(row['UNREALIZED_PNL_GROUP'])

    row['REALIZED_PNL_OVERALL'] = squareOff_dict['REALIZED_PNL_OVERALL']

    return row


def populateDBOptions(squareOff_dict, symbol, time_to_expiry_options_252,
                                                                           time_to_expiry_options_365,
                                                                           spot_value, Configuration):
    position_group_id = squareOff_dict['position_group_id']
    schema_name = squareOff_dict['schema_name']
    isSquareOff =  squareOff_dict['isSquareOff']
    df_positions_existing = squareOff_dict['df_positions_existing']
    df_positions_tracking = transformationPositionsDataframe(df_positions_existing, symbol, time_to_expiry_options_252,
                                                                           time_to_expiry_options_365,
                                                                           spot_value, squareOff_dict, Configuration)
    #print('SQUAREOFF FLAG --> {}'.format(isSquareOff))

    ############################################## FILL NAN VALUES ######################################################
    df_positions_tracking = df_positions_tracking.replace({np.nan: None})

    if isSquareOff:
        # ADD ORDER GROUP ID WHENEVER PLACING ORDER
        df_positions_tracking['order_group_id'] = squareOff_dict['order_group_id']

        # ADD SQUAREOFF TO ORDER MANIFEST
        df_positions_tracking['order_manifest'] = df_positions_tracking['order_manifest']+"_"+squareOff_dict['squareoff_type']

        # # INSERT IN TXNS TABLE
        # df_positions_tracking_txn = df_positions_tracking.copy()
        # df_positions_tracking_txn = df_positions_tracking_txn.apply(lambda row: reverseOrderType(row), axis=1)
        #
        # # POPULATE BROKERAGE
        # OrderGenOptionsHelper.insertFreshPositionTxn(df_positions_tracking_txn)
        #BROKERAGE = BrokerageCalculatorHelper.populateBrokerage(Configuration, position_group_id, symbol)
        BROKERAGE = 0.0 # ENABLE WEN API IS READY
        df_positions_tracking = df_positions_tracking.apply(lambda row: addBrokerage(row, BROKERAGE), axis=1)

        for index, row in df_positions_tracking.iterrows():
            OrdersDAO.insert(row=row, schema_name = schema_name)
            print('Inserted in ORDERS Table')

    ######################################  UPDATE POSITIONS/TRACKING TABLE FOR ACTIVE POSITIONS #######################
    for index, row in df_positions_tracking.iterrows():
        # INSERT IN POSITIONS_TRACKING TABLE
        PositionsTrackingDAO.insert(row = row, schema_name = schema_name)
        PositionsTrackingBackUpDAO.insert(row=row, schema_name=schema_name)

    for index, row_position in df_positions_tracking.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsPostSquareOffAll(row = row_position, schema_name = schema_name)


    print("INSERTED ENTRIES IN POSITIONS TRACKING TABLE!!!")

    ################################## UPDATE MARGIN INFO ############################################################
    #OOPSConfigurationDAO.updateConfiguration('TOTAL_INITIAL_MARGIN', str(squareOff_dict['TOTAL_INITIAL_MARGIN']))

    # SET df_positions_tracking in squareoff_dict
    squareOff_dict['df_positions_tracking'] = df_positions_tracking
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    squareOff_dict['current_time'] = current_time

    # SEND MAIL IN CASE OF SQUAREOFF ALL
    # if isSquareOff:
    #     df_positions_tracking_mail = df_positions_tracking.copy()
    #     send_mail(Configuration, df_positions_tracking_mail, symbol)

    return squareOff_dict

def reverseOrderType(row):
    row['order_type'] = 'SELL' if row['order_type'] == 'BUY' else 'BUY'
    return row

def addBrokerage(row, BROKERAGE):
    row['brokerage'] = BROKERAGE
    row['realized_pnl_overall_brokerage'] = row['realized_pnl_overall'] - BROKERAGE
    return row

def send_mail(Configuration, df_positions_tracking_mail, symbol):
    if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
        REALIZED_PNL_OVERALL = str(df_positions_tracking_mail['realized_pnl_overall'].iloc[0])
        df_positions_tracking_mail['instrument_type'] = df_positions_tracking_mail['strike_price'].astype(int).astype(str)+"_"+df_positions_tracking_mail['instrument_type']
        df_positions_tracking_mail = df_positions_tracking_mail[['realized_pnl','instrument_type','realized_pnl_txn','realized_pnl_options']]
        df_positions_tracking_mail.set_index("instrument_type", inplace=True)
        subject = Configuration['SCHEMA_NAME'] + " | " + symbol + " | PNL_SUMMARY"
        order_placed_body = ' Hi Team,<br><br><nbsp>This is to inform you that '+Configuration['SCHEMA_NAME']+\
                            ' Strategy has closed all positions, Details Mentioned below:<br><br><b>Realized Pnl Overall :  ' \
                            '{0}<br><br>Pnl Breakup :</b><br><br>{1}'.format(REALIZED_PNL_OVERALL,df_positions_tracking_mail.to_html())
        order_placed_body = order_placed_body + '<br>Thanks and Regards,<br>Team DeriveQ'
        parent_utils.send_email_dqns(Configuration, subject, order_placed_body, "HTML")

def insertSquareOffPositionsTracking(df_positions_tracking, schema_name):
    for index, row in df_positions_tracking.iterrows():
        # INSERT IN POSITIONS_TRACKING TABLE
        PositionsTrackingDAO.insert(row=row, schema_name=schema_name)
        PositionsTrackingBackUpDAO.insert(row=row, schema_name=schema_name)
    print('Inserted in POSITIONS_TRACKING Table')


def finalTransformationPositions(squareoffOptions_dict):
    df_positions_options = squareoffOptions_dict['df_positions_options']

    df_positions_squareoff = df_positions_options.copy()

    for column in df_positions_squareoff.columns.values:
        df_positions_squareoff[column] = df_positions_squareoff[column].astype('str')

    return df_positions_squareoff


def updatePositionsPostSquareOffJob(df_positions_squareoff):

    for index, row_position in df_positions_squareoff.iterrows():
        PositionsDAO.updatePositionsPostSquareOffJob(row_position)
    print('Updated in IA_OPTIONS_POSITIONS_OPTIONS Table')



def placeOrderSquareOff(Configuration, df_positions_existing, symbol, expiry_date, position_group_id,
                                    expiry_date_futures, squareOff_dict):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']:
        omAdaptor = OMAdaptor()
        orderType = "SQUAREOFF"
        tradeType = Configuration['TRADE_TYPE']
        df_positions_existing['ORDER_MANIFEST'] = "SQUAREOFF_ALL"

        # isSquareOffRealignment = False
        # if 'squareoff_type' in squareOff_dict and squareOff_dict['squareoff_type'] == 'SQUAREOFF_REALIGNED':
        #     isSquareOffRealignment = True


        ################################################# QTY FREEZE SPLIT ##############################################
        # df_positions_existing, isQtyFreeze = QtyFreezeHelper.qtyFreezeSplit(df_positions_existing, Configuration, symbol)
        #
        # df_positions_existing = omAdaptor.placeOrders(df_positions_existing, Configuration, symbol, orderType, tradeType, expiry_date,
        #                                     position_group_id, expiry_date_futures, isFresh=False, isHedging=False,
        #                                     isSquareOff=True, isQtyFreeze = isQtyFreeze)

        ######################################### CASE 1 : PLACE SELL ORDERS ###############################################
        df_positions_existing_Sell = df_positions_existing[df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.SELL]
        if df_positions_existing_Sell is not None and len(df_positions_existing_Sell) > 0:
            df_positions_existing_Sell, isQtyFreeze = QtyFreezeHelper.qtyFreezeSplit(df_positions_existing_Sell, Configuration, symbol)
            df_positions_existing_Sell = omAdaptor.placeOrders(df_positions_existing_Sell, Configuration, symbol, orderType, tradeType, expiry_date,
                                                position_group_id, expiry_date_futures, isFresh=False, isHedging=False,
                                                isSquareOff=True, isQtyFreeze = isQtyFreeze)
            df_positions_existing = df_positions_existing_Sell

        ######################################### CASE 2 : PLACE HEDGED BUY ORDERS ##################################
        df_positions_existing_Buy = df_positions_existing[df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.BUY]
        if df_positions_existing_Buy is not None and len(df_positions_existing_Buy) > 0:
            df_positions_existing_Buy, isQtyFreeze = QtyFreezeHelper.qtyFreezeSplit(df_positions_existing_Buy, Configuration, symbol)
            df_positions_existing_Buy = omAdaptor.placeOrders(df_positions_existing_Buy, Configuration, symbol, orderType,
                                                               tradeType, expiry_date,
                                                               position_group_id, expiry_date_futures, isFresh=False,
                                                               isHedging=False,
                                                               isSquareOff=True, isQtyFreeze=isQtyFreeze)
            df_positions_existing = df_positions_existing_Sell.append(df_positions_existing_Buy, ignore_index=True)


    return df_positions_existing


def populateWAPBidAskPrices(df_Put_Call, symbol, Configuration, squareOff_dict):
    LOT_SIZE = Configuration['LOT_SIZE_' + symbol]
    #QUANTITY_TXN = squareOff_dict["QUANTITY_TXN"]
    df_Put_Call['LOT_SIZE'] = LOT_SIZE
    df_Put_Call['BID_PRICE'] = df_Put_Call.apply(lambda row: calculateBidAskPriceByMF(row)[0],axis=1).astype(float)
    df_Put_Call['ASK_PRICE'] = df_Put_Call.apply(lambda row: calculateBidAskPriceByMF(row)[1],axis=1).astype(float)

    ############################### QUANTITY FOR THIS TXN, SHOULD BE MF_STEP_SIZE ######################################
    ################################ df_Put_Call[QUANTITY] --> Total QUantity taken trades ##############################
    #df_Put_Call['QUANTITY_TXN'] = QUANTITY_TXN

    return df_Put_Call

def populateCurrentPricesWAP(df_Put_Call, future_value):
    for index, row in df_Put_Call.iterrows():
        # Below order types are during order generation, so reverse
        # in case of squareoff reverse
        if row['TRANSACTION_TYPE'] == TransactionTypeEnum.SELL:
            df_Put_Call.at[index, 'CURRENT_PRICE'] = row['ASK_PRICE']
        else:
            df_Put_Call.at[index, 'CURRENT_PRICE'] = row['BID_PRICE']

    return df_Put_Call

def calculateBidAskPriceByMF(row):
    bid_ask_dict = {}

    bid_ask_dict['BID_PRICE_1'] = row['BID_PRICE_1']
    bid_ask_dict['BID_PRICE_2'] = row['BID_PRICE_2']
    bid_ask_dict['BID_PRICE_3'] = row['BID_PRICE_3']
    bid_ask_dict['BID_PRICE_4'] = row['BID_PRICE_4']
    bid_ask_dict['BID_PRICE_5'] = row['BID_PRICE_5']
    bid_ask_dict['BID_QTY_1'] = row['BID_QTY_1']
    bid_ask_dict['BID_QTY_2'] = row['BID_QTY_2']
    bid_ask_dict['BID_QTY_3'] = row['BID_QTY_3']
    bid_ask_dict['BID_QTY_4'] = row['BID_QTY_4']
    bid_ask_dict['BID_QTY_5'] = row['BID_QTY_5']

    bid_ask_dict['ASK_PRICE_1'] = row['ASK_PRICE_1']
    bid_ask_dict['ASK_PRICE_2'] = row['ASK_PRICE_2']
    bid_ask_dict['ASK_PRICE_3'] = row['ASK_PRICE_3']
    bid_ask_dict['ASK_PRICE_4'] = row['ASK_PRICE_4']
    bid_ask_dict['ASK_PRICE_5'] = row['ASK_PRICE_5']
    bid_ask_dict['ASK_QTY_1'] = row['ASK_QTY_1']
    bid_ask_dict['ASK_QTY_2'] = row['ASK_QTY_2']
    bid_ask_dict['ASK_QTY_3'] = row['ASK_QTY_3']
    bid_ask_dict['ASK_QTY_4'] = row['ASK_QTY_4']
    bid_ask_dict['ASK_QTY_5'] = row['ASK_QTY_5']

    BID_PRICE, ASK_PRICE = parent_utils.getWAPBidAskPrice(bid_ask_dict, row['QUANTITY'])

    return BID_PRICE, ASK_PRICE


def transformationPositionsDataframe(df_positions_existing, symbol, time_to_expiry_options_252,
                                                                           time_to_expiry_options_365,
                                                                           spot_value, squareOff_dict, Configuration):
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
    df_positions_tracking = pd.DataFrame()

    #######################################  ORDER MANIFEST - ADD TRADE DATE ###########################################
    if utils.isBackTestingEnv(Configuration):
        trade_date = ""
        pass
    else:
        trade_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%b%d').upper()
    ####################################################################################################################

    # SQUAREOFF JOB ID
    position_tracking_group_id = UniqueKeyGenerator().getUniqueKeyDateTime()
    squareOff_dict['position_tracking_group_id'] = position_tracking_group_id

    # Convert all columns to lowercase
    df_positions_existing.columns = [x.lower() for x in df_positions_existing.columns]

    # USING 2 DATAFRAMES TO CREATE df_Squareoff_Positions
    # 1. df_Put_Call --> Having Only One Row with Current Values
    # 2. df_positions_existing --> Existing Positions DF with Multiple Rows for PUT / CALL / FUTURES
    for index, row in df_positions_existing.iterrows():
        INSTRUMENT_TYPE_UPPERCASE = row['instrument_type'].value
        INSTRUMENT_TYPE_LOWERCASE = INSTRUMENT_TYPE_UPPERCASE.lower()

        # ENTRY PARAMS
        entry_params = json.loads(row['params'])
        entry_atm_put_price = entry_params['entry_atm_put_price']
        entry_atm_call_price = entry_params['entry_atm_call_price']
        entry_atm_avg_price = entry_params['entry_atm_avg_price']
        entry_atm_price_diff = entry_params['entry_atm_price_diff']
        entry_vix = entry_params['entry_vix']
        entry_underlying = entry_params['entry_underlying']
        if INSTRUMENT_TYPE_UPPERCASE == "FUTURES":
            entry_delta = 0.0
            entry_iv = 0.0
            entry_gamma = 0.0
            entry_theta = 0.0
            entry_vega = 0.0
            entry_net_delta = 0.0
            entry_net_delta_overall = 0.0
            entry_iv_diff_pct = 0.0
            entry_time_to_expiry_options = 0.0
            entry_net_gamma = 0.0
            entry_net_theta = 0.0
            entry_net_vega = 0.0
            order_manifest = str(int(row['strike_price']))+'_'+row['instrument_type'].value+'_'+row['transaction_type'].value
            order_manifest = trade_date + '_' + order_manifest
            # IVs ADDED TO DICT
            squareOff_dict['ENTRY_IV_' + INSTRUMENT_TYPE_UPPERCASE] = entry_iv
            squareOff_dict['CURRENT_IV_' + INSTRUMENT_TYPE_UPPERCASE] = row['iv']
            # POPULATE PRICE PNL PCT
            current_price_pnl_pct = -1 * utils.get_percentage_change(row['current_price'], row['entry_price'])
            squareOff_dict['CURRENT_PRICE_PNL_PCT_' + INSTRUMENT_TYPE_UPPERCASE] = 0.0
        else:
            entry_delta = entry_params['entry_delta_' + INSTRUMENT_TYPE_LOWERCASE]
            entry_iv = entry_params['entry_iv_' + INSTRUMENT_TYPE_LOWERCASE]
            entry_gamma = entry_params['entry_gamma_' + INSTRUMENT_TYPE_LOWERCASE]
            entry_theta = entry_params['entry_theta_' + INSTRUMENT_TYPE_LOWERCASE]
            entry_vega = entry_params['entry_vega_' + INSTRUMENT_TYPE_LOWERCASE]
            entry_net_delta = entry_params['entry_net_delta']
            entry_net_delta_overall = entry_params['entry_net_delta_overall']
            entry_iv_diff_pct = entry_params['entry_iv_diff_pct']
            entry_time_to_expiry_options = entry_params['entry_time_to_expiry_options']
            entry_net_gamma = entry_params['entry_net_gamma']
            entry_net_theta = entry_params['entry_net_theta']
            entry_net_vega = entry_params['entry_net_vega']
            order_manifest = entry_params['order_manifest']

            # IVs ADDED TO DICT
            squareOff_dict['ENTRY_IV_' + INSTRUMENT_TYPE_UPPERCASE] = entry_iv
            squareOff_dict['CURRENT_IV_' + INSTRUMENT_TYPE_UPPERCASE] = row['iv']
            # POPULATE PRICE PNL PCT
            current_price_pnl_pct = -1 * utils.get_percentage_change(row['current_price'], row['entry_price'])
            squareOff_dict['CURRENT_PRICE_PNL_PCT_' + INSTRUMENT_TYPE_UPPERCASE] = current_price_pnl_pct

        ################################## TIME TO EXPIRY POPULATION ###################################################
        time_to_expiry_options = 252

        # POSITION TRACKING PARAMS
        params_dict = json.loads(squareOff_dict['params'])
        #params_dict = dict(list(params_dict.items()) + list(entry_params.items()))
        params_dict['entry_atm_put_price'] = entry_atm_put_price
        params_dict['entry_atm_call_price'] = entry_atm_call_price
        params_dict['entry_atm_avg_price'] = entry_atm_avg_price
        params_dict['entry_atm_price_diff'] = entry_atm_price_diff
        params_dict['entry_vix'] = entry_vix
        params_dict['entry_underlying'] = entry_params['entry_underlying']
        params = json.dumps(params_dict)
        squareOff_dict['params'] = params



        ################################################################################################################

        df_positions_tracking = df_positions_tracking.append(
        {   'signal_id': row['signal_id'],
            'signal_group_id': row['signal_group_id'],
            'position_id': row['position_id'],
            'position_group_id': row['position_group_id'],
            'position_tracking_id': UniqueKeyGenerator().getUniqueKeyDateTime(),
            'order_id': str(int(UniqueKeyGenerator().getUniqueKeyDateTime())),
            'position_tracking_group_id': position_tracking_group_id,
            'moneyness': row['moneyness'],
            'symbol': symbol,
            'expiry_date': row['expiry_date'],
            'strike_price': int(row['strike_price']),
            'instrument_type': row['instrument_type'],
            'transaction_type': row['transaction_type'],
            'order_type': row['order_type'],
            'contract_type': row['contract_type'],
            'num_lots': row['num_lots'],
            'lot_size': row['lot_size'],
            'quantity': row['quantity'],
            'is_active': row['is_active'],
            'is_success': True,
            'is_square_off': row['is_square_off'],
            'margin_overall': row['margin_overall'],
            'entry_price': row['entry_price'],
            'exit_price': row['exit_price'],
            'current_price': row['current_price'],
            'execution_price': row['current_price'],
            'params': params,
            'unrealized_pnl': row['unrealized_pnl'],
            'realized_pnl': row['realized_pnl'],
            'unrealized_pnl_group': row['unrealized_pnl_group'],
            'realized_pnl_group': row['realized_pnl_group'],
            'net_pnl_overall': row['net_pnl_overall'],
            'realized_pnl_overall': row['realized_pnl_overall'],
            'entry_delta': entry_delta,
            'entry_net_delta': entry_net_delta,
            'entry_net_delta_overall': entry_net_delta_overall,
            'current_delta': row['delta'],
            'current_net_delta': row['net_delta'],
            'current_net_delta_overall': row['net_delta_overall'],
            'entry_gamma': entry_gamma,
            'entry_net_gamma': entry_net_gamma,
            'current_gamma': row['gamma'],
            'current_net_gamma': row['net_gamma'],
            'entry_iv': entry_iv,
            'entry_iv_diff_pct': entry_iv_diff_pct,
            'current_iv': row['iv'],
            'current_iv_diff_pct': 'NA',
            'net_delta_threshold': 'NA',
            'entry_theta': entry_theta,
            'current_theta': row['theta'],
            'entry_net_theta': entry_net_theta,
            'current_net_theta': row['net_theta'],
            'entry_vega': entry_vega,
            'current_vega': row['vega'],
            'entry_net_vega': entry_net_vega,
            'current_net_vega': row['net_vega'],
            'contract_expiry_date': row['expiry_date'],
            'entry_time_to_expiry': entry_time_to_expiry_options,
            'current_time_to_expiry': time_to_expiry_options,
            'order_manifest': order_manifest,
            'time_value_options': row['time_value_options'],
            'entry_underlying': entry_underlying,
            'current_underlying': spot_value,
            'expected_theta_pnl_pending': 'NA',
            'current_theta_pnl_pending': row['current_theta_pnl_pending'],
            'net_pnl_threshold': row['net_pnl_threshold'],
            'entry_atm_put_price': entry_atm_put_price,
            'entry_atm_call_price': entry_atm_call_price,
            'entry_atm_avg_price': entry_atm_avg_price,
            'entry_atm_price_diff': entry_atm_price_diff,
            'current_price_pnl_pct': current_price_pnl_pct,
            'entry_vix': entry_vix,
            'broker_order_id': "NA",
            'broker_order_status': "NA"
            }, ignore_index=True)

    return df_positions_tracking


def addBidAskPricesMDS(df_Put_Call, expiry_date, symbol, Configuration, orderGen_dict, isValidationRequired = True, isSquareOffJob = False):
    df_put_existing = orderGen_dict['df_put_existing']
    df_call_existing = orderGen_dict['df_call_existing']

    PRICE_THRESHOLD = float(Configuration['PRICE_THRESHOLD'])
    BID_ASK_THRESHOLD = float(Configuration['BID_ASK_THRESHOLD'])
    BID_ASK_THRESHOLD_ABSOLUTE = float(Configuration['BID_ASK_THRESHOLD_ABSOLUTE'])
    df_Put_Call['STRIKE_PRICE'] = df_Put_Call['STRIKE_PRICE'].astype(float)
    df_Put = df_Put_Call[df_Put_Call['INSTRUMENT_TYPE'] == InstrumentTypeEnum.PUT]
    df_Call = df_Put_Call[df_Put_Call['INSTRUMENT_TYPE'] == InstrumentTypeEnum.CALL]
    df_Put['STRIKE_PRICE'] = df_Put['STRIKE_PRICE'].astype(int)
    df_Call['STRIKE_PRICE'] = df_Call['STRIKE_PRICE'].astype(int)
    # SP_LIST_PUT = df_Put['STRIKE_PRICE'].tolist()
    # SP_LIST_PUT = [int(i) for i in SP_LIST_PUT]
    # SP_LIST_CALL = df_Call['STRIKE_PRICE'].tolist()
    # SP_LIST_CALL = [int(i) for i in SP_LIST_CALL]

    ############################ CALL OPTION CHAIN LITE MARKET DEPTH PUT ###############################################
    mdsAdaptor = utils.getMDSAdaptor(Configuration)
    df_Market_Depth_Put_Final = pd.DataFrame()

    # ADD MARKET DEPTH PUT IF PUT NOT SQUARED OFF
    if len(df_put_existing) > 0:
        #pool = ThreadPoolExecutor(max_workers=3)
        for index, row in df_Put.iterrows():
            df_Market_Depth_Put = mdsAdaptor.getOptionChainLiteMarketDepth(Configuration,
                                              [row['STRIKE_PRICE']], 'PE', row['EXPIRY_DATE'], symbol)

            remove_column_list = ['buy', 'sell']
            df_Market_Depth_Put.drop(remove_column_list, axis=1, inplace=True)
            df_Market_Depth_Put.columns = [str(col).upper() for col in df_Market_Depth_Put.columns]
            df_Market_Depth_Put['STRIKE_PRICE'] = df_Market_Depth_Put.index
            df_Market_Depth_Put.reset_index(inplace=True)
            df_Market_Depth_Put.drop(['index'], axis=1, inplace=True)
            df_Market_Depth_Put['STRIKE_PRICE'] = df_Market_Depth_Put['STRIKE_PRICE'].astype(float)
            df_Market_Depth_Put['EXPIRY_DATE'] = row['EXPIRY_DATE']
            df_Market_Depth_Put_Final = df_Market_Depth_Put_Final.append(df_Market_Depth_Put, ignore_index=True)
        #pool.shutdown(wait=True)

        # df_Market_Depth_Put_Final.drop_duplicates(subset='STRIKE_PRICE', keep="last", inplace = True)
        df_Put = pd.merge(df_Put, df_Market_Depth_Put_Final, how='inner', on=['STRIKE_PRICE', 'EXPIRY_DATE'])
        df_Put_Call = df_Put

    ############################## CALL OPTION CHAIN LITE MARKET DEPTH CALL ###########################################
    if len(df_call_existing) > 0:
        df_Market_Depth_Call_Final = pd.DataFrame()

        #pool = ThreadPoolExecutor(max_workers=3)
        for index, row in df_Call.iterrows():
            df_Market_Depth_Call = mdsAdaptor.getOptionChainLiteMarketDepth(Configuration,
                                               [row['STRIKE_PRICE']], 'CE', row['EXPIRY_DATE'], symbol)

            remove_column_list = ['buy', 'sell']
            df_Market_Depth_Call.drop(remove_column_list, axis=1, inplace=True)
            df_Market_Depth_Call.columns = [str(col).upper() for col in df_Market_Depth_Call.columns]
            df_Market_Depth_Call['STRIKE_PRICE'] = df_Market_Depth_Call.index
            df_Market_Depth_Call.reset_index(inplace=True)
            df_Market_Depth_Call.drop(['index'], axis=1, inplace=True)
            df_Market_Depth_Call['STRIKE_PRICE'] = df_Market_Depth_Call['STRIKE_PRICE'].astype(float)
            df_Market_Depth_Call['EXPIRY_DATE'] = row['EXPIRY_DATE']
            df_Market_Depth_Call_Final = df_Market_Depth_Call_Final.append(df_Market_Depth_Call, ignore_index=True)
        #pool.shutdown(wait=True)

        # df_Market_Depth_Call_Final.drop_duplicates(subset='STRIKE_PRICE', keep="last", inplace=True)
        df_Call = pd.merge(df_Call, df_Market_Depth_Call_Final, how='inner', on=['STRIKE_PRICE', 'EXPIRY_DATE'])

        if len(df_Put_Call) > 0:
            df_Put_Call = pd.concat([df_Put, df_Call], ignore_index=True)
        else:
            df_Put_Call = df_Call

    # # ADD MARKET DEPTH PUT IF PUT NOT SQUARED OFF
    # if len(df_put_existing) > 0:
    #     pool = ThreadPoolExecutor(max_workers=3)
    #     for index, row in df_Put.iterrows():
    #         df_Market_Depth_Put = pool.submit(mdsAdaptor.getOptionChainLiteMarketDepth, Configuration, [row['STRIKE_PRICE']], 'PE', row['EXPIRY_DATE'], symbol).result()
    #
    #         remove_column_list = ['buy', 'sell']
    #         df_Market_Depth_Put.drop(remove_column_list, axis=1, inplace=True)
    #         df_Market_Depth_Put.columns = [str(col).upper() for col in df_Market_Depth_Put.columns]
    #         df_Market_Depth_Put['STRIKE_PRICE'] = df_Market_Depth_Put.index
    #         df_Market_Depth_Put.reset_index(inplace=True)
    #         df_Market_Depth_Put.drop(['index'], axis=1, inplace=True)
    #         df_Market_Depth_Put['STRIKE_PRICE'] = df_Market_Depth_Put['STRIKE_PRICE'].astype(float)
    #         df_Market_Depth_Put['EXPIRY_DATE'] = row['EXPIRY_DATE']
    #         df_Market_Depth_Put_Final = df_Market_Depth_Put_Final.append(df_Market_Depth_Put, ignore_index=True)
    #     pool.shutdown(wait=True)
    #
    #     #df_Market_Depth_Put_Final.drop_duplicates(subset='STRIKE_PRICE', keep="last", inplace = True)
    #     df_Put = pd.merge(df_Put, df_Market_Depth_Put_Final, how='inner', on=['STRIKE_PRICE','EXPIRY_DATE'])
    #     df_Put_Call = df_Put
    #
    # ############################## CALL OPTION CHAIN LITE MARKET DEPTH CALL ###########################################
    # if len(df_call_existing) > 0:
    #     df_Market_Depth_Call_Final = pd.DataFrame()
    #
    #     pool = ThreadPoolExecutor(max_workers=3)
    #     for index, row in df_Call.iterrows():
    #         df_Market_Depth_Call = pool.submit(mdsAdaptor.getOptionChainLiteMarketDepth, Configuration, [row['STRIKE_PRICE']], 'CE', row['EXPIRY_DATE'], symbol).result()
    #
    #         remove_column_list = ['buy', 'sell']
    #         df_Market_Depth_Call.drop(remove_column_list, axis=1, inplace=True)
    #         df_Market_Depth_Call.columns = [str(col).upper() for col in df_Market_Depth_Call.columns]
    #         df_Market_Depth_Call['STRIKE_PRICE'] = df_Market_Depth_Call.index
    #         df_Market_Depth_Call.reset_index(inplace=True)
    #         df_Market_Depth_Call.drop(['index'], axis=1, inplace=True)
    #         df_Market_Depth_Call['STRIKE_PRICE'] = df_Market_Depth_Call['STRIKE_PRICE'].astype(float)
    #         df_Market_Depth_Call['EXPIRY_DATE'] = row['EXPIRY_DATE']
    #         df_Market_Depth_Call_Final = df_Market_Depth_Call_Final.append(df_Market_Depth_Call, ignore_index=True)
    #     pool.shutdown(wait=True)
    #
    #     #df_Market_Depth_Call_Final.drop_duplicates(subset='STRIKE_PRICE', keep="last", inplace=True)
    #     df_Call = pd.merge(df_Call, df_Market_Depth_Call_Final, how='inner', on=['STRIKE_PRICE','EXPIRY_DATE'])
    #
    #     if len(df_Put_Call) > 0:
    #         df_Put_Call = pd.concat([df_Put, df_Call], ignore_index=True)
    #     else:
    #         df_Put_Call = df_Call

    ################################ VALIDATION ON BID ASK #############################################################
    # if not isSquareOffJob and isValidationRequired and 'BID_PRICE_1_PUT' in df_Put_Call.columns:  # IN CASE DEPTH IS NOT THERE IN OPTION CHAIN
    #     df_positions_existing = orderGen_dict['df_positions_existing']
    #
    #     # FILTERATION RULE 2: REMOVE EXISTING STRIKE PRICES OR ACTIVE ORDERS
    #     if not df_positions_existing.empty:
    #         #SP_ATM = df_positions_existing['strike_price'].iloc[0] # TAKE POSITION IN SAME STRIKE PRICE
    #         SP_List = df_positions_existing['strike_price_put'].tolist()
    #         SP_List = [float(SP) for SP in SP_List]
    #         df_Put_Call = df_Put_Call[~df_Put_Call['STRIKE_PRICE'].isin(SP_List)]
    #         df_Put_Call.reset_index(drop=True, inplace=True)
    #
    #     ##################################### SELECT 1ST AND LAST ROW ##################################################
    #     #df_Put_Call = df_Put_Call.iloc[[0, len(df_Put_Call) - 1]]
    #     if df_Put_Call is None or len(df_Put_Call) == 0: return
    #
    #     df_Put_Call = df_Put_Call[(df_Put_Call[['BID_PRICE_1_PUT', 'ASK_PRICE_1_PUT',
    #                                             'BID_PRICE_1_CALL', 'ASK_PRICE_1_CALL']] > PRICE_THRESHOLD).all(axis=1)]
    #     df_Put_Call.reset_index(drop=True, inplace=True)
    #
    #     if df_Put_Call is None or len(df_Put_Call) == 0: return
    #
    #     # FILERATION  RULE 3: BID ASK DIFFERENCE CAN NOT BE GREATER THAN 2 PCT
    #     df_Put_Call['BID_ASK_DIFF_PUT'] = df_Put_Call.apply(lambda row: ((row.ASK_PRICE_1_PUT - row.BID_PRICE_1_PUT) / row.BID_PRICE_1_PUT) * 100, axis=1)
    #     df_Put_Call['BID_ASK_DIFF_CALL'] = df_Put_Call.apply(lambda row: ((row.ASK_PRICE_1_CALL - row.BID_PRICE_1_CALL) / row.BID_PRICE_1_CALL) * 100, axis=1)
    #     df_Put_Call = df_Put_Call[(df_Put_Call[['BID_ASK_DIFF_PUT', 'BID_ASK_DIFF_CALL']] < BID_ASK_THRESHOLD).all(axis=1)]
    #     df_Put_Call.reset_index(drop=True, inplace=True)
    #
    #     if df_Put_Call is None or len(df_Put_Call) == 0: return
    #
    #     # FILERATION  RULE 4: BID ASK DIFFERENCE ABSOLUTE CAN NOT BE GREATER THAN 2
    #     df_Put_Call['BID_ASK_DIFF_PUT_ABSOLUTE'] = df_Put_Call.apply(lambda row: (row.ASK_PRICE_1_PUT - row.BID_PRICE_1_PUT), axis=1)
    #     df_Put_Call['BID_ASK_DIFF_CALL_ABSOLUTE'] = df_Put_Call.apply(lambda row: (row.ASK_PRICE_1_CALL - row.BID_PRICE_1_CALL), axis=1)
    #     df_Put_Call = df_Put_Call[(df_Put_Call[['BID_ASK_DIFF_PUT_ABSOLUTE','BID_ASK_DIFF_CALL_ABSOLUTE']] <= BID_ASK_THRESHOLD_ABSOLUTE).all(axis=1)]
    #     df_Put_Call.reset_index(drop=True, inplace=True)
    #print(df_Put_Call)
    return df_Put_Call

def populateExecutionPricesWAPFutures(df_Put_Call):
    for index, row in df_Put_Call.iterrows():
        if row['ORDER_TYPE_FUTURES'] == 'BUY':
            df_Put_Call.at[index, 'EXECUTION_PRICE_FUTURES'] = row['ASK_PRICE_FUTURES']
            df_Put_Call.at[index, 'EXECUTION_PRICE_FUTURES_ACTUAL'] = row['ASK_PRICE_FUTURES']
            df_Put_Call.at[index, 'CURRENT_PRICE_FUTURES'] = row['BID_PRICE_FUTURES']
            df_Put_Call.at[index, 'CURRENT_PRICE_FUTURES_ACTUAL'] = row['BID_PRICE_FUTURES']

        elif row['ORDER_TYPE_FUTURES'] == 'SELL':
            df_Put_Call.at[index, 'EXECUTION_PRICE_FUTURES'] = row['BID_PRICE_FUTURES']
            df_Put_Call.at[index, 'EXECUTION_PRICE_FUTURES_ACTUAL'] = row['BID_PRICE_FUTURES']
            df_Put_Call.at[index, 'CURRENT_PRICE_FUTURES'] = row['ASK_PRICE_FUTURES']
            df_Put_Call.at[index, 'CURRENT_PRICE_FUTURES_ACTUAL'] = row['ASK_PRICE_FUTURES']

    return df_Put_Call

def calculateQuantity(LOT_SIZE, MF_IA):
    return float(LOT_SIZE) * float(MF_IA)

def populateNetDelta(squareOff_dict, Configuration):
    df_positions_existing = squareOff_dict['df_positions_existing']

    NET_DELTA_OVERALL_OPTIONS = float(df_positions_existing['NET_DELTA'].iloc[0])

    df_positions_existing['NET_DELTA_OVERALL'] = NET_DELTA_OVERALL_OPTIONS

    squareOff_dict['df_positions_existing'] = df_positions_existing
    return squareOff_dict

def populatePnlBeforeRealignment(position_group_id, symbol):
    REALIZED_PNL_OVERALL_BEFORE_REALIGNMENT = 0.0
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')
    df_non_active_positions_options = PositionsDAO.getNonActivePositionsByTransIdSymbolAndInstrumentType(position_group_id, symbol, "PUT")

    # FILTER BASED ON DATE/ORDER_TYPE
    df_non_active_positions_options = df_non_active_positions_options[
        df_non_active_positions_options['order_type'].str.contains("SELL") &
        df_non_active_positions_options['created_date'].str.contains(current_date)]

    # SORTING BASED ON DATE
    if len(df_non_active_positions_options) > 0:
        df_non_active_positions_options.sort_values(by=['created_date'], inplace=True, ascending=False)

    # POPULATE ATM THRESHOLD AND REALIZED_PNL_OVERALL_BEFORE_REALIGNMENT
    if len(df_non_active_positions_options) > 0:
        REALIZED_PNL_OVERALL_BEFORE_REALIGNMENT = float(df_non_active_positions_options['realized_pnl_overall'].iloc[0])

    return REALIZED_PNL_OVERALL_BEFORE_REALIGNMENT


def populateVIXGreeksIV(symbol, Configuration, df_Put_Call, orderGen_dict):

    # POPULATE VIX
    mdsAdaptor = utils.getMDSAdaptor(Configuration)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata'))
    current_date_str = current_date.strftime('%Y-%m-%d')
    current_time_str = current_date.strftime('%H:%M')
    toDateTime = current_date_str + '+' + current_time_str+':00'
    fromDateTime = toDateTime

    # 2017-12-15+09:15:00
    VIX = mdsAdaptor.callHistoricalPricesAPIForVIX(fromDateTime, toDateTime, Configuration, symbol="VIX")
    df_Put_Call['VIX'] = VIX
    orderGen_dict['VIX'] = VIX

    # POPULATE GREEKS
    expiry_date_greeks = orderGen_dict['expiry_date']
    expiry_date_greeks = datetime.strptime(expiry_date_greeks, '%y%b%d').strftime('%Y%b%d')
    current_date_greeks = current_date.strftime('%Y%b%d')
    current_time_greeks = current_date.strftime('%H:%M')

    for index, row in df_Put_Call.iterrows():
        instrument_type = getInstrumentTypeGreeks(row['INSTRUMENT_TYPE'].value)
        option_price = row['CURRENT_PRICE']
        underlying_spot = orderGen_dict['spot_value']
        strike_price = row['STRIKE_PRICE']
        order_type = row['TRANSACTION_TYPE'].value
        num_lots = row['NUM_LOTS']
        greeksSign = 1

        # ADD SIGN TO DELTA
        # FOR LONG DELTA POSITIVE, FOR SHORT DELTA NEGATIVE
        if order_type == 'SELL':
            greeksSign = -1

        # CALL GREEKS API
        iv, delta, gamma, theta, vega = GreeksAdaptor.fetchGreeks(expiry_date_greeks, current_date_greeks, current_time_greeks, instrument_type,
                                  option_price,underlying_spot,strike_price,order_type, Configuration, raiseException=False)

        # ADD SIGN TO DELTA
        # FOR LONG DELTA POSITIVE, FOR SHORT DELTA NEGATIVE
        # if (instrument_type == 'CE' and order_type == 'SELL') or (instrument_type == 'PE' and order_type == 'BUY'):
        #     delta = -delta

        # UPDATE GREEKS WITH LOTS AND SIGN
        delta = greeksSign * delta * num_lots
        gamma = greeksSign * gamma * num_lots
        theta = greeksSign * theta * num_lots
        vega = greeksSign * vega * num_lots

        # ADD GREEKS TO DATAFRAME
        df_Put_Call.at[index, 'IV'] = iv
        df_Put_Call.at[index, 'DELTA'] = delta
        df_Put_Call.at[index, 'GAMMA'] = gamma
        df_Put_Call.at[index, 'THETA'] = theta
        df_Put_Call.at[index, 'VEGA'] = vega

    # NET GREEKS VALUES
    df_Put_Call['NET_DELTA'] = df_Put_Call['DELTA'].sum()
    df_Put_Call['NET_GAMMA'] = df_Put_Call['GAMMA'].sum()
    df_Put_Call['NET_THETA'] = df_Put_Call['THETA'].sum()
    df_Put_Call['NET_VEGA'] = df_Put_Call['VEGA'].sum()

    return df_Put_Call

def getInstrumentTypeGreeks(instrument_type):
    if instrument_type == 'PUT':
        return 'PE'
    else:
        return 'CE'

def populateExecutionThetaPnlPending(row, Configuration, orderGen_dict):
    #EXISTING_EXECUTION_PNL_PENDING = orderGen_dict['EXISTING_EXECUTION_PNL_PENDING']
    symbol = orderGen_dict['symbol']
    NET_PNL_PENDING = utils.calculateThetaPnlPending(row['NET_THETA'], Configuration, symbol)

    row['NET_PNL_PENDING'] = NET_PNL_PENDING
    return row

def populateIndividualTheta(row):
    row['THETA_INDIVIDUAL'] = 0.0
    if 'INSTRUMENT_TYPE' in row.keys():
        if row['INSTRUMENT_TYPE'] == 'PUT':
            row['THETA_INDIVIDUAL'] = row['THETA_PUT']
        else:
            row['THETA_INDIVIDUAL'] = row['THETA_CALL']

    return row

def populateIndividualVega(row):
    row['VEGA_INDIVIDUAL'] = 0.0
    if 'INSTRUMENT_TYPE' in row.keys():
        if row['INSTRUMENT_TYPE'] == 'PUT':
            row['VEGA_INDIVIDUAL'] = row['VEGA_PUT']
        else:
            row['VEGA_INDIVIDUAL'] = row['VEGA_CALL']

    return row

if __name__ == "__main__":
   print('')