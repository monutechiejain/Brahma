
from dao import ClientConfigurationDAO, PositionsDAO, TrackerDAO
import pandas as pd
import numpy as np
from adaptor import OMRequestProcessor
from helper import OrderGenOptionsHelper
from common import utils
import logging
from entities.Enums import TransactionTypeEnum

REFRESH_INTERVAL = "100"

def getPositionTrackerDict(symbol, contract_type, current_underlying, tradeType, orderType,
                                          Configuration, position_group_id, margin_used_broker,
                                          execution_iv, current_iv, params, df_positions_tracking, current_time,
                                          squareOff_dict, current_price_pnl_pct):

    ######################################## POPULATE POSITIONS DATA IN DICT #######################################
    tracker_dict = {}
    tracker_dict["SYMBOL"] = symbol
    tracker_dict["ACCOUNT"] = Configuration['SCHEMA_NAME']
    tracker_dict["QTY_SELL"] = 0.0
    tracker_dict["SP_SELL"] = 0.0
    tracker_dict["MARGIN_USED_BROKER"] = 0.0
    tracker_dict["EXECUTION_IV"] = "NA"
    tracker_dict["CURRENT_IV"] = "NA"
    tracker_dict["PARAMS"] = "NA"
    tracker_dict["CURRENT_UNDERLYING"] = current_underlying
    tracker_dict["NET_PNL"] = 0.0
    tracker_dict["NET_PNL_PCT"] = 0.0
    tracker_dict["MARGIN_USED"] = 0.0
    tracker_dict["POSITIONS_CLOSED"] = "N"
    tracker_dict["POSITIONS_MATCHED"] = "Y"
    tracker_dict['LAST_UPDATED_TIME'] = "NA"
    tracker_dict['RAG'] = "G"
    tracker_dict["QTY_COUNT_DB"] = 0.0
    tracker_dict["QTY_COUNT_BROKER"] = 0.0
    tracker_dict["REFRESH_INTERVAL"] = REFRESH_INTERVAL
    tracker_dict["MARGIN_USED_BROKER"] = margin_used_broker
    tracker_dict["EXECUTION_IV"] = execution_iv
    tracker_dict["CURRENT_IV"] = current_iv
    tracker_dict["TARGET"] = float(Configuration['TARGET_THRESHOLD_PCT_' + symbol])
    tracker_dict["STOPLOSS"] = float(Configuration['STOPLOSS_THRESHOLD_PCT_' + symbol])
    tracker_dict["PARAMS"] = params

    ######################################## GET ALL POSITIONS FROM DB TO FETCH NET PNL ###############################
    # df_Positions_db_All = PositionsDAO.getActivePositionsByPositionsGroupIdAndSymbolAll(position_group_id, symbol)
    # if len(df_Positions_db_All) > 0:
    #
    #     if len(df_positions_tracking) > 0:
    #         tracker_dict["NET_PNL"] = round(float(df_positions_tracking['net_pnl_overall'].iloc[0]), 2)
    #         tracker_dict["MARGIN_USED"] = df_positions_tracking['margin_overall'].iloc[0]
    #         tracker_dict["QTY_SELL"] = int(float(df_positions_tracking['quantity'].iloc[0]))
    #
    #
    # ##################################### GET ACTIVE POSITIONS FROM DB #####################################
    df_Active_Positions_db = PositionsDAO.getActivePositionsBySymbolAndContractType(Configuration['SCHEMA_NAME'], symbol, contract_type)

    LOWER_SP = None
    UPPER_SP = None
    if len(df_Active_Positions_db) > 0:
        df_Active_Positions_db['quantity_db'] = df_Active_Positions_db['QUANTITY']
        df_Active_Positions_db['quantity_db'] = df_Active_Positions_db['quantity_db'].astype(float)
        df_Active_Positions_db.sort_values(by=['STRIKE_PRICE'], ascending=True, inplace=True)
        df_Active_Positions_db.reset_index(drop=True, inplace=True)
        LOWER_SP = df_Active_Positions_db['STRIKE_PRICE'].iloc[0]
        UPPER_SP = df_Active_Positions_db['STRIKE_PRICE'].iloc[-1]
        tracker_dict["QTY_COUNT_DB"] = df_Active_Positions_db['quantity_db'].sum()

    ################################################ TACKING POSITIONS #################################################
    if len(df_positions_tracking) > 0:
        tracker_dict["NET_PNL"] = round(float(df_positions_tracking['net_pnl_overall'].iloc[0]), 2)
        tracker_dict["MARGIN_USED"] = df_positions_tracking['margin_overall'].iloc[0]
        tracker_dict["QTY_SELL"] = int(float(df_positions_tracking['quantity'].iloc[0]))
        tracker_dict["SP_SELL"] = int(float(df_positions_tracking['strike_price'].iloc[0]))
        tracker_dict["LAST_UPDATED_TIME"] = current_time
        df_positions_tracking['quantity_db'] = df_positions_tracking['quantity']
        df_positions_tracking['quantity_db'] = df_positions_tracking['quantity_db'].astype(float)

        # COLUMNS TRANSFORMATION
        # df_Positions_db.columns = [x.lower() for x in df_Positions_db.columns]
        df_positions_tracking['strike_price'] = df_positions_tracking['strike_price'].astype(int).astype(str)
        # expiry_date_options, expiry_date_futures = OrderGenOptionsHelper.callExpiryDateAPI(Configuration,
        #                                                                                    contract_type)
        #
        # df_positions_tracking['tradingsymbol'] = df_positions_tracking.apply(
        #     lambda row: populateTradingSymbol(row, symbol, expiry_date_options ,expiry_date_futures), axis=1)
        # df_positions_tracking = df_positions_tracking[['position_id','quantity_db', 'tradingsymbol']]

    tracker_dict["POSITIONS_COUNT_DB"] = len(df_Active_Positions_db)
    ########################################################################################################


    ################################ GET ACTIVE POSITIONS FROM BROKER #####################################
    OM_URL = Configuration['OM_URL']
    activePositionsResponse = utils.getOMServiceCallHelper(Configuration).fetchActivePositions(Configuration, OM_URL
                                                                       ,orderType)
    df_ActivePositions = pd.DataFrame(activePositionsResponse)
    df_ActivePositions_reconcile = pd.DataFrame()

    if len(df_ActivePositions) > 0:
        # GET ONLY SELECTED TRADE TYPE e.g. MIS ONLY
        # FETCH STRIKE PRICES FROM TRADING SYMBOL
        df_ActivePositions['strike_price'] = df_ActivePositions['tradingsymbol'].str[-7:-2].astype(float)
        df_ActivePositions.sort_values(by=['strike_price'], ascending=True, inplace=True)

        df_ActivePositions = df_ActivePositions[(df_ActivePositions['product'] == tradeType) &
                                                (df_ActivePositions["tradingsymbol"].str.startswith(symbol
                                                                                                    ,na=False)) &
                                                (df_ActivePositions['quantity'] != 0)]
        df_ActivePositions['quantity_active'] = df_ActivePositions['quantity'].abs()
        df_ActivePositions['quantity_active'] = df_ActivePositions['quantity_active'].astype(float)
        df_ActivePositions.reset_index(drop=True, inplace=True)

        # SP FILTERING - LOWER SP AND UPPER SP, SO THAT WE AN NAKED TRADES FOR OPTION SELLING IF REQUIRED
        if LOWER_SP is not None or UPPER_SP is not None:
            df_ActivePositions = df_ActivePositions[(df_ActivePositions['strike_price'] >= LOWER_SP) &
                                                    (df_ActivePositions['strike_price'] <= UPPER_SP)]
            df_ActivePositions.sort_values(by=['strike_price'], ascending=True, inplace=True)
            df_ActivePositions.sort_index(inplace=True)
            df_ActivePositions.reset_index(drop=True, inplace=True)

        tracker_dict["QTY_COUNT_BROKER"] = df_ActivePositions['quantity_active'].sum()
        df_ActivePositions = df_ActivePositions[['quantity','quantity_active', 'tradingsymbol']]
        df_ActivePositions.reset_index(drop=True,inplace=True)
        #df_ActivePositions_reconcile = df_ActivePositions.copy()
    tracker_dict["POSITIONS_COUNT_BROKER"] = len(df_ActivePositions)
    ########################################################################################################

    ####################################### NO OF POSITIONS MISMATCH #######################################
    if len(df_Active_Positions_db) != len(df_ActivePositions):
        tracker_dict["POSITIONS_MATCHED"] = "N"
        tracker_dict['RAG'] = "R"

    if len(df_Active_Positions_db) == 0 and len(df_ActivePositions) == 0:
        tracker_dict["POSITIONS_CLOSED"] = "Y"
    #######################################################################################################

    ############################################# IF QTY MISMATCH, SET FLAG IN DICT ########################
    if len(df_Active_Positions_db) == len(df_ActivePositions) and len(df_positions_tracking) != 0 and len(df_ActivePositions) != 0:
        # df_ActivePositions = pd.merge(df_positions_tracking, df_ActivePositions, on='tradingsymbol',
        #                               how='inner')

        if len(df_Active_Positions_db) != len(df_ActivePositions):
            tracker_dict["POSITIONS_MATCHED"] = "N"
            tracker_dict['RAG'] = "R"

        if len(df_Active_Positions_db) == len(df_ActivePositions):
            df_ActivePositions['isMatched'] = np.where(df_ActivePositions['quantity_active'] ==
                                                       df_Active_Positions_db['quantity_db'], True, False)
            df_Not_Matched = df_ActivePositions[df_ActivePositions['isMatched'] == False]
            df_Not_Matched.reset_index(drop=True, inplace=True)

            if len(df_Not_Matched) > 0:
                tracker_dict["POSITIONS_MATCHED"] = "N"
                tracker_dict['RAG'] = "R"
    ####################################################################################################################

    ############################### CALCULATE NET_PNL_OVERALL_PCT ######################################################
    NET_PNL = tracker_dict["NET_PNL"]
    MARGIN_USED = float(tracker_dict["MARGIN_USED"])
    if NET_PNL != 0.0 and MARGIN_USED != 0.0:
        NET_PNL_PCT = round(utils.percentageIsXofY(NET_PNL, MARGIN_USED), 2)
        tracker_dict["NET_PNL_PCT"] = NET_PNL_PCT

    ############################## ADJUSTMENT RAG ######################################################################
    if not squareOff_dict['isSquareOff'] and tracker_dict['RAG'] == "G":
        ADJUSTMENT_PRICE_THRESHOLD_ALERT = 190
        # ADJUSTMENT STATUS
        isCallAdjustmentDone = squareOff_dict["isCallAdjustmentSquareOffDone"]
        isPutAdjustmentDone = squareOff_dict["isPutAdjustmentSquareOffDone"]
        #CURRENT_PRICE_CALL_LEG_3 = squareOff_dict["CURRENT_PRICE_CALL_LEG_3"]
        #CURRENT_PRICE_PUT_LEG_3 = squareOff_dict["CURRENT_PRICE_PUT_LEG_3"]
        # CASE 1: PUT ADJUSTMENT OR CALL ADJUSTMENT
        if isCallAdjustmentDone:
            tracker_dict['RAG'] = "CAD"
        elif isPutAdjustmentDone:
            tracker_dict['RAG'] = "PAD"

        # # CASE 2: CALL ADJUSTMENT ALERT OR PUT ADJUSTMENT ALERT
        # if not isCallAdjustmentDone and CURRENT_PRICE_CALL_LEG_3 >= ADJUSTMENT_PRICE_THRESHOLD_ALERT:
        #     tracker_dict['RAG'] = "CAA"
        # elif not isPutAdjustmentDone and CURRENT_PRICE_PUT_LEG_3 >= ADJUSTMENT_PRICE_THRESHOLD_ALERT:
        #     tracker_dict['RAG'] = "PAA"
        #
        # # CASE 3: CALL ADJUSTMENT AND PUT ADJUSTMENT
        # if isCallAdjustmentDone and isPutAdjustmentDone:
        #     tracker_dict['RAG'] = "PACAD"

    # CONVERT EVERYTHING IN STRING
    for key in tracker_dict:
        tracker_dict[key] = str(tracker_dict[key])

    ############################################## RECONCILIATION SQUAREOFF ############################################
    # if tracker_dict['RAG'] == "R" and len(df_ActivePositions_reconcile) > 0 and len(df_Positions_db) > 0:
    #     try:
    #         ReconciliationUtil.reconciliationSquareoff(df_Positions_db, df_ActivePositions_reconcile, Configuration, OM_URL,
    #                                                symbol, position_group_id)
    #     except Exception as ex:
    #         template = "Exception {} occurred while RECONCILIATION SQUAREOFF  with  message : {}"
    #         message = template.format(type(ex).__name__, ex.args)
    #         print(message)
    #         logging.info(traceback.format_exc())
    #         print(traceback.format_exc())
    #         logging.info(message)

    # UPDATE RAG WITH MORE INFO
    tracker_dict['RAG'] = tracker_dict['RAG']+current_price_pnl_pct

    return tracker_dict

def populateTradingSymbol(row, symbol, expiry_date_options ,expiry_date_futures):

    futures_symbol, put_symbol, call_symbol = OMRequestProcessor.populateTradingSymbol(symbol, expiry_date_options,
                                                                                       expiry_date_futures, row['strike_price'])

    # POPULATE SYMBOL
    if row['instrument_type'].value == 'PUT':
        trading_symbol = put_symbol
    else:
        trading_symbol = call_symbol

    return trading_symbol

def insertTrackerPositions(Configuration, squareOff_dict):
    # CONFIGURATION CHECK
    #Configuration = ClientConfigurationDAO.getConfigurations(Configuration['SCHEMA_NAME'])
    tradeType = Configuration['TRADE_TYPE']
    orderType = "POSITIONS_MATCH"
    symbol = squareOff_dict['symbol']
    position_group_id = squareOff_dict['position_group_id']
    contract_type = squareOff_dict['contract_type']
    current_underlying = squareOff_dict['spot_value']
    margin_used_broker = round(squareOff_dict['utilised'],2)

    execution_iv, current_iv, current_price_pnl_pct= 0.0, 0.0, 0.0
    if 'ENTRY_IV_PUT' in squareOff_dict and 'ENTRY_IV_CALL' in squareOff_dict:
        execution_iv = str(round(squareOff_dict['ENTRY_IV_PUT'],4))+', '+str(round(squareOff_dict['ENTRY_IV_CALL'],4))
        current_iv = str(round(squareOff_dict['CURRENT_IV_PUT'],4))+', '+str(round(squareOff_dict['CURRENT_IV_CALL'],4))
        current_price_pnl_pct = '('+str(round(squareOff_dict['CURRENT_PRICE_PNL_PCT_PUT'],2))+','+str(round(squareOff_dict['CURRENT_PRICE_PNL_PCT_CALL'],2))+')'
    elif 'ENTRY_IV_PUT' in squareOff_dict:
        execution_iv = str(round(squareOff_dict['ENTRY_IV_PUT'],4))
        current_iv = str(round(squareOff_dict['CURRENT_IV_PUT'],4))
        current_price_pnl_pct = '('+str(round(squareOff_dict['CURRENT_PRICE_PNL_PCT_PUT'], 2))+')'
    elif 'ENTRY_IV_CALL' in squareOff_dict:
        execution_iv = str(round(squareOff_dict['ENTRY_IV_CALL'],4))
        current_iv = str(round(squareOff_dict['CURRENT_IV_CALL'],4))
        current_price_pnl_pct = '('+str(round(squareOff_dict['CURRENT_PRICE_PNL_PCT_CALL'], 2))+')'

    params = 'NA'
    df_positions_tracking = squareOff_dict['df_positions_tracking']
    current_time = squareOff_dict['current_time']


    tracker_dict = getPositionTrackerDict(symbol, contract_type, current_underlying, tradeType, orderType,
                                          Configuration, position_group_id, margin_used_broker,
                                          execution_iv, current_iv, params, df_positions_tracking, current_time, squareOff_dict, current_price_pnl_pct)


    if tracker_dict['LAST_UPDATED_TIME'] != 'NA':
        TrackerDAO.insertOrUpdateTrackerPositions(Configuration['SCHEMA_NAME'], tracker_dict, symbol)
    else:
        TrackerDAO.insertOrUpdateTrackerPositionsWithoutTime(Configuration['SCHEMA_NAME'], tracker_dict, symbol)

    logging.info("{}: Tracker Updated Successfully for symbol : {}".format(Configuration['SCHEMA_NAME'], symbol))
