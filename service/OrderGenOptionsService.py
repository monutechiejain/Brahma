import pandas as pd
from helper import OrderGenOptionsHelper, LotsCalculatorHelper
from dao import PositionsDAO
from rules import OrderGenRuleEngine
from common import utils
import logging
import traceback
from datetime import datetime
import pytz


desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',15)
pd.set_option('display.max_rows',200)


def orderGenOptions(**kwargs):
    symbol = kwargs['symbol']
    contract_type = kwargs['contract_type']
    Configuration = kwargs['Configuration']
    schema_name = kwargs['schema_name']

    orderGen_dict = {}
    orderGen_dict['symbol'] = symbol
    position_group_id = kwargs['position_group_id']
    orderGen_dict['position_group_id'] = position_group_id
    orderGen_dict['isDealAllowed'] = False

    ###################################  GET EXISTING POSITIONS ########################################################
    df_positions_existing = PositionsDAO.getActivePositionsBySymbolAndContractType(schema_name, symbol, contract_type)
    orderGen_dict['df_positions_existing'] = df_positions_existing
    if len(df_positions_existing) > 0:
        print("{}: Function:orderGenOptions, {} : POSITIONS ALREADY TAKEN IN OPTIONS, EXITING NOW FROM ORDERGEN OPTIONS.".format(Configuration['SCHEMA_NAME'], symbol))
        logging.info("{}: Function:orderGenOptions, {} : POSITIONS ALREADY TAKEN IN OPTIONS, EXITING NOW FROM ORDERGEN OPTIONS.".format(Configuration['SCHEMA_NAME'], symbol))
        return orderGen_dict
    ####################################################################################################################

    ################################################ MDS CALL TO GET OPTION CHAIN #######################################
    df_Call, df_Put, spot_value, expiry_date_options, expiry_date_futures, future_value = OrderGenOptionsHelper.\
                                                                    getOptionsChain(symbol, contract_type, Configuration)
    expiry_date = expiry_date_options
    if df_Call is None or len(df_Call) == 0: return
    orderGen_dict['contract_type'] = contract_type
    orderGen_dict['expiry_date'] = expiry_date
    orderGen_dict['expiry_date_futures'] = expiry_date_futures
    orderGen_dict['spot_value'] = spot_value
    orderGen_dict['future_value'] = float(future_value)


    ############################################ ADD LEVELS TO PUT CALL AND MERGE ######################################
    df_Put_Call, SP_ATM = OrderGenOptionsHelper.addLevelPutCall(df_Call, df_Put, spot_value, Configuration)
    df_Level = df_Put_Call.copy()
    orderGen_dict['df_Level'] = df_Level # ADD LEVEL ONLY DF
    # INITIALIZE GREEKS AND IVs
    df_Put_Call = OrderGenOptionsHelper.initializeGreeksIV(df_Put_Call)
    orderGen_dict['SP_ATM'] = SP_ATM
    df_Put_Call['EXPIRY_DATE'] = expiry_date

    ################################################ MARGIN CALCULATOR API #############################################
    orderGen_dict = LotsCalculatorHelper.calculateNumLotsMarginAPI(Configuration, symbol, orderGen_dict)
    MF_STEP_SIZE = orderGen_dict['MF_STEP_SIZE']

    ########################################### TIME TO EXPIRY CALCULATIONS ###########################################
    time_to_expiry_options_252, time_to_expiry_options_annualized_252, \
    time_to_expiry_options_365, time_to_expiry_options_annualized_365 = OrderGenOptionsHelper.populateTimeToExpiry(
        expiry_date, Configuration)


    # POPULATE BOUNDS NOW
    ########################################  CALL MDS FOR MARKET DEPTH FOR OPTIONS AND FUTURES #####################
    isValidationRequired = True
    df_Put_Call = OrderGenOptionsHelper.addBidAskPricesMDS(df_Put_Call, expiry_date, symbol, Configuration,
                                                           orderGen_dict,
                                                           isValidationRequired=isValidationRequired)
    if df_Put_Call is None or len(df_Put_Call) == 0:
        print("{}: Function:orderGenOptions, EMPTY DATAFRAME BID ASK VALIDATIONS OPTIONS, EXITING NOW.".format(Configuration['SCHEMA_NAME']))
        logging.info("{}: Function:orderGenOptions, EMPTY DATAFRAME BID ASK VALIDATIONS OPTIONS, EXITING NOW.".format(Configuration['SCHEMA_NAME']))
        return orderGen_dict

    ######################################  WAP BID ASK PRICES WITH QUANTITY #########################################
    df_Put_Call = OrderGenOptionsHelper.populateWAPBidAskPrices(df_Put_Call, MF_STEP_SIZE, symbol, Configuration)

    ########################################  SET EXECUTION PRICES FROM BID ASK POST DECISION ############################
    df_Put_Call['DAYS_IN_YEAR'] = '252'
    df_Put_Call['ORDER_TYPE_CALL'] = 'NA'
    df_Put_Call['ORDER_TYPE_PUT'] = 'NA'
    df_Put_Call = OrderGenOptionsHelper.populateExecutionPricesWAP(df_Put_Call)  # FOR OPTIONS POPULATE EXECUTION PRICES

    ######################################## ORDER RULE ENGINE ON LTP ###################################################
    orderGen_dict['df_Put_Call'] = df_Put_Call
    orderGen_dict = OrderGenRuleEngine.decisonMaker(orderGen_dict, Configuration)
    df_Put_Call = orderGen_dict['df_Put_Call']

    ########################################  RETURN IN CASE OF EMPTY DATAFRAME OR IS_DEAL_ALLOWED == FALSE ############
    if df_Put_Call is None or len(df_Put_Call) == 0 or not orderGen_dict['isDealAllowed']:
        print("Function:orderGenOptions, {} : EMPTY DATAFRAME AFTER ORDER RULE ENGINE/ DEAL ALLOWED FALSE, EXITING NOW.".format(symbol))
        logging.info("Function:orderGenOptions, {} : EMPTY DATAFRAME AFTER ORDER RULE ENGINE/ DEAL ALLOWED FALSE, EXITING NOW.".format(symbol))
        return orderGen_dict

    ######################################## POPULATE GREEKS FROM API ##################################################
    df_Put_Call = OrderGenOptionsHelper.populateVIXGreeksIV(symbol, Configuration, df_Put_Call, orderGen_dict)

    ########################################  SET EXECUTION PRICES FROM BID ASK POST DECISION ############################
    df_Put_Call = OrderGenOptionsHelper.populateExecutionPricesWAP(df_Put_Call)  # FOR OPTIONS POPULATE EXECUTION PRICES

    ############################################# ADD MARGIN INFO ######################################################
    df_Put_Call = OrderGenOptionsHelper.addMarginInfo(MF_STEP_SIZE, orderGen_dict, Configuration, df_Put_Call, symbol)

    ######################################## ORDER RULE ENGINE ON IV MAX MIN THRESHOLD #################################
    orderGen_dict['df_Put_Call'] = df_Put_Call
    orderGen_dict['MARGIN_ONE_MF'] = 0.0

    orderGen_dict = OrderGenRuleEngine.IVThresholdCheckStopTrading(orderGen_dict, Configuration)
    df_Put_Call = orderGen_dict['df_Put_Call']

    ########################################  RETURN IN CASE OF EMPTY DATAFRAME OR IS_DEAL_ALLOWED == FALSE ############
    if df_Put_Call is None or len(df_Put_Call) == 0 or not orderGen_dict['isDealAllowed']:
        print("Function:orderGenOptions, TRADING NOT ALLOWED TODAY IV OR THETA THRESHOLD BREACHED, EXITING NOW.")
        logging.info("Function:orderGenOptions, TRADING NOT ALLOWED TODAY IV OR THETA THRESHOLD BREACHED, EXITING NOW.")
        return orderGen_dict

    ############################################## PLACE DEALS AND POPULATE DB ##########################################
    orderGen_dict = OrderGenOptionsHelper.placeDealsAndPopulateDBOptions(position_group_id, MF_STEP_SIZE, orderGen_dict, Configuration, df_Put_Call,
                                                         symbol, expiry_date, time_to_expiry_options_252, time_to_expiry_options_365,
                                                         spot_value, expiry_date_futures, schema_name)

    return orderGen_dict