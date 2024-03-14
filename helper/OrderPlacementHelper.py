
from adaptor.OMAdaptor import OMAdaptor
from common import utils, MarginUtil, MarginAllocationUtil
import math
import logging
import pandas as pd
from helper import OrderGenOptionsHelper
import time
import traceback
from exceptions.FreshIterationRejectionException import FreshIterationRejectionException

def orderPlacement(Configuration, df_Put_Call, expiry_date, expiry_date_futures, position_group_id, symbol, orderGen_dict):

    ##################################### ORDER PLACEMENT FOR ALL ITERATIONS ###########################################
    df_Put_Call = orderPlacementIterations(Configuration, df_Put_Call, expiry_date, expiry_date_futures, position_group_id, symbol, orderGen_dict)

    return df_Put_Call

def orderPlacementIterations(Configuration, df_Put_Call, expiry_date, expiry_date_futures, position_group_id, symbol, orderGen_dict):
    ########################################## ITERATION : ONLY FOR PROD ###############################################
    if not (Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']):
        return df_Put_Call

    #############################################   ITERATION 1 ########################################################
    df_Put_Call = orderPlacement_Iteration_1(Configuration, df_Put_Call, expiry_date, expiry_date_futures, position_group_id, symbol, orderGen_dict)

    return df_Put_Call
    # if not (Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']):
    #     return df_Put_Call
    #
    # isMarginNotUtilised = True
    # ITERATION_NUM = 2
    # orderGen_dict['isMarginAPIFailure'] = False
    #
    # ######################################### MARGIN MARGIN_UTILISED_PCT_THRESHOLD #####################################
    # MARGIN_UTILISED_PCT_THRESHOLD = MarginAllocationUtil.getMarginAllocation(Configuration, symbol, df_Put_Call, expiry_date,
    #                                                                          orderGen_dict, position_group_id)
    # logging.info("Inside OrderPlacementHelper:: MARGIN_UTILISED_PCT_THRESHOLD : {}, SYMBOL : {}".format(MARGIN_UTILISED_PCT_THRESHOLD, symbol))
    #
    # ############################################## ITERATION STARTS #########################################################
    # while isMarginNotUtilised:
    #     logging.info("################################## ITERATION "+str(ITERATION_NUM)+": START ###############################################")
    #     ITERATION_NAME = "Iteration_"+str(ITERATION_NUM)
    #     time.sleep(2)
    #     df_Put_Call.columns = [x.upper() for x in df_Put_Call.columns]
    #     df_Put_Call, isMarginUtilised = orderPlacement_Iteration_N(Configuration, df_Put_Call_Iteration, df_Put_Call, expiry_date, expiry_date_futures, position_group_id,
    #                                orderGen_dict, symbol, ITERATION_NAME, MARGIN_UTILISED_PCT_THRESHOLD, ITERATION_NUM)
    #     df_Put_Call.columns = [x.upper() for x in df_Put_Call.columns]
    #     logging.info("################################## ITERATION "+str(ITERATION_NUM)+": END ###############################################\n")
    #
    #     ######################################## EXIT IF MARGIN UTILISED FULLY #########################################
    #     if isMarginUtilised:
    #         break
    #
    #     # INCREMENT ITERATION
    #     ITERATION_NUM = ITERATION_NUM+1
    #
    #     # DROP ITERATION COLUMNS
    #     df_Put_Call.drop(['EXECUTION_PRICE_PUT_ACTUAL_ITERATION', 'EXECUTION_PRICE_CALL_ACTUAL_ITERATION','QUANTITY_PUT_ITERATION',
    #                   'QUANTITY_CALL_ITERATION', 'MULTI_FACTOR_ITERATION'], axis=1, inplace=True)
    # ############################################## ITERATION ENDS #########################################################
    #
    # return df_Put_Call


def orderPlacement_Iteration_N(Configuration, df_Put_Call_Iteration, df_Put_Call, expiry_date, expiry_date_futures, position_group_id,
                               orderGen_dict, symbol, ITERATION_NAME, MARGIN_UTILISED_PCT_THRESHOLD, ITERATION_NUM):
    isMarginUtilised = False
    ###############################################  POPULATE ITERATION MF ############################################
    MF_STEP_SIZE, MARGIN_UTILISED_PCT, live_balance = populateIterationMF(Configuration, orderGen_dict, ITERATION_NAME, symbol)

    ############################### EXCEPTION HANDLING IN CASE MARGIN CALL NOT BEING UPDATED ############################
    if MARGIN_UTILISED_PCT == 0 or orderGen_dict['isMarginAPIFailure']:
        NEXT_ITERATION_MF_PCT = float(Configuration["NEXT_ITERATION_MF_PCT"])
        MF_STEP_SIZE_ITR_1 = orderGen_dict["MF_STEP_SIZE"]
        MF_STEP_SIZE = math.floor(utils.xPercentageOfY(NEXT_ITERATION_MF_PCT, MF_STEP_SIZE_ITR_1))
        orderGen_dict["MF_STEP_SIZE"] = MF_STEP_SIZE
        orderGen_dict['isMarginAPIFailure'] = True

        ####################################### STOP AUTOMATION FOR NIFTY ##############################################
        utils.stopAutomation("NIFTY")

        logging.info("MARGIN CALL EXCEPTION SCENARIO:: ITERATION_NAME : {}, MF_STEP_SIZE : {}, "
                     "live_balance : {}, MARGIN_UTILISED_PCT : {}".format(str(ITERATION_NAME), str(MF_STEP_SIZE), str(live_balance), str(MARGIN_UTILISED_PCT)))
    #####################################################################################################################

    ############################################## ITERATION ORDER CONDITIONS ##########################################
    # 1. DO NOT PLACE ITERATION ORDER, MIN MF THRESHOLD NOT PASSED
    # 2. MARGIN UTILISED PCT THRESHOLD NOT PASSED
    if MF_STEP_SIZE < float(Configuration["MIN_MF_LEVEL_2_" + symbol]) \
            or MARGIN_UTILISED_PCT > MARGIN_UTILISED_PCT_THRESHOLD\
            or live_balance < 0:
        isMarginUtilised = True
        return df_Put_Call, isMarginUtilised

    ########################################## PLACE ORDER: ATM SELL AND STATIC HEDGING #################################
    df_Put_Call, isMarginUtilised = placeOrderIterationWrapper(Configuration, MF_STEP_SIZE, df_Put_Call_Iteration, df_Put_Call, expiry_date,
                                                 expiry_date_futures, position_group_id, symbol, isMarginUtilised, ITERATION_NUM, orderGen_dict)

    return df_Put_Call, isMarginUtilised


def placeOrderIterationWrapper(Configuration, MF_STEP_SIZE, df_Put_Call_Iteration, df_Put_Call, expiry_date,
                                                 expiry_date_futures, position_group_id, symbol, isMarginUtilised, ITERATION_NUM, orderGen_dict):
     isFreshRealignment = False
     if orderGen_dict['order_gen_type'] == "REALIGNED":
        isFreshRealignment = True

     try:
        ######################################### POPULATE ITERATION DF ####################################################
        LOT_SIZE = Configuration['LOT_SIZE_' + symbol]
        df_Put_Call_Iteration['MULTI_FACTOR'] = MF_STEP_SIZE
        df_Put_Call_Iteration['QUANTITY_PUT'] = df_Put_Call_Iteration.apply(lambda row: calculateQuantity(LOT_SIZE, MF_STEP_SIZE), axis=1).astype(float)
        df_Put_Call_Iteration['QUANTITY_CALL'] = df_Put_Call_Iteration.apply(lambda row: calculateQuantity(LOT_SIZE, MF_STEP_SIZE), axis=1).astype(float)

        ######################################### CASE 1 : PLACE SELL CALL ORDERS ###############################################
        df_Call_Sell = df_Put_Call_Iteration[(df_Put_Call_Iteration['ORDER_TYPE'] == 'SELL') & (df_Put_Call_Iteration['INSTRUMENT_TYPE'] == 'CALL')]
        df_Call_Sell = placeOrder(Configuration, df_Call_Sell, symbol, expiry_date, expiry_date_futures,position_group_id, isIteration_1 = False,
                                  isFreshRealignment = isFreshRealignment)

        # MOCK DATA
        if utils.isMockPrdEnv(Configuration) or utils.isMockDevEnv(Configuration):
            df_Call_Sell['execution_price_call_actual'] = 160.0

        ######################################### CASE 2 : PLACE STATIC HEDGED BUY CALL ORDERS ##################################
        df_Call_Static_Hedged_Buy = df_Put_Call_Iteration[(df_Put_Call_Iteration['ORDER_TYPE'] == 'BUY') & (df_Put_Call_Iteration['INSTRUMENT_TYPE'] == 'CALL')]
        Configuration['STRIKE_PRICE_CALL_STATIC_HEDGED'] = str(df_Call_Static_Hedged_Buy['STRIKE_PRICE'].iloc[0])
        df_Call_Static_Hedged_Buy = placeOrderStaticHedging(Configuration, df_Call_Static_Hedged_Buy, symbol,
                                                                expiry_date,
                                                                expiry_date_futures, position_group_id, isFreshRealignment = isFreshRealignment)

        # MOCK DATA
        if utils.isMockPrdEnv(Configuration) or utils.isMockDevEnv(Configuration):
            df_Call_Static_Hedged_Buy['execution_price_call_actual'] = 0.5

        ######################## ADD A DELAY FOR HIGHER ITERATIONS FOR MARGIN BENEFIT TO KICK IN #######################
        if ITERATION_NUM >= 5:
            time.sleep(2)

        ######################################### CASE 3 : PLACE SELL PUT ORDERS ###############################################
        df_Put_Sell = df_Put_Call_Iteration[(df_Put_Call_Iteration['ORDER_TYPE'] == 'SELL') & (df_Put_Call_Iteration['INSTRUMENT_TYPE'] == 'PUT')]
        df_Put_Sell = placeOrder(Configuration, df_Put_Sell, symbol, expiry_date, expiry_date_futures,position_group_id, isIteration_1 = False,
                                 isFreshRealignment = isFreshRealignment)

        # MOCK DATA
        if utils.isMockPrdEnv(Configuration) or utils.isMockDevEnv(Configuration):
            df_Put_Sell['execution_price_put_actual'] = 150.0

        ######################################### CASE 4 : PLACE STATIC HEDGED BUY PUT ORDERS ##################################
        df_Put_Static_Hedged_Buy = df_Put_Call_Iteration[(df_Put_Call_Iteration['ORDER_TYPE'] == 'BUY') & (df_Put_Call_Iteration['INSTRUMENT_TYPE'] == 'PUT')]
        df_Put_Static_Hedged_Buy = placeOrderStaticHedging(Configuration, df_Put_Static_Hedged_Buy, symbol,
                                                                expiry_date,
                                                                expiry_date_futures, position_group_id, isFreshRealignment = isFreshRealignment)

        # MOCK DATA
        if utils.isMockPrdEnv(Configuration) or utils.isMockDevEnv(Configuration):
            df_Put_Static_Hedged_Buy['execution_price_put_actual'] = 1.0

        ######################################## CASE 5 : APPEND DATAFRAMES POST ORDER PLACEMENT ###############################
        df_Put_Call_Iteration = df_Call_Sell.append(df_Put_Sell, ignore_index=True)
        df_Put_Call_Iteration = df_Put_Call_Iteration.append(df_Call_Static_Hedged_Buy, ignore_index=True)
        df_Put_Call_Iteration = df_Put_Call_Iteration.append(df_Put_Static_Hedged_Buy, ignore_index=True)

        ######################################### CASE 6 : ADD QTY/MF AND CALCULATE WAP ##################################
        df_Put_Call = filterAndCalculateWAPIterationPositions(df_Put_Call_Iteration, df_Put_Call)

     except FreshIterationRejectionException as ex:
        isMarginUtilised =  True
        template = "Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(message)
        print(traceback.format_exc())
        logging.info(traceback.format_exc())
        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            subject = "FAILURE | " + Configuration['SCHEMA_NAME'] + " | " + symbol + " | FRESH ORDER ITERATION_N REJECTION"
            utils.send_email_dqns(Configuration, subject, message, "HTML")
            utils.send_sns(Configuration)

     return df_Put_Call, isMarginUtilised


def populateIterationMF(Configuration, orderGen_dict, ITERATION_NAME, symbol):
    # ******************************************************************************************************************
    # THIS METHOD IS ALSO BEING USED BY LOTS CALCULATOR HELPER , SO EVERY CHANGE WILL BE AVAILABLE TO ITERATION1 AS WELL
    # ******************************************************************************************************************

    # FETCH MARGIN CALL
    ################################################ GET AVAILABLE MARGIN CALL #########################################
    margin_profile_dict = MarginUtil.getMarginProfileDetails(Configuration)

    # MOCK DATA
    if utils.isMockPrdEnv(Configuration) or utils.isMockDevEnv(Configuration):
        mockMarginData(margin_profile_dict, ITERATION_NAME, symbol)

    ############################################### POPULATE MF ITERATION ##############################################
    live_balance = margin_profile_dict['live_balance']
    opening_balance = margin_profile_dict['opening_balance']
    utilised = margin_profile_dict['utilised']
    TOTAL_INITIAL_MARGIN = float(Configuration["TOTAL_INITIAL_MARGIN"])
    ################### IN CASE OF LOSSES, IF OPENING BALANCE IS LESS THAN TOTAL INITIAL MARGIN #######################
    TOTAL_INITIAL_MARGIN = min(TOTAL_INITIAL_MARGIN, (utilised+live_balance))

    MF_THRESHOLD_MINUS_PCT = float(Configuration["MF_THRESHOLD_MINUS_PCT_"+symbol])
    QTY_FREEZE_LIMIT_MF = float(Configuration["QTY_FREEZE_LIMIT_MF_" + symbol])
    MF_PER_LOT = orderGen_dict["MF_PER_LOT"]

    # GET OPENING BALANCE - AFTER HAIRCUT of 15 Pct or 100000 whichever is higher
    MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD = getMarginHaircutPctThreshold(Configuration, orderGen_dict['expiry_date'])
    MARGIN_OPENING_BALANCE_HAIRCUT_AMOUNT_THRESHOLD = float(Configuration['MARGIN_OPENING_BALANCE_HAIRCUT_AMOUNT_THRESHOLD'])
    OPENING_BALANCE = utilised+live_balance
    OPENING_BALANCE_POST_HAIRCUT_PCT = OPENING_BALANCE - utils.xPercentageOfY(MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD, OPENING_BALANCE)
    OPENING_BALANCE_POST_HAIRCUT_AMOUNT = OPENING_BALANCE - MARGIN_OPENING_BALANCE_HAIRCUT_AMOUNT_THRESHOLD
    OPENING_BALANCE = min(OPENING_BALANCE_POST_HAIRCUT_PCT, OPENING_BALANCE_POST_HAIRCUT_AMOUNT)

    # MF CALCULATION
    AVAILABLE_MARGIN_FOR_TRADING_POST_HAIRCUT = (min((OPENING_BALANCE), TOTAL_INITIAL_MARGIN) - utilised)
    MF_STEP_SIZE = AVAILABLE_MARGIN_FOR_TRADING_POST_HAIRCUT / MF_PER_LOT
    MF_THRESHOLD_MINUS_PCT_VALUE = utils.xPercentageOfY(MF_THRESHOLD_MINUS_PCT, MF_STEP_SIZE)
    MARGIN_UTILISED_PCT = round(utils.percentageIsXofY(utilised, TOTAL_INITIAL_MARGIN),2)
    MF_STEP_SIZE = min(math.floor(MF_STEP_SIZE - MF_THRESHOLD_MINUS_PCT_VALUE), QTY_FREEZE_LIMIT_MF) # CANNOT EXCEED MORE THAN QTY FREEZE LIMIT
    logging.info("Inside OrderPlacementHelper :: "+ITERATION_NAME+" :: SYMBOL : {}, MF_PER_LOT: {}, TOTAL_INITIAL_MARGIN: {}, "
                 "opening_balance: {}, opening_balance_post_haircut: {}, live_balance: {}, utilised : {}, AVAILABLE_MARGIN_FOR_TRADING_POST_HAIRCUT: {},"
                 " MARGIN_UTILISED_PCT : {}, MF_STEP_SIZE: {}".format(orderGen_dict['symbol'], str(MF_PER_LOT), str(TOTAL_INITIAL_MARGIN),
                 str(opening_balance), str(OPENING_BALANCE), str(live_balance), str(utilised), str(AVAILABLE_MARGIN_FOR_TRADING_POST_HAIRCUT),
                                                                      str(MARGIN_UTILISED_PCT), str(MF_STEP_SIZE)))
    return MF_STEP_SIZE, MARGIN_UTILISED_PCT, live_balance

def getMarginHaircutPctThreshold(Configuration, expiry_date):

    # HAIRCUT MAX ON FRIDAY MONDAY For e.g. 17%
    HAIRCUT_MAX_ON_DAYS_LIST = Configuration['HAIRCUT_MAX_ON_DAYS'].split(',')
    if utils.checkIfTodayisParticularDayFromList(HAIRCUT_MAX_ON_DAYS_LIST):
        return float(Configuration['MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD_3'])

    # MOCK DATA
    #expiry_date = '21SEP13'

    # HAIRCUT MODERATE ON TUESDAY/WEDNESDAY e.g. 15%
    # HAIRCUT MIN ON EXPIRY e.g. 10%
    if not utils.isExpiryToday(expiry_date):
        MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD = float(Configuration['MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD_1'])
    else:
        MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD = float(Configuration['MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD_2'])

    return MARGIN_OPENING_BALANCE_HAIRCUT_PCT_THRESHOLD


def filterAndCalculateWAPIterationPositions(df_Put_Call_Iteration, df_Put_Call):
    df_Put_Call.columns = [x.upper() for x in df_Put_Call.columns]
    df_Put_Call_Iteration.columns = [x.upper() for x in df_Put_Call_Iteration.columns]

    # ADD EXECUTION PRICES
    df_Put_Call_Iteration = df_Put_Call_Iteration[['EXECUTION_PRICE_PUT_ACTUAL', 'EXECUTION_PRICE_CALL_ACTUAL', 'QUANTITY_PUT', 'QUANTITY_CALL','MULTI_FACTOR', "STRIKE_PRICE",
                                                         "INSTRUMENT_TYPE"]]
    # RENAME COLUMNS
    df_Put_Call_Iteration['STRIKE_PRICE'] = df_Put_Call_Iteration['STRIKE_PRICE'].astype(float)
    df_Put_Call_Iteration['QUANTITY_PUT'] = df_Put_Call_Iteration['QUANTITY_PUT'].astype(float)
    df_Put_Call_Iteration['QUANTITY_CALL'] = df_Put_Call_Iteration['QUANTITY_CALL'].astype(float)
    df_Put_Call_Iteration['MULTI_FACTOR'] = df_Put_Call_Iteration['MULTI_FACTOR'].astype(float)
    df_Put_Call_Iteration.rename(columns={'EXECUTION_PRICE_PUT_ACTUAL': 'EXECUTION_PRICE_PUT_ACTUAL_ITERATION',
                                            'EXECUTION_PRICE_CALL_ACTUAL': 'EXECUTION_PRICE_CALL_ACTUAL_ITERATION',
                                            'QUANTITY_PUT': 'QUANTITY_PUT_ITERATION',
                                            'QUANTITY_CALL': 'QUANTITY_CALL_ITERATION',
                                            'MULTI_FACTOR': 'MULTI_FACTOR_ITERATION'
                                            }, inplace=True)

    # MERGE ITERATION DF TO EXISTING DF
    df_Put_Call = pd.merge(df_Put_Call_Iteration,df_Put_Call, how='inner',
                                                     on=['INSTRUMENT_TYPE', 'STRIKE_PRICE'])

    # Update df_Put_Call with WAP/ MULTI FACTOR/ MARGIN TXN
    df_Put_Call['MULTI_FACTOR'] = pd.to_numeric(
        df_Put_Call.apply(lambda row: float(row['MULTI_FACTOR']) + float(row['MULTI_FACTOR_ITERATION']), axis=1), downcast='signed')
    df_Put_Call['EXECUTION_PRICE_PUT_ACTUAL'] = pd.to_numeric(df_Put_Call.apply(lambda row:
                                                                          OrderGenOptionsHelper.calculateWAPExecutionPrice(
                                                                              float(row['EXECUTION_PRICE_PUT_ACTUAL']),
                                                                              float(row['EXECUTION_PRICE_PUT_ACTUAL_ITERATION']),
                                                                              float(row['QUANTITY_PUT']),
                                                                              float(row['QUANTITY_PUT_ITERATION'])), axis=1), downcast='float')
    df_Put_Call['EXECUTION_PRICE_CALL_ACTUAL'] = pd.to_numeric(df_Put_Call.apply(lambda row:
                                                                          OrderGenOptionsHelper.calculateWAPExecutionPrice(
                                                                              float(row['EXECUTION_PRICE_CALL_ACTUAL']),
                                                                              float(row['EXECUTION_PRICE_CALL_ACTUAL_ITERATION']),
                                                                              float(row['QUANTITY_CALL']),
                                                                              float(row['QUANTITY_CALL_ITERATION'])), axis=1), downcast='float')
    # ADD QUANTITY POST WAP CALCULATION
    df_Put_Call['QUANTITY_PUT'] = pd.to_numeric(df_Put_Call.apply(lambda row: float(row['QUANTITY_PUT']) + float(row['QUANTITY_PUT_ITERATION']), axis=1),
                                            downcast='signed')
    df_Put_Call['QUANTITY_CALL'] = pd.to_numeric(df_Put_Call.apply(lambda row: float(row['QUANTITY_CALL']) + float(row['QUANTITY_CALL_ITERATION']), axis=1),
                                            downcast='signed')
    df_Put_Call.columns = [x.lower() for x in df_Put_Call.columns]
    return df_Put_Call

def orderPlacement_Iteration_1(Configuration, df_Put_Call, expiry_date, expiry_date_futures, position_group_id, symbol, orderGen_dict):

    ######################################### CASE 1 : PLACE STATIC HEDGED BUY CALL AND PUT ORDERS #####################
    df_Static_Hedged_Buy = df_Put_Call[df_Put_Call['ORDER_MANIFEST'].str.contains("BUY")]
    if df_Static_Hedged_Buy is not None and len(df_Static_Hedged_Buy)>0:
        df_Static_Hedged_Buy = placeOrderStaticHedging(Configuration, df_Static_Hedged_Buy, symbol,
                                                                expiry_date,
                                                                expiry_date_futures, position_group_id)


    ######################################### CASE 2 : PLACE SELL PUT AND CALL ORDERS ##################################
    # DIVIDE CALL AND PUT SELL, FOR MARGIN Benefit
    df_Call_Sell = df_Put_Call[df_Put_Call['ORDER_MANIFEST'].str.contains("CALL_SELL")]
    df_Call_Sell = placeOrder(Configuration, df_Call_Sell, symbol, expiry_date, expiry_date_futures,
                                  position_group_id, isIteration_1=True)

    time.sleep(3)
    df_Put_Sell = df_Put_Call[df_Put_Call['ORDER_MANIFEST'].str.contains("PUT_SELL")]
    df_Put_Sell = placeOrder(Configuration, df_Put_Sell, symbol, expiry_date, expiry_date_futures,
                                  position_group_id, isIteration_1=True)
    df_Put_Call = df_Call_Sell.append(df_Put_Sell, ignore_index=True)

    ######################################## CASE 3 : APPEND DATAFRAMES POST ORDER PLACEMENT ###############################
    if df_Static_Hedged_Buy is not None and len(df_Static_Hedged_Buy) > 0:
        df_Put_Call = df_Put_Call.append(df_Static_Hedged_Buy, ignore_index=True)
    return df_Put_Call


def placeOrder(Configuration, df_Put_Call, symbol, expiry_date, expiry_date_futures, position_group_id, isIteration_1):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']:
        omAdaptor = OMAdaptor()
        orderType = "FRESH"
        tradeType = Configuration['TRADE_TYPE']
        df_Put_Call = omAdaptor.placeOrders(df_Put_Call, Configuration, symbol, orderType, tradeType, expiry_date,
                            position_group_id, expiry_date_futures, isFresh = True, isHedging = False, isSquareOff = False, isIteration_1 = isIteration_1)
        df_Put_Call.columns = [x.lower() for x in df_Put_Call.columns]
    else:  # Default Broker related fields
        df_Put_Call['broker_order_id_fresh_put'] = "NA"
        df_Put_Call['broker_order_id_fresh_call'] = "NA"
        df_Put_Call['broker_order_id_fresh_futures'] = "NA"
        df_Put_Call['broker_order_status_fresh_put'] = "NA"
        df_Put_Call['broker_order_status_fresh_call'] = "NA"
        df_Put_Call['broker_order_status_fresh_futures'] = "NA"
        df_Put_Call['broker_order_id_squareoff_put'] = "NA"
        df_Put_Call['broker_order_id_squareoff_call'] = "NA"
        df_Put_Call['broker_order_id_squareoff_futures'] = "NA"
        df_Put_Call['broker_order_status_squareoff_put'] = "NA"
        df_Put_Call['broker_order_status_squareoff_call'] = "NA"
        df_Put_Call['broker_order_status_squareoff_futures'] = "NA"

    return df_Put_Call

def placeOrderStaticHedging(Configuration, df_Put_Call, symbol, expiry_date, expiry_date_futures, position_group_id):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']:
        omAdaptor = OMAdaptor()
        orderType = "STATIC_HEDGING"
        tradeType = Configuration['TRADE_TYPE']
        df_Put_Call = omAdaptor.placeOrders(df_Put_Call, Configuration, symbol, orderType, tradeType, expiry_date,
                            position_group_id, expiry_date_futures, isFresh = False, isHedging = True, isSquareOff = False)
        df_Put_Call.columns = [x.lower() for x in df_Put_Call.columns]

    else:  # Default Broker related fields
        df_Put_Call['broker_order_id_fresh_put'] = "NA"
        df_Put_Call['broker_order_id_fresh_call'] = "NA"
        df_Put_Call['broker_order_id_fresh_futures'] = "NA"
        df_Put_Call['broker_order_status_fresh_put'] = "NA"
        df_Put_Call['broker_order_status_fresh_call'] = "NA"
        df_Put_Call['broker_order_status_fresh_futures'] = "NA"
        df_Put_Call['broker_order_id_squareoff_put'] = "NA"
        df_Put_Call['broker_order_id_squareoff_call'] = "NA"
        df_Put_Call['broker_order_id_squareoff_futures'] = "NA"
        df_Put_Call['broker_order_status_squareoff_put'] = "NA"
        df_Put_Call['broker_order_status_squareoff_call'] = "NA"
        df_Put_Call['broker_order_status_squareoff_futures'] = "NA"

    return df_Put_Call

def calculateQuantity(LOT_SIZE, MF_IA):
    return float(LOT_SIZE) * float(MF_IA)

def mockMarginData1(margin_profile_dict, ITERATION_NAME, symbol):
    if ITERATION_NAME == "Iteration_2" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 1500000
        margin_profile_dict['opening_balance'] = 1500000
        margin_profile_dict['utilised'] = 0
    elif ITERATION_NAME == "Iteration_3" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 1500000
        margin_profile_dict['opening_balance'] = 1500000
        margin_profile_dict['utilised'] = 0
    elif ITERATION_NAME == "Iteration_4" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 1500000
        margin_profile_dict['opening_balance'] = 1500000
        margin_profile_dict['utilised'] = 0
    elif ITERATION_NAME == "Iteration_5" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 1500000
        margin_profile_dict['opening_balance'] = 1500000
        margin_profile_dict['utilised'] = 0
    elif ITERATION_NAME == "Iteration_6" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 1500000
        margin_profile_dict['opening_balance'] = 1500000
        margin_profile_dict['utilised'] = 0
    elif ITERATION_NAME == "Iteration_7" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 1500000
        margin_profile_dict['opening_balance'] = 1500000
        margin_profile_dict['utilised'] = 0

# Qty Freeze
def mockMarginData2(margin_profile_dict, ITERATION_NAME, symbol):
    if ITERATION_NAME == "Iteration_2" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 9200000
        margin_profile_dict['opening_balance'] = 15200000
        margin_profile_dict['utilised'] = 6000000
    elif ITERATION_NAME == "Iteration_3" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 6200000
        margin_profile_dict['opening_balance'] = 15200000
        margin_profile_dict['utilised'] = 9200000
    elif ITERATION_NAME == "Iteration_4" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 3000000
        margin_profile_dict['opening_balance'] = 15200000
        margin_profile_dict['utilised'] = 12000000
    elif ITERATION_NAME == "Iteration_5" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 2400000
        margin_profile_dict['opening_balance'] = 5200000
        margin_profile_dict['utilised'] = 13000000
    elif ITERATION_NAME == "Iteration_6" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 300000
        margin_profile_dict['opening_balance'] = 15200000
        margin_profile_dict['utilised'] = 14900000
    elif ITERATION_NAME == "Iteration_7" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 100000
        margin_profile_dict['opening_balance'] = 15200000
        margin_profile_dict['utilised'] = 15100000

def mockMarginData(margin_profile_dict, ITERATION_NAME, symbol):
    if ITERATION_NAME == "Iteration_2" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 3200000
        margin_profile_dict['opening_balance'] = 5200000
        margin_profile_dict['utilised'] = 2000000
    elif ITERATION_NAME == "Iteration_3" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 2200000
        margin_profile_dict['opening_balance'] = 5200000
        margin_profile_dict['utilised'] = 3200000
    elif ITERATION_NAME == "Iteration_4" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 1000000
        margin_profile_dict['opening_balance'] = 5200000
        margin_profile_dict['utilised'] = 4000000
    elif ITERATION_NAME == "Iteration_5" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 800000
        margin_profile_dict['opening_balance'] = 5200000
        margin_profile_dict['utilised'] = 4400000
    elif ITERATION_NAME == "Iteration_6" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 300000
        margin_profile_dict['opening_balance'] = 5200000
        margin_profile_dict['utilised'] = 4900000
    elif ITERATION_NAME == "Iteration_7" and symbol == "BANKNIFTY":
        margin_profile_dict['live_balance'] = 100000
        margin_profile_dict['opening_balance'] = 5200000
        margin_profile_dict['utilised'] = 5100000
