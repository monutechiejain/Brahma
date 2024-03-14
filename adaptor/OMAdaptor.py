import pandas as pd
from common import utils, PositionsUtil, CleanUpUtil, ReconciliationUtil
import logging
import traceback
import time
from adaptor import OMRequestProcessor, OMResponseProcessor
from dao import TrackerDAO, PositionsDAO, ClientConfigurationDAO
import json
import numpy as np
from exceptions.FreshIterationRejectionException import FreshIterationRejectionException
from datetime import datetime
import pytz


class OMAdaptor:

    def __init__(self):
        pass

    def placeOrders(self, df_Put_Call, Configuration, symbol, orderType, tradeType, expiry_date_options, position_group_id,
                    expiry_date_futures, isFresh = False, isHedging = False, isSquareOff = False, isMarginFreeUp = False,
                    isQtyFreeze = False, isIteration_1 = False):
        '''
        Place Orders Using BROKER Order Manager API
        :param df_Put_Call:
        :return:
        '''

        df_Put_Call_Order = df_Put_Call.copy()
        OM_URL = Configuration['OM_URL']

        ############################## CREATE BROKER ORDER REQUEST ####################################################
        df_Order, order_dict = OMRequestProcessor.populateOMRequest(Configuration, df_Put_Call_Order,
                                                                    expiry_date_futures,
                                                                    expiry_date_options, isFresh, isHedging,
                                                                    isSquareOff,
                                                                    position_group_id, orderType, symbol, tradeType, isMarginFreeUp, isQtyFreeze)

        ########################## RECONCILE QUANTITY BEFORE PLACING ORDER ############################################
        # if isSquareOff and not isQtyFreeze and not utils.isMockPrdEnv(Configuration) and not utils.isMockDevEnv(Configuration):  # CHECK ONLY IN CASE OF FULL SQUAREOFF
        #     checkReconciliation(df_Order, OM_URL, orderType, Configuration, symbol, position_group_id, isSquareOffHedging)

        ################################ CALL BROKER ORDER MANAGER API- PLACE AND PROCESS ORDERS ##################
        df_Order = self.processOrder(OM_URL, order_dict, df_Order, Configuration, symbol, orderType,
                                     position_group_id, isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze, isIteration_1,
                                     expiry_date_options, expiry_date_futures)

        ############################## CREATE BROKER ORDER REQUEST ####################################################
        df_Put_Call_Order = OMResponseProcessor.transformOMResponse(Configuration, df_Put_Call_Order,
                                                                   df_Order, isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze)

        return df_Put_Call_Order



    def processOrder(self, OM_URL, order_dict, df_Order, Configuration, symbol, orderType,
                                     position_group_id, isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze, isIteration_1,
                                     expiry_date_options, expiry_date_futures):

        try:
            ts_start = time.time()

            isStaticHedging = False
            if orderType == "STATIC_HEDGING":
                isStaticHedging = True

            ###################################### PLACE FRESH ORDER ###################################################
            orderResponseDict, statusCode = utils.getOMServiceCallHelper(Configuration).placeOrder(Configuration, OM_URL, order_dict, orderType, symbol,
                                                                           isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze, isIteration_1)
            ############################################################################################################

            df_orderDigestResponse = pd.DataFrame()
            # STATUS CODE CHECK
            #if True:
            if statusCode == 503:
                ####################################### NO FILLED SCENARIO #############################################
                message = json.dumps(orderResponseDict, indent = 4)
                subject = "NO_ORDER_FILLED | " + Configuration['SCHEMA_NAME'] + " | " + symbol + " | " + orderType
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)

                if isStaticHedging or isFresh or isSquareOff:
                    time.sleep(3)
                    squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)

                raise ValueError(Configuration['SCHEMA_NAME']+": NO_ORDER_FILLED_SCENARIO.")

            elif statusCode == 551:
                ####################################### PARTIAL FILLED SCENARIO ########################################
                df_orderDigestResponse = self.handlePartialFilledScenario(Configuration, df_orderDigestResponse,
                                                                          isFresh, isHedging, isSquareOff,
                                                                          orderResponseDict, orderType, symbol, isMarginFreeUp, OM_URL, position_group_id)

            elif statusCode == 201:
                ####################################### HAPPY PATH SCENARIO ############################################
                df_orderDigestResponse = self.processOrderDigestResponse(orderResponseDict)

            ############################ STATUS CHECK :: IF ANY STATUS IS NOT COMPLETE, SEND MAIL ######################
            df_Order_Copy = df_Order.copy()
            df_orderDigestResponse = self.statusCheckResponse(Configuration, df_orderDigestResponse, isSquareOff, orderResponseDict, orderType,
                                     symbol, OM_URL, position_group_id, isFresh, isHedging, isMarginFreeUp, isIteration_1, df_Order_Copy,
                                                              expiry_date_options, expiry_date_futures)
            ############################################################################################################


            logging.info('{}: df_orderDigestResponse Values - \n{}'.format(Configuration['SCHEMA_NAME'], df_orderDigestResponse))
            ts_end = time.time()
            time_taken = ts_end - ts_start
            logging.info('{}: TIME TAKEN TO PLACE {} ORDER IN BROKER FOR {} IS : {}'.format(Configuration['SCHEMA_NAME'],
                orderType, str(symbol),str(time_taken)))
            logging.info('\n********************************************* BROKER PLACE ORDER : END  ************************************************\n')

            ############################### POST ORDER PLACEMENT - MERGE INPUT AND OUTPUT DF ###########################
            if not isQtyFreeze:
                df_orderDigestResponse['TRADING_SYMBOL'] = df_orderDigestResponse['tradingsymbol']
                df_Order = pd.merge(df_Order, df_orderDigestResponse, on='TRADING_SYMBOL', how='inner')
            else:
                df_orderDigestResponse['TAG'] = df_orderDigestResponse['tag']
                df_Order = pd.merge(df_Order, df_orderDigestResponse, on='TAG', how='inner')

            ################################ RECONCILIATION OF AVERAGE PRICE ###########################################
            # 1. Active Positions Call
            # 2. Merge and Get Buy or Sell Average Price for symbol, will not work for squaredoff symbols
            # 3. Get Average Price for particular symbol and Replace in df_orderDigestResponse
            # REMOVED FOR NOW, WAS CAUSING PROBLEMS IN CASE OF FRESH PARTIAL ORDER SCENARIO
            # df_Order = ReconciliationUtil.reconcileAveragePrice(df_Order, Configuration, OM_URL, orderType, symbol)
            # logging.info("{}: Average Price Reconciliation Done!!!".format(Configuration['SCHEMA_NAME']))
            logging.info('{} : df_Order Values - \n{}'.format(Configuration['SCHEMA_NAME'],df_Order))


            # SEND MAIL AFTER BROKER ORDER PLACEMENT
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y' and Configuration['ENVIRONMENT']=='PRD':
                send_mail(Configuration, df_Order, symbol, orderType)

        except FreshIterationRejectionException as ex:
            template = Configuration['SCHEMA_NAME']+": Exception {} occurred with message : {}"
            message = template.format(type(ex).__name__, ex.args)
            raise FreshIterationRejectionException(message)
        except Exception as ex:
            ################################# UNKNOWN EXCEPTION HANDLING e.g. 504 TIMEOUT ERROR ########################
            isStaticHedging = False
            if orderType == "STATIC_HEDGING":
                isStaticHedging = True
            if isSquareOff or isFresh or isStaticHedging:
                # SQUAREOFF ACTIVE POSITIONS AND STOP AUTOMATION
                squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)

                # STOP AUTOMATION
                stopAutomation(Configuration['SCHEMA_NAME'], symbol)
            ############################################################################################################
            template = Configuration['SCHEMA_NAME']+": Exception {} occurred with message : {}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            logging.info(message)
            print(traceback.format_exc())
            logging.info(traceback.format_exc())
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                subject = "FAILURE | "+Configuration['SCHEMA_NAME']+ " | " + symbol + " | " + orderType
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)
            raise ValueError(message)

        return df_Order

    ####################################################################################################################
    def statusCheckResponse(self, Configuration, df_orderDigestResponse, isSquareOff, orderResponseDict, orderType,
                            symbol,OM_URL, position_group_id, isFresh, isHedging, isMarginFreeUp, isIteration_1, df_Order_Copy,
                            expiry_date_options, expiry_date_futures):
        #INSTRUMENT_TYPE = df_Order_Copy['INSTRUMENT_TYPE'].iloc[0]

        # MOCK DATA
        #if isFresh and not isIteration_1 and INSTRUMENT_TYPE == 'PUT':
        #if isSquareOff:
        # if orderType == "STATIC_HEDGING":
        #   df_orderDigestResponse["status"] = "REJECTED"

        df_Status_Check = df_orderDigestResponse[~df_orderDigestResponse["status"].str.contains("COMPLETE")]
        isStaticHedging = False
        if orderType == "STATIC_HEDGING":
            isStaticHedging = True

        if df_Status_Check is not None and len(df_Status_Check) > 0:
            ##################################### STEP 1: ONE LEG REVERT SCENARIO DUTING FRESH ORDER ###################
            # if isFresh and not isIteration_1:
            #     # WE ARE PLACING CALL SELL/CALL BUY/PUT SELL/ PUT BUY SEQUENCE IN FRESH ORDERS
            #     # IF CALL IS REJECTED IN ITERATION_N ORDER, THROW EXCEPTION, NO NEED TO SQUAREOFF ANYTHING
            #     # IF PUT IS REJECTED IN ITERATION_N ORDER, THROW EXCEPTION, SQUAREOFF CALL WHICH ARE ADDED IN THE SAME LEG
            #     df_orderDigestResponse = df_orderDigestResponse[df_orderDigestResponse["status"].str.contains("COMPLETE")]
            #
            #     # HAPPY PATH SCENARIOS
            #     # FIRE ORDERS IN SEQUENCE FOR ALL ITERATIONS - CALL SELL/ CALL BUY/ PUT SELL/ PUT BUY
            #
            #     # EXCEPTION SCENARIOS:
            #     # 1. CALL/PUT IN ITERATION_1 OR STATIC HEDGING IN ANY ITERATION FAILURES - SQUAREOFF ALL
            #     # 2. CALL FAILURE IN ITERATION_N - EXIT FROM THE ITERATION, THROW EXCEPTION, POPULATE IN DB ALL THE PREVIOUS ITERATIONS
            #     # 3. PUT FAILURE IN ITERTAION_N - SQUAREOFF CALL FROM EXISTING ITERATION, EXIT FROM THE ITERATION, THROW EXCEPTION,
            #     #                                POPULATE IN DB ALL THE PREVIOUS ITERATIONS
            #     if (df_orderDigestResponse is None or len(df_orderDigestResponse) == 0) and INSTRUMENT_TYPE == 'CALL':
            #         raise FreshIterationRejectionException("FRESH_ITERATION_N_CALL_ORDER_REJECTED", "FRESH_ITERATION_N_CALL_ORDER_REJECTED")
            #     else:
            #         df_Order_Copy = df_Order_Copy[["QUANTITY", "TRADING_SYMBOL"]]
            #         df_Order_Copy["tradingsymbol"] = df_Order_Copy["TRADING_SYMBOL"]
            #         df_Order_Copy.columns = [x.lower() for x in df_Order_Copy.columns]
            #         df_Order_Copy["order_type"] = "BUY"
            #
            #         # AS PUT GOT REJECTED AS WE HAVE ALREADY ADDED CALL IN SAME ITERATION, SO SQUAREOFF CALL
            #         df_Order_Copy['tradingsymbol'] = df_Order_Copy['tradingsymbol'].str.replace('PE', 'CE')
            #         # SQUAREOFF SINGLE EXTRA LEG i.e. CALL SELL AND CALL BUY, APPEND CALL BUY SQUAREOFF STATIC HEDGED ORDER AS WELL
            #         futures_symbol, put_symbol, call_symbol = OMRequestProcessor.populateTradingSymbol(symbol,expiry_date_options,
            #                                                                                            expiry_date_futures,
            #                                                                                            Configuration['STRIKE_PRICE_CALL_STATIC_HEDGED'])
            #         quantity = df_Order_Copy['quantity'].iloc[0]
            #         tradingsymbol = call_symbol
            #         order_type = "SELL"
            #         order_call_buy_dict = {'quantity': quantity, 'tradingsymbol': tradingsymbol, 'order_type': order_type}
            #         df_Order_Call_Buy = pd.DataFrame(order_call_buy_dict, index=[0])
            #         df_Order_Copy = df_Order_Copy.append(df_Order_Call_Buy, ignore_index=True)
            #         df_Order_Copy.drop('trading_symbol', axis=1, inplace=True)
            #         squareOffPositionsByDataframe(df_Order_Copy, Configuration, OM_URL, symbol, position_group_id)
            #         raise FreshIterationRejectionException("FRESH_ITERATION_N_PUT_ORDER_REJECTED","FRESH_ITERATION_N_PUT_ORDER_REJECTED")

            ############## STEP 2: STOP AUTOMATION IN CASE OF SQUAREFF/FRESH_ITERATION_1/STATIC_HEDGING #################
            if isSquareOff or isFresh or isStaticHedging:
                # SQUAREOFF ACTIVE POSITIONS AND STOP AUTOMATION
                squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)

                # STOP AUTOMATION
                stopAutomation(Configuration['SCHEMA_NAME'], symbol)
                raise ValueError(Configuration['SCHEMA_NAME']+": ACTIVE POSITIONS SQUAREDOFFF, NEED MANUAL INTERVENTION.")

            ######################### STEP 3: HANDLING IN CASE OF DYNAMIC HEDGING OR MARGIN FREE UP ####################
            # REMOVE ALL CANCELLED OR REJECTED ORDERS
            if isHedging or isMarginFreeUp:
                df_orderDigestResponse = df_orderDigestResponse[df_orderDigestResponse["status"].str.contains("COMPLETE")]
                if df_orderDigestResponse is None or len(df_orderDigestResponse) == 0:
                    raise ValueError(Configuration['SCHEMA_NAME']+": ORDERS NOT COMPLETED WHILE HEDGING OR MARGIN FREE UP. CHECK POSITIONS MISMATCH. EXITING.")

            ########################### STEP 4 - SEND MAIL WITH MANUAL INTERVENTION REQUIRED ###########################
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                subject = "FAILURE : STATUS_NOT_COMPLETED | CHECK POSITIONS MISMATCH | " + Configuration[
                    'SCHEMA_NAME'] + " | " + symbol + " | " + orderType
                message = json.dumps(orderResponseDict, indent=4)
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)

        ################################################################################################################

        return df_orderDigestResponse

    def handlePartialFilledScenario(self, Configuration, df_orderDigestResponse, isFresh, isHedging, isSquareOff,
                                    orderResponseDict, orderType, symbol, isMarginFreeUp, OM_URL, position_group_id):
        isStaticHedging = False
        if orderType == "STATIC_HEDGING":
            isStaticHedging = True
        if isFresh or isStaticHedging:
            # STEP 1: SQUAREOFF ACTIVE POSITIONS AND STOP AUTOMATION
            time.sleep(3)
            squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)
            stopAutomation(Configuration['SCHEMA_NAME'], symbol)

            # STEP 2 - SEND MAIL WITH MANUAL INTERVENTION REQUIRED
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                subject = "PARTIAL_ORDER_FILLED | NEED_MANUAL_INTERVENTION | " + Configuration['SCHEMA_NAME'] + " | " + \
                          symbol + " | " + orderType
                message = json.dumps(orderResponseDict, indent=4)
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)

            # STEP 3 - RAISE EXCEPTION
            raise ValueError(Configuration['SCHEMA_NAME']+": PARTIAL_ORDER_FILLED_SCENARIO.")
        elif isHedging:
            # STEP 1 - SEND MAIL WITH MANUAL INTERVENTION REQUIRED
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                subject = "PARTIAL_ORDER_FILLED | NEED_MANUAL_INTERVENTION | " + Configuration[
                    'SCHEMA_NAME'] + " | " + \
                          symbol + " | " + orderType
                message = json.dumps(orderResponseDict, indent=4)
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)

            # STEP 2 - PROCESS PARTIAL FILLED RESPONSE
            df_orderDigestResponse = self.processOrderDigestResponse(orderResponseDict)

            # STEP 3: REMOVE NaN Values
            df_orderDigestResponse = df_orderDigestResponse.dropna(subset=['zorderid'])
        elif isSquareOff:
            # STEP 1: SQUAREOFF ACTIVE POSITIONS AND STOP AUTOMATION
            time.sleep(3)
            squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)

            # STOP AUTOMATION
            stopAutomation(Configuration['SCHEMA_NAME'], symbol)

            # STEP 2 - SEND MAIL WITH MANUAL INTERVENTION REQUIRED
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                subject = "PARTIAL_ORDER_FILLED | NEED_MANUAL_INTERVENTION | " + Configuration[
                    'SCHEMA_NAME'] + " | " + \
                          symbol + " | " + orderType
                message = json.dumps(orderResponseDict, indent=4)
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)

            # STEP 3 - RAISE EXCEPTION
            raise ValueError(Configuration['SCHEMA_NAME']+": PARTIAL_ORDER_FILLED_SCENARIO.")
        elif isMarginFreeUp:
            # STEP 1 - SEND MAIL WITH MANUAL INTERVENTION REQUIRED
            subject = "PARTIAL_ORDER_FILLED | NEED_MANUAL_INTERVENTION | " + Configuration[
                'SCHEMA_NAME'] + " | " + \
                      symbol + " | " + orderType
            message = json.dumps(orderResponseDict, indent=4)
            utils.send_email_dqns(Configuration, subject, message, "HTML")
            utils.send_sns(Configuration)
            raise ValueError("PARTIAL_ORDER_FILLED_SCENARIO.")

        return df_orderDigestResponse

    def processOrderDigestResponse(self, orderResponseDict):
        df_orderDigestResponse = pd.read_json(orderResponseDict["order_digest"])
        df_orderDigestResponse = df_orderDigestResponse[
            ['zorderid', 'tradingsymbol', 'status', 'status_message', 'average_price', 'tag']]
        df_orderDigestResponse['status_message'].fillna("NA", inplace=True)  # FILL NAN VALUES WITH NA
        df_orderDigestResponse['status'] = df_orderDigestResponse['status'] + " | "+df_orderDigestResponse['tag']
        return df_orderDigestResponse

def checkReconciliation(df_Order, OM_URL, orderType, Configuration, symbol, position_group_id, isSquareOffHedging):
    tradeType = Configuration['TRADE_TYPE']

    try:
        df_SquareOff_Order = df_Order.copy()
        df_SquareOff_Order['quantity_squareoff'] = df_SquareOff_Order['QUANTITY']
        df_SquareOff_Order['tradingsymbol'] = df_SquareOff_Order['TRADING_SYMBOL']
        df_SquareOff_Order = df_SquareOff_Order[['quantity_squareoff', 'tradingsymbol']]

        ################################ FETCH ACTIVE POSITIONS ############################################################
        activePositionsResponse = utils.getOMServiceCallHelper(Configuration).fetchActivePositions(Configuration, OM_URL, orderType)
        ####################################################################################################################

        ################################ IF NO ACTIVE POSITIONS, RECILIATION FAILED, STOP AUTOMATION ###################
        if len(activePositionsResponse) == 0:
            stopAutomationAndRaiseExceptionNoPositions(Configuration, orderType, symbol)
        ################################################################################################################


        df_ActivePositions = pd.DataFrame(activePositionsResponse)

        # GET ONLY SELECTED TRADE TYPE e.g. MIS ONLY
        ####################################### FILTER BASED ON MIS/SYMBOL/QUANTITY/ORDER TYPE #########################
        if not isSquareOffHedging:
            df_ActivePositions = df_ActivePositions[(df_ActivePositions['product'] == tradeType) &
                                                    (df_ActivePositions["tradingsymbol"].str.startswith(symbol, na=False)) &
                                                    (df_ActivePositions['quantity'] < 0)]
        else:
            df_ActivePositions = df_ActivePositions[(df_ActivePositions['product'] == tradeType) &
                                                    (df_ActivePositions["tradingsymbol"].str.startswith(symbol, na=False)) &
                                                    (df_ActivePositions['quantity'] > 0)]

        df_ActivePositions.reset_index(drop=True)

        df_ActivePositions['quantity_active'] = df_ActivePositions['quantity'].abs()
        df_ActivePositions = df_ActivePositions[['quantity_active', 'tradingsymbol']]

        ########### IF ACTIVE POSITION LESS THAN SQUAREOFF POSITIONS, RECILIATION FAILED, STOP AUTOMATION ##############
        if len(df_ActivePositions) == 0:
            stopAutomationAndRaiseExceptionNoPositions(Configuration, orderType, symbol)

        if len(df_ActivePositions) < len(df_SquareOff_Order):
            squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)
            stopAutomationAndRaiseExceptionMismatch(Configuration, df_ActivePositions, orderType, symbol)
        ################################################################################################################

        df_ActivePositions = pd.merge(df_SquareOff_Order, df_ActivePositions, on='tradingsymbol',
                                          how='inner')

        ########### IF ACTIVE POSITION EQUALS TO ZERO, RECILIATION FAILED, STOP AUTOMATION #############################
        if len(df_ActivePositions) == 0:
            stopAutomationAndRaiseExceptionNoPositions(Configuration, orderType, symbol)

        if len(df_ActivePositions) < len(df_SquareOff_Order):
            squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)
            stopAutomationAndRaiseExceptionMismatch(Configuration, df_ActivePositions, orderType, symbol)
        ################################################################################################################

        df_ActivePositions['isMatched'] = np.where(df_ActivePositions['quantity_active'] == df_SquareOff_Order['quantity_squareoff'], True, False)
        print(df_ActivePositions)

        df_Not_Matched = df_ActivePositions[df_ActivePositions['isMatched'] == False]
        df_Not_Matched.reset_index(drop=True, inplace=True)


        ############################################# IF MISMATCH, RECILIATION FAILED, STOP AUTOMATION #################
        if len(df_Not_Matched) > 0:
            squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)
            stopAutomationAndRaiseExceptionMismatch(Configuration, df_ActivePositions, orderType, symbol)
        ################################################################################################################

    except Exception as ex:
        # MAKE BROKER API ACTIVE AND SYMBOL ACTIVE FALSE
        squareOffActivePositions(Configuration, OM_URL, symbol, position_group_id)
        stopAutomation(Configuration['SCHEMA_NAME'], symbol)

        template = "Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(message)
        print(traceback.format_exc())
        logging.info(traceback.format_exc())
        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            subject = "FAILURE: ORDER_TYPE : " + orderType + " | SYMBOL : " + symbol + " | STRATEGY : " + Configuration[
                'SCHEMA_NAME']
            utils.send_email_dqns(Configuration, subject, message, "HTML")
            utils.send_sns(Configuration)
        raise ValueError(message)


def stopAutomation(schema_name, symbol):
    from dao import GlobalConfigurationDAO
    configurationDAO = ClientConfigurationDAO
    # configurationDAO.updateConfiguration('BROKER_API_ACTIVE', 'N')
    configurationDAO.updateConfiguration(schema_name, 'SYMBOL_ACTIVE_' + symbol, 'N')

def send_mail(Configuration, df_Order, symbol, orderType):
    df_Order_mail = df_Order.copy()
    df_Order_mail["trading_symbol"] = df_Order_mail["TRADING_SYMBOL"]
    df_Order_mail["qty"] = df_Order_mail["QUANTITY"]
    df_Order_mail = df_Order_mail[["trading_symbol", "qty", "average_price", "status", "zorderid"]]
    df_Order_mail.set_index("trading_symbol", inplace=True)
    subject = Configuration['SCHEMA_NAME']+ " | " + symbol + " | " + orderType
    order_placed_body = ' Hi Team,<br><br><nbsp>This is to inform you that '+Configuration['SCHEMA_NAME']+' Strategy has taken position, Details Mentioned below:<br><br>{0}'.format(df_Order_mail.to_html())
    order_placed_body = order_placed_body + '<br>Thanks and Regards,<br>Team Finnovesh'
    utils.send_email_dqns(Configuration, subject, order_placed_body, "HTML")

def stopAutomationAndRaiseExceptionMismatch(Configuration, df_ActivePositions, orderType, symbol):
    # MAKE BROKER API ACTIVE AND SYMBOL ACTIVE FALSE
    print("###########################   stopAutomationAndRaiseExceptionMismatch #######################################")
    logging.info("#######################   stopAutomationAndRaiseExceptionMismatch #######################################")
    stopAutomation(Configuration['SCHEMA_NAME'], symbol)
    message = '<b>QUANTITY MISMATCH IN BROKER AND DB. NEED MANUAL INTERVENTION.</b><br><br>' + df_ActivePositions.to_html()
    subject = "RECONCILIATION/SQUAREOFF FAILED: ORDER_TYPE : " + orderType + " | SYMBOL : " + symbol + " | STRATEGY : " + \
              Configuration['SCHEMA_NAME']
    if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
        utils.send_email_dqns(Configuration, subject, message, "HTML")
    raise ValueError(subject)


def stopAutomationAndRaiseExceptionNoPositions(Configuration, orderType, symbol):
    print("###########################   stopAutomationAndRaiseExceptionNoPositions #######################################")
    logging.info("#######################   stopAutomationAndRaiseExceptionNoPositions #######################################")
    # MAKE BROKER API ACTIVE AND SYMBOL ACTIVE FALSE
    stopAutomation(Configuration['SCHEMA_NAME'], symbol)
    message = '<b>QUANTITY MISMATCH IN BROKER AND DB. NO ACTIVE POSITIONS IN ZERODHA. NEED MANUAL INTERVENTION.</b>'
    subject = "RECONCILIATION/SQUAREOFF FAILED: ORDER_TYPE : " + orderType + " | SYMBOL : " + symbol + " | STRATEGY : " + \
              Configuration['SCHEMA_NAME']
    if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
        utils.send_email_dqns(Configuration, subject, message, "HTML")
    raise ValueError(subject)

def reverseOrderType(orderType):
    if orderType == 'BUY':
        return 'SELL'
    else:
        return 'BUY'

def squareOffActivePositions(Configuration, ZERODHA_ORDER_URL, symbol, position_group_id):
    ####################################################################################################################
    # USAGE : 7 PLACES FROM OMAdaptor, 1 PLACE FROM OOPSSquareoffActivePositionsController
    # BE CAREFUL IF U ADD OR REMOVE PARAMETER
    ####################################################################################################################
    tradeType = Configuration['TRADE_TYPE']
    orderType = "SQUAREOFF_ACTIVE_EXCEPTION"

    # squareoff_params = "NA"
    # if isSquareOffRealignment:
    #     squareoff_params = "SQUAREOFF_REALIGNED"


    max_retries = 4
    retry = 0
    while (retry < max_retries):
        try:
            time.sleep(3)

            retry = retry + 1
            ts_start = time.time()
            ################################ FETCH ACTIVE POSITIONS ############################################################
            activePositionsResponse = utils.getOMServiceCallHelper(Configuration).fetchActivePositions(Configuration, ZERODHA_ORDER_URL, orderType)
            ####################################################################################################################

            ############################################## IF NO ACTIVE POSITIONS, RETURN ###################################
            if len(activePositionsResponse) == 0:
                updateTrackerTableWhileSquareOffException(Configuration, symbol)
                updatePositionsTablePnl(Configuration, symbol, position_group_id)
                return
            ################################################################################################################

            df_ActivePositions = pd.DataFrame(activePositionsResponse)

            ############################### FILTER TRADES BASED ON SYMBOL AND ADD ORDER TYPE ###############################
            df_ActivePositions = df_ActivePositions[(df_ActivePositions['product'] == tradeType) &
                                                    (df_ActivePositions["tradingsymbol"].str.startswith(symbol, na=False)) &
                                                    (df_ActivePositions['quantity'] != 0)]
            df_ActivePositions['order_type'] =  np.where(df_ActivePositions['quantity']>0, 'SELL', 'BUY')  # Reverse as squaring off
            df_ActivePositions['quantity'] = df_ActivePositions['quantity'].abs()
            df_ActivePositions.reset_index(drop=True)

            ############################################## IF NO ACTIVE POSITIONS, RETURN ###################################
            if len(df_ActivePositions) == 0:
                updateTrackerTableWhileSquareOffException(Configuration, symbol)
                updatePositionsTablePnl(Configuration, symbol, position_group_id)
                return


            ######################### PLACE BUY(ALREDAY SELL PLACED) ORDER FIRST TO AVOID MARGIN CALLS #################
            df_Order = pd.DataFrame()
            df_ActivePositions_Buy = df_ActivePositions[df_ActivePositions['order_type'] == 'BUY']
            if len(df_ActivePositions_Buy) > 0:
                df_Order_Sell = placeOrderSquareOffActivePositions(Configuration, ZERODHA_ORDER_URL, df_ActivePositions_Buy,
                                                                   position_group_id, orderType, symbol, tradeType, ts_start)
                df_Order = df_Order_Sell
            ############################################################################################################

            ######################### PLACE SELL(ALREDAY BUY PLACED) ORDER SECOND TO AVOID MARGIN CALLS ################
            df_ActivePositions_Sell = df_ActivePositions[df_ActivePositions['order_type'] == 'SELL']
            if len(df_ActivePositions_Sell) > 0:
                df_Order_Buy = placeOrderSquareOffActivePositions(Configuration, ZERODHA_ORDER_URL, df_ActivePositions_Sell,
                                                                  position_group_id, orderType, symbol, tradeType, ts_start)

                if len(df_Order) > 0:
                    df_Order = pd.concat([df_Order, df_Order_Buy], ignore_index=True)
                else:
                    df_Order = df_Order_Buy
            ############################################################################################################

            # SEND MAIL AFTER BROKER ORDER PLACEMENT
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                send_mail(Configuration, df_Order, symbol, orderType)

            # Exit Loop
            if retry == max_retries:
                updateTrackerTableWhileSquareOffException(Configuration, symbol)
                updatePositionsTablePnl(Configuration, symbol, position_group_id)
                break
        except Exception as ex:
            template = "Exception {} occurred with message : {}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            logging.info(message)
            print(traceback.format_exc())
            logging.info(traceback.format_exc())
            if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                subject = "FAILURE: ORDER_TYPE : " + orderType + " | SYMBOL : " + symbol + " | STRATEGY : " + Configuration[
                    'SCHEMA_NAME']
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)

            # Exit Loop
            if retry == max_retries:
                updateTrackerTableWhileSquareOffException(Configuration, symbol)
                updatePositionsTablePnl(Configuration, symbol, position_group_id)
                break


def placeOrderSquareOffActivePositions(Configuration, ZERODHA_ORDER_URL, df_ActivePositions, oops_trans_id, orderType, symbol, tradeType,
                   ts_start):
    ############################################ CREATE ORDER REQUEST ##############################################
    df_Order, order_manifest, isQtyFreeze = OMRequestProcessor.createOrderDataFrameSquareOffException(
        df_ActivePositions, Configuration, symbol)
    margin_txn = -10000000.0
    Configuration['ORDER_TYPE_MARKET_LIMIT'] = 'MARKET'
    order_dict = OMRequestProcessor.createOrderRequest(df_Order, order_manifest, Configuration, orderType,
                                                       margin_txn, tradeType, oops_trans_id)
    ###################################### PLACE FRESH ORDER ###################################################
    orderResponseDict, statusCode = utils.getOMServiceCallHelper(Configuration).placeOrder(Configuration,
                                                                                           ZERODHA_ORDER_URL,
                                                                                           order_dict,
                                                                                           orderType, symbol,
                                                                                           isFresh=False,
                                                                                           isHedging=False,
                                                                                           isSquareOff=True,
                                                                                           isMarginFreeUp=False,
                                                                                           isQtyFreeze=isQtyFreeze,
                                                                                           isIteration_1=True)
    df_orderDigestResponse = OMAdaptor().processOrderDigestResponse(orderResponseDict)
    ############################################################################################################
    logging.info(
        '{}: df_orderDigestResponse Values - \n{}'.format(Configuration['SCHEMA_NAME'], df_orderDigestResponse))
    ts_end = time.time()
    time_taken = ts_end - ts_start
    logging.info('{}: TIME TAKEN TO PLACE {} ORDER IN BROKER FOR {} IS : {}'.format(Configuration['SCHEMA_NAME'],
        orderType, str(symbol), str(time_taken)))
    logging.info(
        '\n********************************************* BROKER PLACE ORDER : END  ************************************************\n')
    ############################### POST ORDER PLACEMENT - MERGE INPUT AND OUTPUT DF ###########################
    if not isQtyFreeze:
        df_orderDigestResponse['TRADING_SYMBOL'] = df_orderDigestResponse['tradingsymbol']
        df_Order = pd.merge(df_Order, df_orderDigestResponse, on='TRADING_SYMBOL', how='inner')
    else:
        df_orderDigestResponse['TAG'] = df_orderDigestResponse['tag']
        df_Order = pd.merge(df_Order, df_orderDigestResponse, on='TAG', how='inner')
    return df_Order


def squareOffPositionsByDataframe(df_orderDigestResponse, Configuration, ZERODHA_ORDER_URL, symbol, oops_trans_id):
    ####################################################################################################################
    # USAGE : 1.PLACE FROM OMAdaptor, 2.ReconciliationUtil
    ####################################################################################################################
    # df_orderDigestResponse contains 3 columns --> quantity, tradingsymbol, order_type -- use this DF when to squareoff
    #                                               for reconciliation
    ####################################################################################################################
    tradeType = Configuration['TRADE_TYPE']
    orderType = "SQUAREOFF_POSITIONS_PARTIAL"
    try:
        ts_start = time.time()

        ############################################ CREATE ORDER REQUEST ##############################################
        # REQUIRE 3 COLUMNS IN DF : quantity/tradingsymbol/order_type
        df_Order, order_manifest, isQtyFreeze = OMRequestProcessor.createOrderDataFrameSquareOffException(df_orderDigestResponse, Configuration, symbol)

        margin_txn = -10000000.0
        order_dict = OMRequestProcessor.createOrderRequest(df_Order, order_manifest, Configuration, orderType,
                                        margin_txn, tradeType, oops_trans_id)

        ###################################### PLACE FRESH ORDER ###################################################
        orderResponseDict, statusCode = utils.getOMServiceCallHelper(Configuration).placeOrder(Configuration, ZERODHA_ORDER_URL, order_dict,
                                                                       orderType, symbol,
                                                                       isFresh=False, isHedging=False, isSquareOff=True, isMarginFreeUp=False, isQtyFreeze = isQtyFreeze, isIteration_1=True)
        df_orderDigestResponse = OMAdaptor().processOrderDigestResponse(orderResponseDict)
        ############################################################################################################

        logging.info(
            '{}: df_orderDigestResponse Values - \n{}'.format(Configuration['SCHEMA_NAME'], df_orderDigestResponse))
        ts_end = time.time()
        time_taken = ts_end - ts_start
        logging.info('{}: TIME TAKEN TO PLACE {} ORDER IN BROKER FOR {} IS : {}'.format(Configuration['SCHEMA_NAME'],
            orderType, str(symbol), str(time_taken)))
        logging.info('\n********************************************* BROKER PLACE ORDER : END  ************************************************\n')

        ############################### POST ORDER PLACEMENT - MERGE INPUT AND OUTPUT DF ###########################
        if not isQtyFreeze:
            df_orderDigestResponse['TRADING_SYMBOL'] = df_orderDigestResponse['tradingsymbol']
            df_Order = pd.merge(df_Order, df_orderDigestResponse, on='TRADING_SYMBOL', how='inner')
        else:
            df_orderDigestResponse['TAG'] = df_orderDigestResponse['tag']
            df_Order = pd.merge(df_Order, df_orderDigestResponse, on='TAG', how='inner')

        # SEND MAIL AFTER BROKER ORDER PLACEMENT
        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            send_mail(Configuration, df_Order, symbol, orderType)


    except Exception as ex:
        template = "Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(message)
        print(traceback.format_exc())
        logging.info(traceback.format_exc())
        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            subject = "FAILURE: ORDER_TYPE : " + orderType + " | SYMBOL : " + symbol + " | STRATEGY : " + Configuration[
                'SCHEMA_NAME']
            utils.send_email_dqns(Configuration, subject, message, "HTML")
            utils.send_sns(Configuration)


def updatePositionsTablePnl(Configuration, symbol, oops_trans_id):
    ############################################################################################################
    ########################################## UPDATE TRACKER TABLE - FORCEFULLY CLOSED STATUS #################
    try:
        positionsResponseDict = PositionsUtil.fetchPositions(Configuration)

        ########################### NET PNL DETAILS #####################################
        netPositionsResponseDict = positionsResponseDict['netPositionsDetails']
        NET_PNL = float(netPositionsResponseDict['netPnl_'+symbol])

        PositionsDAO.updatePositionsPnlSquareOffException(Configuration['SCHEMA_NAME'], oops_trans_id, symbol, False, str(NET_PNL))

        logging.info("{}: Positions Table with Pnl Updated Successfully Post SquareOff for symbol : {}".format(Configuration['SCHEMA_NAME'], symbol))

    except Exception as ex:
        template = "Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(message)
        print(traceback.format_exc())
        logging.info(traceback.format_exc())
        subject = "FAILURE | SQUAREOFF ACTIVE POSITIONS WHILE UPDATING POSITIONS TABLE| " + Configuration[
            'SCHEMA_NAME']
        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            utils.send_email_dqns(Configuration, subject, message, "HTML")
    ############################################################################################################

def updateCRMPnlAudit(Configuration, symbol, oops_trans_id):
    ############################################################################################################
    ########################################## UPDATE TRACKER TABLE - FORCEFULLY CLOSED STATUS #################
    try:
        squareOff_dict = {}
        squareOff_dict['symbol'] = symbol
        squareOff_dict['oops_trans_id'] = oops_trans_id

        logging.info("CRM Pnl Audit Updated Successfully Post SquareOff for symbol : {}".format(symbol))

    except Exception as ex:
        template = "Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(message)
        print(traceback.format_exc())
        logging.info(traceback.format_exc())
        subject = "FAILURE | SQUAREOFF ACTIVE POSITIONS WHILE UPDATING CRM AUDIT| " + Configuration[
            'SCHEMA_NAME']
        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            utils.send_email_dqns(Configuration, subject, message, "HTML")
    ############################################################################################################

def updateTrackerTableWhileSquareOffException(Configuration, symbol):
    ############################################################################################################
    ########################################## UPDATE TRACKER TABLE - FORCEFULLY CLOSED STATUS #################
    try:
        tracker_dict = {}
        tracker_dict["QTY_COUNT_BROKER"] = 0.0
        tracker_dict["POSITIONS_COUNT_DB"] = 0.0
        tracker_dict["QTY_COUNT_DB"] = 0.0
        tracker_dict["POSITIONS_CLOSED"] = 'N'
        tracker_dict["POSITIONS_MATCHED"] = 'N'
        tracker_dict["RAG"] = "BFC"
        tracker_dict["NET_PNL"] = 0.0
        tracker_dict["NET_PNL_PCT"] = 0.0
        positionsResponseDict = PositionsUtil.fetchPositions(Configuration)

        positionsDetails = positionsResponseDict['positionsDetailsList']
        activePositionsSymbol = positionsDetails['activePositions_'+symbol]

        ########################### NET PNL DETAILS #####################################
        netPositionsResponseDict = positionsResponseDict['netPositionsDetails']
        NET_PNL = float(netPositionsResponseDict['netPnl_'+symbol])
        MARGIN_USED = float(Configuration['TOTAL_INITIAL_MARGIN'])
        if NET_PNL != 0.0 and MARGIN_USED != 0.0:
            NET_PNL_PCT = round(utils.percentageIsXofY(NET_PNL, MARGIN_USED), 2)
            tracker_dict["NET_PNL"] = NET_PNL
            tracker_dict["NET_PNL_PCT"] = NET_PNL_PCT

        ########################## POSITIONS COUNT #####################################
        df_ActivePositions_Symbol = pd.DataFrame(activePositionsSymbol)
        if len(df_ActivePositions_Symbol) > 0 :
            df_ActivePositions_Symbol['quantity_active'] = df_ActivePositions_Symbol['quantity'].abs()
            df_ActivePositions_Symbol['quantity_active'] = df_ActivePositions_Symbol['quantity_active'].astype(float)
            tracker_dict["QTY_COUNT_BROKER"] = df_ActivePositions_Symbol['quantity_active'].sum()

        tracker_dict["POSITIONS_COUNT_BROKER"] = len(df_ActivePositions_Symbol)

        # UPDATED TIME
        tracker_dict["LAST_UPDATED_TIME"] = datetime.now(pytz.timezone('Asia/Kolkata')).strftime(
            '%Y-%m-%d %H:%M:%S')

        # UPDATE POSITIONS_CLOSED FLAG IN CASE ALL SQUARED-OFF
        if tracker_dict["POSITIONS_COUNT_BROKER"] == 0 and tracker_dict["QTY_COUNT_BROKER"] == 0:
            tracker_dict["POSITIONS_CLOSED"] = 'Y'
            tracker_dict["POSITIONS_MATCHED"] = 'Y'

        # CONVERT EVERYTHING IN STRING
        for key in tracker_dict:
            tracker_dict[key] = str(tracker_dict[key])
        TrackerDAO.updateTrackerPostSquareOffException(Configuration['SCHEMA_NAME'], tracker_dict, symbol)
        logging.info("{}: Tracker Updated Successfully Post SquareOff for symbol : {}".format(Configuration['SCHEMA_NAME'], symbol))

    except Exception as ex:
        template = Configuration['SCHEMA_NAME']+": Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(message)
        print(traceback.format_exc())
        logging.info(traceback.format_exc())
        subject = "FAILURE | SQUAREOFF ACTIVE POSITIONS WHILE UPDATING TRACKER TABLE| " + Configuration[
            'SCHEMA_NAME']
        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            utils.send_email_dqns(Configuration, subject, message, "HTML")
    ############################################################################################################

