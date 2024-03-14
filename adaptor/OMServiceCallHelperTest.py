import json
import logging
import requests
import traceback
import time
from common import utils

def placeOrder(Configuration, OM_URL, order_dict, orderType, symbol, isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze, isIteration_1):
    ####################################################################################################################
    # USED IN 3 PLACES FROM OM ADAPTOR - processOrder, squareOffActivePositions, squareOffPositionsByDataframe
    ######################################## PLACE ORDER IN BROKER ####################################################

    # OM_URL_PLACE_ORDER = OM_URL + '/order/zerodha/v2'
    #
    # r = requests.post(OM_URL_PLACE_ORDER, json=order_dict, headers={'X_AUTH': Configuration["BROKER_API_AUTH_KEY"]})
    # orderResponseJson = r.text
    # statusCode = r.status_code
    # logging.info('######################### {} ORDER BROKER API RESPONSE ############################\n{}\n'.format(orderType, orderResponseJson))
    # orderResponseDict = json.loads(orderResponseJson)

    ########################################## MOCK BROKER RESPONSE ###########################################
    if isFresh and isIteration_1:
        statusCode = 201
        file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_options_" + symbol + ".txt"

        # NO_ORDER_FILLED
        # statusCode = 503
        # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_NO_ORDER_FILLED_" + symbol + ".txt"
        #
        # statusCode = 551
        # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_PARTIAL_ORDER_FILLED_" + symbol + ".txt"

    if isFresh and not isIteration_1:
        statusCode = 201
        file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_options_iteration_N_" + symbol + ".txt"


    if isHedging:
        statusCode = 201
        file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_hedging_" + symbol + ".txt"

        # statusCode = 551
        # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_hedging_partial_" + symbol + ".txt"

    if isSquareOff and not isQtyFreeze:
        statusCode = 201
        file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_squareoff_" + symbol + ".txt"

        # statusCode = 551
        # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_squareoff_partial_" + symbol + ".txt"

    if isMarginFreeUp:
        statusCode = 201
        file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_options_" + symbol + ".txt"

        # statusCode = 551
        # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_hedging_partial_" + symbol + ".txt"

    if isSquareOff and isQtyFreeze:
        statusCode = 201
        file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_qty_freeze_" + symbol + ".txt"

        # statusCode = 551
        # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_response_squareoff_partial_" + symbol + ".txt"

    with open(file_path) as json_file:
        orderResponseDict = json.load(json_file)
        orderResponseDict = replaceOrdersMockExpiryDate(orderResponseDict)
    ###########################################################################################################
    return orderResponseDict, statusCode


def fetchActivePositions(Configuration, OM_URL, orderType):
    # MAKE A BROKER CALL TO GET ACTIVE POSITIONS
    OM_URL_ACTIVE_POSITIONS = OM_URL + 'portfolio/positions'

    max_retries = 5
    retry = 0
    while (retry < max_retries):

        try:
            ######################## BROKER API FETCH STATUS BY ORDER ID CALL #########################################
            #activePositionsResponse = requests.get(OM_URL_ACTIVE_POSITIONS, headers={'X_AUTH': Configuration["BROKER_API_AUTH_KEY"]}).json()

            ########################################### MOCK BROKER ORDER STATUS RESPONSE ##############################
            file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_active_positions_BANKNIFTY.txt"
            with open(file_path) as json_file:
                activePositionsResponse = json.load(json_file)
                activePositionsResponse = replaceOrdersMockExpiryDate(activePositionsResponse)
            ###########################################################################################################

            logging.info('######################### Fetch {} : Active Positions Response #####################\n{}\n'.format(orderType, activePositionsResponse))

            #raise ValueError("NetworkException, Too many requests")
            break
        except Exception as exception:
            message = 'Exception occurred while Fetching Active Positions for OrderType {} , Exception: {}'.format(
                str(orderType), str(exception))
            logging.info(message)
            print(message)
            logging.info(traceback.format_exc())
            # retrying in case of failures
            time.sleep(3)
            retry = retry + 1
            if retry == max_retries:
                raise Exception(message)

    return activePositionsResponse

def getMarginProfile(Configuration, OM_URL):
    # MAKE A BROKER CALL TO GET AVAILABLE MARGIN
    OM_URL_MARGIN_PROFILE = OM_URL + 'margin/profile'

    max_retries = 5
    retry = 0
    while (retry < max_retries):

        try:
            ######################## BROKER API FETCH STATUS BY ORDER ID CALL #########################################
            #marginProfileResponse = requests.get(OM_URL_MARGIN_PROFILE, headers={'X_AUTH': Configuration["BROKER_API_AUTH_KEY"]}).json()

            ####################################### MOCK BROKER MARGIN PROFILE RESPONSE ###############################
            file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_margin_profile.txt"
            with open(file_path) as json_file:
                marginProfileResponse = json.load(json_file)
            ###########################################################################################################

            logging.info('######################### Fetch Margin Profile Response #####################\n{}\n'.format(marginProfileResponse))

            break
        except Exception as exception:
            message = 'Exception occurred while Fetching Margin Profile , Exception: {}'.format(str(exception))
            logging.info(message)
            print(message)
            logging.info(traceback.format_exc())
            # retrying in case of failures
            time.sleep(3)
            retry = retry + 1
            if retry == max_retries:
                raise Exception(message)

    return marginProfileResponse

def getMarginCalculator(Configuration, OM_URL, margin_calculator_req, symbol, isHedged = False):
    # MAKE A BROKER CALL TO GET MARGIN CALCULATOR
    OM_URL_MARGIN_CALCULATOR = OM_URL + '/order/zerodha/margin/calculator'

    max_retries = 5
    retry = 0
    while (retry < max_retries):

        try:
            ########################## BROKER API TO FETCH MARGIN CALCULATOR ##########################################
            # r = requests.post(OM_URL_MARGIN_CALCULATOR, json=margin_calculator_req, headers={'X_AUTH': Configuration["BROKER_API_AUTH_KEY"]})
            # marginCalculatorResponse = r.text
            # statusCode = r.status_code
            # if statusCode != 200:
            #     raise Exception(marginCalculatorResponse)
            # marginCalculatorResponseDict = json.loads(marginCalculatorResponse)

            ####################################### MOCK BROKER MARGIN CALCULATOR RESPONSE ###############################
            if not isHedged:
                file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_margin_calculate_" + symbol + ".txt"
                with open(file_path) as json_file:
                    marginCalculatorResponseDict = json.load(json_file)

            if isHedged:
                file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_margin_calculate_hedged_"+symbol+".txt"
                with open(file_path) as json_file:
                    marginCalculatorResponseDict = json.load(json_file)
            ###########################################################################################################

            #logging.info('######################### Fetch Margin Calculator Response #####################\n{}\n'.format(marginCalculatorResponse))

            break
        except Exception as exception:
            message = 'Exception occurred while Fetching Margin Calculator , Exception: {}'.format(str(exception))
            logging.info(message)
            print(message)
            logging.info(traceback.format_exc())
            # retrying in case of failures
            time.sleep(3)
            retry = retry + 1
            if retry == max_retries:
                raise Exception(message)

    return marginCalculatorResponseDict

def crmPnlAuditInsert(Configuration, CRM_PNL_AUDIT_INSERT_URL, crm_pnl_insert_audit_dict):
    # MAKE A CRM_PNL_AUDIT_INSERT_URL TO ADD PNL TO CRM
    statusCode = 200
    try:
        ################################### CRM_PNL_AUDIT_INSERT_URL CALL #########################################
        # crmPnlAuditInsertResponse = requests.post(CRM_PNL_AUDIT_INSERT_URL, json=crm_pnl_insert_audit_dict, headers={'X_AUTH': Configuration["BROKER_API_AUTH_KEY"]})
        # crmPnlAuditInsertResponseJson = crmPnlAuditInsertResponse.text
        # statusCode = crmPnlAuditInsertResponse.status_code
        # logging.info('######################### Fetch CRM_PNL_AUDIT_INSERT_URL Response #####################\n{}\n'.format(crmPnlAuditInsertResponse))
        print("crmPnlAuditInsert Call Response status code: "+str(statusCode))

    except Exception as exception:
        message = 'Exception occurred while CRM_PNL_AUDIT_INSERT , Exception: {}'.format(str(exception))
        logging.info(message)
        print(message)
        logging.info(traceback.format_exc())

    return statusCode


def fetchAllOrders(Configuration, OM_URL):
    ########################################## GET ALL ORDERS CALL #################################################
    # MAKE A BROKER CALL TO GET ALL ORDERS
    OM_URL_FETCH_ALL_ORDERS = OM_URL + '/order/zerodha/v2/orders'

    max_retries = 5
    retry = 0
    while (retry < max_retries):

        try:
            ######################## BROKER API FETCH STATUS BY ORDER ID CALL #########################################
            #ordersIntradayResponse = requests.get(OM_URL_FETCH_ALL_ORDERS, headers={'X_AUTH': Configuration["BROKER_API_AUTH_KEY"]}).json()

            ########################################### MOCK BROKER ALL ORDERS STATUS RESPONSE ##############################
            file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\zerodha_orders_all_NIFTY.txt"
            with open(file_path) as json_file:
                ordersIntradayResponse = json.load(json_file)
                ordersIntradayResponse = replaceOrdersMockExpiryDate(ordersIntradayResponse)

            ###########################################################################################################
            logging.info('fetchAllOrders:: Fetch All Intraday Orders from Zerodha: {}'.format(ordersIntradayResponse))
            print('fetchAllOrders:: Fetch All Intraday Orders from Zerodha: {}'.format(ordersIntradayResponse))

            #raise ValueError("NetworkException, Too many requests")
            break
        except Exception as exception:
            message = 'Exception occurred while Fetching All Intraday Orders , Exception: {}'.format(str(exception))
            logging.info(message)
            print(message)
            logging.info(traceback.format_exc())
            # retrying in case of failures
            time.sleep(3)
            retry = retry + 1
            if retry == max_retries:
                raise Exception(message)


    return ordersIntradayResponse


def fetchOrderByOrderIds(Configuration, OM_URL_WITH_IDS, orderType, order_ids):
    max_retries = 5
    retry = 0
    while (retry < max_retries):

        try:
            ######################## BROKER API FETCH STATUS BY ORDER ID CALL #########################################
            #orderDigestStatusResponse = requests.get(OM_URL_WITH_IDS, headers={'X_AUTH': Configuration["BROKER_API_AUTH_KEY"]}).json()

            ########################################### MOCK BROKER ORDER STATUS RESPONSE ##############################
            file_path = "E:\\Workspace\\Workspace_Python_2019\\NeutralOptionsSelling\\oops1\\src\\data\\mock\\zerodha_order_status_NIFTY.txt"
            with open(file_path) as json_file:
                orderDigestStatusResponse = json.load(json_file)
                orderDigestStatusResponse = replaceOrdersMockExpiryDate(orderDigestStatusResponse)
            ###########################################################################################################
            logging.info('fetchOrderStatus:: Response from FETCH ORDER STATUS Call: {}'.format(
                str(orderDigestStatusResponse)))
            # raise ValueError("NetworkException, Too many requests")

            break
        except Exception as exception:
            message = 'Exception occurred while Fetching Order Status from Broker for OrderIDs: {} , Exception: {}'.format(
                str(order_ids), str(exception))
            logging.info(message)
            print(message)
            logging.info(traceback.format_exc())
            # retrying in case of failures
            time.sleep(3)
            retry = retry + 1
            if retry == max_retries:
                raise Exception(message)

    logging.info('Fetch {} Order Status By Order ID Retry Response: {}'.format(orderType, orderDigestStatusResponse))
    return orderDigestStatusResponse

def replaceOrdersMockExpiryDate(response_dict_list):
    # Test String to Replace
    old_expiry_date = '21JUN'
    expiry_date_options, expiry_date_futures = utils.getWeeklyExpiryDate_v2().strftime('%y%b%d').upper(), \
                                               utils.getExpiryDate().strftime('%y%b%d').upper()

    ######################################### NEW EXPIRY DATE ###########################################################
    if expiry_date_options == expiry_date_futures:
        new_expiry_date = utils.getExpiryDate().strftime('%y%b').upper()
    else:
        new_expiry_date = utils.getWeeklyExpiryDate_v2().strftime('%y%#m%d').upper()

        if utils.getExpiryDate().strftime('%#m') in ['10','11','12']:
            if utils.getExpiryDate().strftime('%#m') == '10':
                new_expiry_date = utils.getExpiryDate().strftime('%y')+'O'+utils.getWeeklyExpiryDate_v2().strftime('%d')
            elif utils.getExpiryDate().strftime('%#m') == '11':
                new_expiry_date = utils.getExpiryDate().strftime('%y') + 'N' + utils.getWeeklyExpiryDate_v2().strftime('%d')
            elif utils.getExpiryDate().strftime('%#m') == '12':
                new_expiry_date = utils.getExpiryDate().strftime('%y') + 'D' + utils.getWeeklyExpiryDate_v2().strftime('%d')
    ####################################################################################################################

    if not isinstance(response_dict_list, list):
        for k, v in response_dict_list.items():
            response_dict_list[k] = response_dict_list[k].replace(old_expiry_date, new_expiry_date)
    else:# LIST OF DICTS FOR ACTIVE POSITION RESPONSE
        for i in response_dict_list:
            for k, v in i.items():
                if isinstance(i[k], str):
                    i[k] = i[k].replace(old_expiry_date, new_expiry_date)

    return response_dict_list
