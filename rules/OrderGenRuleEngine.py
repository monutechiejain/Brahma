import logging
from common import utils
from datetime import datetime
import pytz


def checkBBWThreshold(orderGen_dict, Configuration):

    # BBW VALUE
    BBW = orderGen_dict['BBW']
    BBW_THRESHOLD = float(Configuration['BBW_THRESHOLD'])
    orderGen_dict['isDealAllowed'] = False

    # RULE 1: IF BBW IS LESS THAN THRESHOLD, IS DEAL ALLOWED TRUE
    if BBW < BBW_THRESHOLD:
        orderGen_dict['isDealAllowed'] = True

    return orderGen_dict


def decisonMaker(orderGen_dict, Configuration):
    #################################### RULES ENGINE FACTORY ##########################################################
    df_Put_Call = orderGen_dict['df_Put_Call']
    position_group_id = orderGen_dict['position_group_id']
    symbol = orderGen_dict['symbol']
    SP_ATM = orderGen_dict['SP_ATM']
    orderGen_dict['ENTRY_ATM_PUT_PRICE'] = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == SP_ATM]['BID_PRICE_PUT'].iloc[0]
    orderGen_dict['ENTRY_ATM_CALL_PRICE'] = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == SP_ATM]['BID_PRICE_CALL'].iloc[0]
    orderGen_dict['ENTRY_ATM_AVG_PRICE'] = (orderGen_dict['ENTRY_ATM_PUT_PRICE'] + orderGen_dict['ENTRY_ATM_CALL_PRICE'])/2
    orderGen_dict['ENTRY_ATM_PRICE_DIFF'] = abs((orderGen_dict['ENTRY_ATM_PUT_PRICE'] - orderGen_dict['ENTRY_ATM_CALL_PRICE'])
                                                /min(orderGen_dict['ENTRY_ATM_PUT_PRICE'],orderGen_dict['ENTRY_ATM_CALL_PRICE'])) *100
    spot_value = orderGen_dict['spot_value']
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')
    MF_STEP_SIZE = orderGen_dict['MF_STEP_SIZE']
    BID_ASK_THRESHOLD = float(Configuration['BID_ASK_THRESHOLD'])
    orderGen_dict['isDealAllowed'] = True
    df_Put_Call_Original = df_Put_Call.copy()

    ################################### STEP 1:  FILTER BID_PRICE/ASK_PRICE WITCH AS ZERO VALUES #################################
    df_Put_Call = df_Put_Call[((df_Put_Call['BID_PRICE_PUT'] > 0) & (df_Put_Call['STRIKE_PRICE'] <= SP_ATM)) | (
                                                df_Put_Call['STRIKE_PRICE'] >= SP_ATM)]
    df_Put_Call.reset_index(drop=True, inplace=True)

    df_Put_Call = df_Put_Call[((df_Put_Call['ASK_PRICE_PUT'] > 0) & (df_Put_Call['STRIKE_PRICE'] <= SP_ATM)) | (
            df_Put_Call['STRIKE_PRICE'] >= SP_ATM)]
    df_Put_Call.reset_index(drop=True, inplace=True)

    df_Put_Call = df_Put_Call[((df_Put_Call['BID_PRICE_CALL'] > 0) & (df_Put_Call['STRIKE_PRICE'] >= SP_ATM)) | (
            df_Put_Call['STRIKE_PRICE'] < SP_ATM)]
    df_Put_Call.reset_index(drop=True, inplace=True)

    df_Put_Call = df_Put_Call[((df_Put_Call['ASK_PRICE_CALL'] > 0) & (df_Put_Call['STRIKE_PRICE'] >= SP_ATM)) | (
            df_Put_Call['STRIKE_PRICE'] < SP_ATM)]
    df_Put_Call.reset_index(drop=True, inplace=True)
    ####################################################################################################################

    ################################### STEP 2: FILTER CALL LEGS   #####################################################
    # CALL LEG 1 -> CALL SELL WITH ATM SP
    ################################ LEG 1 CALL PROCESSING ########################################
    df_Call_Leg_1 = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == SP_ATM]
    df_Call_Leg_1['INSTRUMENT_TYPE'] = 'CALL'
    df_Call_Leg_1['ORDER_TYPE_CALL'] = 'SELL'
    df_Call_Leg_1['QUANTITY_CALL'] = df_Call_Leg_1['QUANTITY_CALL']
    df_Call_Leg_1['MULTI_FACTOR'] = df_Call_Leg_1['MULTI_FACTOR']
    SP_CALL_LEG_1 = int(df_Call_Leg_1['STRIKE_PRICE'].iloc[0])
    ORDER_MANIFEST = str(SP_CALL_LEG_1) + '_CALL_SELL_LEG_1'

    #######################################  ORDER MANIFEST - ADD TRADE DATE ###########################################
    if utils.isBackTestingEnv(Configuration):
        trade_date =""
        pass
    else:
        trade_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%b%d').upper()
    ORDER_MANIFEST = trade_date + '_' + ORDER_MANIFEST
    ####################################################################################################################

    df_Call_Leg_1['ORDER_MANIFEST'] = ORDER_MANIFEST
    df_Call_Leg_1.reset_index(drop=True, inplace=True)

    if len(df_Call_Leg_1) is not 1:
        raise ValueError("Size of df_Call_Leg_1 Dataframe should be 1!!")

    # ADD TO PARENT DATAFRAME
    df_Call = df_Call_Leg_1

    ################################### STEP 3: FILTER PUT LEGS   #####################################################
    # PUT LEG 1 -> PUT SELL WITH ATM SP
    ################################ LEG 1 PUT PROCESSING ########################################
    df_Put_Leg_1 = df_Put_Call[df_Put_Call['STRIKE_PRICE'] == SP_ATM]
    df_Put_Leg_1['INSTRUMENT_TYPE'] = 'PUT'
    df_Put_Leg_1['ORDER_TYPE_PUT'] = 'SELL'
    df_Put_Leg_1['QUANTITY_PUT'] = df_Put_Leg_1['QUANTITY_PUT']
    df_Put_Leg_1['MULTI_FACTOR'] = df_Put_Leg_1['MULTI_FACTOR']
    SP_PUT_LEG_1 = int(df_Put_Leg_1['STRIKE_PRICE'].iloc[0])
    ORDER_MANIFEST = str(SP_PUT_LEG_1) + '_PUT_SELL_LEG_1'

    #######################################  ORDER MANIFEST - ADD TRADE DATE ###########################################
    if utils.isBackTestingEnv(Configuration):
        trade_date = ""
        pass
    else:
        trade_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%b%d').upper()
    ORDER_MANIFEST = trade_date + '_' + ORDER_MANIFEST
    ####################################################################################################################
    df_Put_Leg_1['ORDER_MANIFEST'] = ORDER_MANIFEST
    df_Put_Leg_1.reset_index(drop=True, inplace=True)

    if len(df_Put_Leg_1) is not 1:
        raise ValueError("Size of df_Put_Leg_1 Dataframe should be 1!!")

    # ADD TO PARENT DATAFRAME
    df_Put = df_Put_Leg_1
    ####################################################################################################################

    ################################### STEP 4: CREATE df_Put_Call DF, CONCAT   #########################################
    df_Put_Call = df_Put.append(df_Call, ignore_index=True)

    # ADD df_Put_Call to Dictionary
    orderGen_dict['df_Put_Call'] = df_Put_Call

    return orderGen_dict

def populateBIDAskDiff(row):

    if row['ORDER_TYPE_'+row['INSTRUMENT_TYPE']] == 'BUY':
        row['BID_ASK_DIFF_'+row['INSTRUMENT_TYPE']] = abs(((row['BID_PRICE_'+row['INSTRUMENT_TYPE']] - row['ASK_PRICE_'+row['INSTRUMENT_TYPE']])/
                                                       row['ASK_PRICE_'+row['INSTRUMENT_TYPE']]) *100)
    else:
        row['BID_ASK_DIFF_'+row['INSTRUMENT_TYPE']] = abs(((row['ASK_PRICE_'+row['INSTRUMENT_TYPE']] - row['BID_PRICE_'+row['INSTRUMENT_TYPE']])/
                                                       row['BID_PRICE_'+row['INSTRUMENT_TYPE']]) *100)

    return row

def isDealAllowedCheck(df_Put_Call, orderGen_dict):
    df_Put_Call = df_Put_Call[df_Put_Call['isDealAllowed'] == True]
    df_Put_Call.reset_index(drop=True, inplace=True)
    print(df_Put_Call)
    if len(df_Put_Call) == 0:
        orderGen_dict['isDealAllowed'] = False
    return df_Put_Call

def IVThresholdCheckStopTrading(orderGen_dict, Configuration):
    #################################### RULES ENGINE FACTORY ##########################################################
    df_Put_Call = orderGen_dict['df_Put_Call']

    ########################################### GET IV THRESHOLD AND EXECUTION VALUES ##################################
    symbol = orderGen_dict['symbol']
    IV_THRESHOLD_MAX = float(Configuration['IV_THRESHOLD_MAX_' + symbol])
    IV_THRESHOLD_MIN = float(Configuration['IV_THRESHOLD_MIN_' + symbol])

    # POPULATE PUT AND CALL IVS of LEG 1 BUY
    EXECUTION_IV_PUT = df_Put_Call[df_Put_Call['INSTRUMENT_TYPE'] == 'PUT']['IV_PUT'].iloc[0]
    EXECUTION_IV_CALL = df_Put_Call[df_Put_Call['INSTRUMENT_TYPE'] == 'CALL']['IV_CALL'].iloc[0]

    EXECUTION_IV_MAX = max(EXECUTION_IV_PUT, EXECUTION_IV_CALL)
    EXECUTION_IV_MIN = min(EXECUTION_IV_PUT, EXECUTION_IV_CALL)
    EXECUTION_IV_AVG = (EXECUTION_IV_PUT + EXECUTION_IV_CALL)/2

    # MOCK DATA
    # EXECUTION_IV_MAX = 0.30
    # EXECUTION_IV_MIN = 0.30
    # EXECUTION_IV_AVG = 0.12
    # IV_THRESHOLD_MIN = 0.13
    if utils.isMockAndLocalEnv(Configuration):
        IV_THRESHOLD_MAX = 0.70

    ##################################### IV THRESHOLD RULES ###########################################################
    # IF IV IS OUTSIDE THRESHOLD RANGE, STOP TRADING FOR THAT SYMBOL FOR DAY
    if EXECUTION_IV_AVG != 0.0 and 'PRD' in Configuration['ENVIRONMENT']  and\
            (EXECUTION_IV_AVG > IV_THRESHOLD_MAX or EXECUTION_IV_MIN < IV_THRESHOLD_MIN
            ):
        df_Put_Call = None

        # STOP TRADING FOR DAY
        from dao import ClientConfigurationDAO
        ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'SYMBOL_ACTIVE_' + symbol, 'N')
        orderGen_dict['isDealAllowed'] =  False
        logging.info("{}: EXECUTION_IV_MAX : {}, EXECUTION_IV_MIN : {}, EXECUTION_IV_AVG: {}".format(Configuration['SCHEMA_NAME'],
            EXECUTION_IV_MAX, EXECUTION_IV_MIN, EXECUTION_IV_AVG))

        # SEND MAIL
        message = '<b>TRADING STOPPED TODAY, IV IS OUTSIDE OF EXECUTION RANGE, IV_AVG : '+str(EXECUTION_IV_AVG)+'</b>'
        subject = Configuration['SCHEMA_NAME']+" | " + symbol + " | TRADING STOPPED TODAY | IV IS OUTSIDE OF EXECUTION RANGE | IV_AVG : "+str(EXECUTION_IV_AVG)

        if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            utils.send_email_dqns(Configuration, subject, message, "HTML")
    ####################################################################################################################

    # ADD df_Put_Call to Dictionary
    orderGen_dict['df_Put_Call'] = df_Put_Call

    return orderGen_dict