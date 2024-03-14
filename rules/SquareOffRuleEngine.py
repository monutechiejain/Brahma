from datetime import datetime
import pytz
import logging
from common import utils as parent_utils
import pandas as pd
from dao import ClientConfigurationDAO
from entities.Enums import TransactionTypeEnum
from datetime import date
from config.cache.BasicCache import BasicCache
import json

def decisonMaker(squareOff_dict, Configuration):
    df_positions_existing = squareOff_dict['df_positions_existing']
    symbol = squareOff_dict['symbol']
    df_positions_existing['is_square_off'] = False
    df_positions_existing['current_theta_pnl_pending'] = 'NA'
    df_positions_existing['net_pnl_threshold'] = 'NA'
    squareOff_dict['expected_theta_pnl_pending'] = 0.0
    squareOff_dict['current_theta_pnl_pending'] = 0.0
    squareOff_dict['net_pnl_threshold'] = 0.0
    squareOff_dict['squareoff_type'] = 'NA'
    schema_name = squareOff_dict['schema_name']

    ###########################################    SQUAREOFF RULES  ####################################################
    df_positions_existing = df_positions_existing.apply(lambda row: populateTimeValue(row, squareOff_dict), axis=1)
    df_positions_existing['TIME_VALUE_OPTIONS'] = df_positions_existing['TIME_VALUE_OPTIONS'].sum()

    ######################## RULE NO N-1 : MARKET CLOSE TIME, SQUAREOFF ALL EXISTING POSITIONS #########################
    # LAST DAY BEFORE EXPIRY EXIT AT 3:20 AROUND
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
    current_date = date.today()
    EXIT_TIME = Configuration['EXIT_TIME'].split(",")
    expiry_date = squareOff_dict['expiry_date']

    ####################################### MOCK DELTA FOR LOCAL TESTING ###############################################
    if parent_utils.isMockAndLocalEnv(Configuration):
        current_time = BasicCache().get("EXIT_TIME_CURRENT")
        current_date = BasicCache().get("CURRENT_DATE")

    if parent_utils.isBackTestingEnv(Configuration):
        current_time = BasicCache().get('CURRENT_TIME')
        current_date = BasicCache().get('CURRENT_DATE')
    ####################################################################################################################

    if current_time in EXIT_TIME:
        df_positions_existing = df_positions_existing.apply(lambda row: positionsCloseTime(row, squareOff_dict), axis=1)
        logging.info("!!!!!!! SQUARED OFF ALL, AS IT IS EXIT TIME: {} AND LAST DAY BEFORE EXPIRY : {}".format(current_time, current_date))

    ############################  RULE NO N : IS_MANUAL_OVERRIDE == 'Y', THEN SQUAREOFF ################################
    df_positions_existing = df_positions_existing.apply(lambda row: isManualOverrideCheck(row, symbol, Configuration, squareOff_dict), axis=1)

    squareOff_dict['df_positions_existing'] = df_positions_existing
    return squareOff_dict


def setInactive(row):
    row['IS_ACTIVE'] = 'N'
    return row


def isManualOverrideCheck(row, symbol, Configuration, squareOff_dict):
    IS_MANUAL_OVERRIDE = Configuration['IS_MANUAL_OVERRIDE_'+symbol]

    if IS_MANUAL_OVERRIDE == "Y":
        setSquareOffFields(row, squareOff_dict)

        # RESET MANUAL OVERRIDE FLAG
        ClientConfigurationDAO.updateConfiguration(squareOff_dict['schema_name'], 'IS_MANUAL_OVERRIDE_' + symbol, 'N')
        logging.info("MANUAL OVERRIDE TRIGGERED FOR SYMBOL : {}".format(symbol))

    return row

def populateTimeValue(row, squareOff_dict):
    spot_value = float(squareOff_dict['spot_value'])
    TIME_VALUE = 0.0

    # ADD TIME VALUE ONLY FOR OPTIONS SELL POSITION LIKE PUT_SELL, CALL_SELL
    if row['INSTRUMENT_TYPE'] == 'CALL' and row['ORDER_TYPE'] == 'SELL':
        TIME_VALUE = row['CURRENT_PRICE_ACTUAL'] - max(0, (spot_value - float(row['STRIKE_PRICE'])))

    if row['INSTRUMENT_TYPE'] == 'PUT' and row['ORDER_TYPE'] == 'SELL':
        TIME_VALUE = row['CURRENT_PRICE_ACTUAL'] - max(0, (float(row['STRIKE_PRICE']) - spot_value))


    row['TIME_VALUE_OPTIONS'] = TIME_VALUE
    return row


def checkTrailingPnl(Configuration, df_positions_existing, squareOff_dict, symbol):
    row = df_positions_existing.iloc[0]

    # MOCK DATA
    #row['UNREALIZED_PNL_TXN'] = 20000.0
    #row['TRAILING_PNL_POSITION'] = 21500.0

    IS_TRAILING_ACTIVE = row['IS_TRAILING_ACTIVE_POSITION']

    # TRAILING_PNL_LAST
    if row['TRAILING_PNL_POSITION'] != "NA":
        TRAILING_PNL_LAST = float(row['TRAILING_PNL_POSITION'])
    else:
        TRAILING_PNL_LAST = row['TRAILING_PNL_POSITION']

    PROFIT_AMOUNT_TRALING_ACTIVE_THRESHOLD = float(Configuration['PROFIT_AMOUNT_TRALING_ACTIVE_THRESHOLD_' + symbol])
    PROFIT_PCT_TRAILING_SQUAREOFF_THRESHOLD = float(Configuration['PROFIT_PCT_TRAILING_SQUAREOFF_THRESHOLD_' + symbol])
    PROFIT_AMOUNT_TRAILING_SQUAREOFF_THRESHOLD = float(
        Configuration['PROFIT_AMOUNT_TRAILING_THRESHOLD_' + symbol])

    # NET_PNL_OVERALL
    if row['REALIZED_PNL_OVERALL'] != "NA":
        NET_PNL_OVERALL = float(row['UNREALIZED_PNL_TXN']) + float(row['REALIZED_PNL_OVERALL'])
    else:
        NET_PNL_OVERALL = float(row['UNREALIZED_PNL_TXN'])

    # TRAILING_PNL_CURRENT
    TRAILING_PNL_CURRENT = NET_PNL_OVERALL - min(PROFIT_AMOUNT_TRAILING_SQUAREOFF_THRESHOLD,
                                                 parent_utils.xPercentageOfY(PROFIT_PCT_TRAILING_SQUAREOFF_THRESHOLD,
                                                                             NET_PNL_OVERALL))
    #################################### TRAILING RULES ##############################################################
    # FIRST TIME TRAILING SET
    if IS_TRAILING_ACTIVE == 'N' and NET_PNL_OVERALL > PROFIT_AMOUNT_TRALING_ACTIVE_THRESHOLD:
        row['IS_TRAILING_ACTIVE'] = 'Y'
        row['TRAILING_PNL'] = TRAILING_PNL_CURRENT
    # UPDATE TRAILING PNL, IT HAS MOVED UP
    elif IS_TRAILING_ACTIVE == 'Y' and TRAILING_PNL_CURRENT > TRAILING_PNL_LAST:
        row['IS_TRAILING_ACTIVE'] = 'Y'
        row['TRAILING_PNL'] = TRAILING_PNL_CURRENT
    # DO NOT UPDATE TRAILING PNL, IF LAST TRAILING PNL BETWEEN TRAILING_PNL_CURRENT AND NET_PNL_OVERALL
    elif IS_TRAILING_ACTIVE == 'Y' and TRAILING_PNL_CURRENT < TRAILING_PNL_LAST < NET_PNL_OVERALL:
        row['IS_TRAILING_ACTIVE'] = 'Y'
        row['TRAILING_PNL'] = TRAILING_PNL_LAST
    # SQUAREOFF IF NET_PNL_OVERALL IS LESS THAN LAST TRAILING PNL
    elif IS_TRAILING_ACTIVE == 'Y' and NET_PNL_OVERALL < TRAILING_PNL_LAST:
        row['IS_TRAILING_ACTIVE'] = 'Y'
        row['TRAILING_PNL'] = TRAILING_PNL_LAST

        # SET SQUAREOFF FIELDS
        row['is_square_off'] = True
        squareOff_dict['isSquareOff'] = True
        squareOff_dict['squareoff_type'] = 'SQUAREOFF_FINAL'
        row['is_partially_square_off'] = False
        row['is_active'] = 'N'
    else:
        row['IS_TRAILING_ACTIVE'] = row['IS_TRAILING_ACTIVE_POSITION']
        row['TRAILING_PNL'] = row['TRAILING_PNL_POSITION']

    df_positions_existing = pd.DataFrame([row])
    return df_positions_existing

def checkStopLossTargetPnl(Configuration, df_positions_existing, squareOff_dict, symbol):
    NET_PNL_OVERALL = df_positions_existing[df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.SELL]['NET_PNL_OVERALL'].iloc[0]

    ############################ KEEPING NET_PNL_OVERALL AS UNREALIZED_PNL_GROUP #######################################
    # WE ARE DOING THIS TO CATER PARTIAL SQUAREOFF SCENARIO
    # THIS CAN VARY FROM STRATEGY TO STRATEGY
    # THIS MIGHT CHANGE IF STRATEGY HAS FREQUENT ADJUSTMENTS( UNREALIZED_PNL_GROUP AS NET_PNL_OVERALL)
    #NET_PNL_OVERALL = df_positions_existing[df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.SELL]['UNREALIZED_PNL_GROUP'].iloc[0]
    ####################################################################################################################

    currentDay = parent_utils.getCurrentDay().upper()
    STOP_LOSS_PER_LOT = float(Configuration['STOP_LOSS_PER_LOT_'+currentDay+'_'+symbol])
    TARGET_PER_LOT = float(Configuration['TARGET_PER_LOT_'+currentDay+'_'+symbol])
    TIME_DECAY_TGT_PER_LOT = float(Configuration['TIME_DECAY_TGT_PER_LOT_'+currentDay+'_'+symbol])

    ####################################### MOCK DELTA FOR LOCAL TESTING ###############################################
    if parent_utils.isMockAndLocalEnv(Configuration):
        NET_PNL_OVERALL = BasicCache().get("NET_PNL_OVERALL")
    ####################################################################################################################

    # squareOff_dict = LotsCalculatorHelper.calculateNumLotsMarginAPI(Configuration, symbol,squareOff_dict,
    #                                                                         None, None, None)
    # NUM_LOTS = float(squareOff_dict["MF_STEP_SIZE"])

    # To cater partial squareoff, we are taking MF from positions table not calculating from TOTAL_INITIAL MARGIN
    NUM_LOTS = float(squareOff_dict["MF_EXISTING"])

    STOP_LOSS_AMOUNT_THRESHOLD = -1*NUM_LOTS*STOP_LOSS_PER_LOT
    TARGET_AMOUNT_THRESHOLD = NUM_LOTS * TARGET_PER_LOT
    TIME_DECAY_TGT_AMOUNT_THRESHOLD = NUM_LOTS * TIME_DECAY_TGT_PER_LOT
    #################################### SQUAREOFF RULES ###############################################################
    ############################################### RULE 1: STOPLOSS PER LOT HIT #######################################
    logging.info("########################## NET_PNL_OVERALL : {} ################################################".format(NET_PNL_OVERALL))
    if NET_PNL_OVERALL < STOP_LOSS_AMOUNT_THRESHOLD:
        logging.info("!!!!!!! SQUARED OFF ALL BECAUSE STOP LOSS HIT !!!!!. NET_PNL_OVERALL : {} , "
                     "STOP_LOSS_AMOUNT_THRESHOLD :  {}".format(str(NET_PNL_OVERALL), str(STOP_LOSS_AMOUNT_THRESHOLD)))
        df_positions_existing = df_positions_existing.apply(lambda row: isStopLossTargetTriggered(row, squareOff_dict), axis=1)
        return df_positions_existing

    ############################################### RULE 2: TARGET PER LOT HIT #########################################
    if NET_PNL_OVERALL > TARGET_AMOUNT_THRESHOLD:
        logging.info("!!!!!!! SQUARED OFF ALL BECAUSE TARGET HIT !!!!!. NET_PNL_OVERALL : {} , "
                     "TARGET_AMOUNT_THRESHOLD :  {}".format(str(NET_PNL_OVERALL), str(TARGET_AMOUNT_THRESHOLD)))
        df_positions_existing = df_positions_existing.apply(lambda row: isStopLossTargetTriggered(row, squareOff_dict), axis=1)
        return df_positions_existing

    ############################################### RULE 3: TIME DECAY TGT PER LOT HIT #################################
    params_dict = json.loads(squareOff_dict['params'])
    isCallAdjustmentSquareOffDone = params_dict['isCallAdjustmentSquareOffDone']
    isPutAdjustmentSquareOffDone = params_dict['isPutAdjustmentSquareOffDone']
    if not isPutAdjustmentSquareOffDone and not isCallAdjustmentSquareOffDone and NET_PNL_OVERALL > TIME_DECAY_TGT_AMOUNT_THRESHOLD:
        logging.info("!!!!!!! SQUARED OFF ALL BECAUSE TIME DECAY TGT PER LOT HIT !!!!!. NET_PNL_OVERALL : {} , "
                     "TIME_DECAY_TGT_AMOUNT_THRESHOLD :  {}".format(str(NET_PNL_OVERALL), str(TIME_DECAY_TGT_AMOUNT_THRESHOLD)))
        df_positions_existing = df_positions_existing.apply(lambda row: isStopLossTargetTriggered(row, squareOff_dict), axis=1)
        return df_positions_existing
    ####################################################################################################################

    if squareOff_dict['isSquareOff']:
        return df_positions_existing

    return df_positions_existing

def positionsCloseTime(row, squareOff_dict):

    # SET SQUAREOFF FIELDS
    setSquareOffFields(row, squareOff_dict)
    return row

def isStopLossTargetTriggered(row, squareOff_dict):

    # SET SQUAREOFF FIELDS
    setSquareOffFields(row, squareOff_dict)
    return row

def setSquareOffFields(row, squareOff_dict):
    row['is_square_off'] = True
    squareOff_dict['isSquareOff'] = True
    squareOff_dict['squareoff_type'] = 'SQUAREOFF_FINAL'
    row['is_partially_square_off'] = False
    row['IS_ACTIVE'] = False

def checkStopLossPortfolioTargetSLPctPnl(Configuration, df_positions_existing, squareOff_dict, symbol):

    ############################################### NET_PNL_OVERALL ####################################################
    NET_PNL_OVERALL = df_positions_existing[df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.SELL]['NET_PNL_OVERALL'].iloc[0]

    ####################################### MOCK DELTA FOR LOCAL TESTING ###############################################
    if parent_utils.isMockAndLocalEnv(Configuration):
        NET_PNL_OVERALL = BasicCache().get("NET_PNL_OVERALL_PORTFOLIO_PNL_TGT_SL_PCT")
    ####################################################################################################################

    ########################################### FETCH MARGIN USED #######################################################
    MARGIN_OVERALL = df_positions_existing[df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.SELL]['MARGIN_OVERALL'].iloc[0]
    NET_PNL_OVERALL_PCT = round(parent_utils.percentageIsXofY(NET_PNL_OVERALL, MARGIN_OVERALL), 2)
    TARGET_THRESHOLD_PCT = float(Configuration['TARGET_THRESHOLD_PCT_'+symbol])
    STOPLOSS_THRESHOLD_PCT = float(Configuration['STOPLOSS_THRESHOLD_PCT_' + symbol])

    #################################### EXECUTION THETA PNL RULES #####################################################
    df_positions_existing = df_positions_existing.apply(
        lambda row: isStopLossTargetPortfolioPnlSquareOff(row, NET_PNL_OVERALL_PCT, TARGET_THRESHOLD_PCT,
                                                 STOPLOSS_THRESHOLD_PCT, squareOff_dict, symbol), axis=1)

    return df_positions_existing

def isStopLossTargetPortfolioPnlSquareOff(row, NET_PNL_OVERALL_PCT, TARGET_THRESHOLD_PCT,
                                                 STOPLOSS_THRESHOLD_PCT, squareOff_dict, symbol):

    NET_PNL_OVERALL_PCT_ABS = abs(NET_PNL_OVERALL_PCT)

    if (NET_PNL_OVERALL_PCT < 0 and NET_PNL_OVERALL_PCT_ABS >= STOPLOSS_THRESHOLD_PCT) or \
        (NET_PNL_OVERALL_PCT > 0 and NET_PNL_OVERALL_PCT_ABS >= TARGET_THRESHOLD_PCT) :
        # SET SQUAREOFF FIELDS
        setSquareOffFields(row, squareOff_dict)

        logging.info("!!!!!!! SQUARED OFF, TARGET OR STOPLOSS Triggered !!!!!."
                     " NET_PNL_OVERALL_PCT :  {}, TARGET_THRESHOLD_PCT : {}, STOPLOSS_THRESHOLD_PCT : {}".format(
                str(NET_PNL_OVERALL_PCT), str(TARGET_THRESHOLD_PCT), str(STOPLOSS_THRESHOLD_PCT)))

    return row