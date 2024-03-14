
import pandas as pd
import json
from dao import PositionsDAO, OrdersDAO
from helper import OrderGenOptionsHelper, SquareOffOptionsHelper
from adaptor.OMAdaptor import OMAdaptor
from entities.Enums import AdjustmentTypeEnum, TransactionTypeEnum, InstrumentTypeEnum
from common.UniqueKeyGenerator import UniqueKeyGenerator
from common import utils
import logging
from config.cache.BasicCache import BasicCache
from datetime import datetime
import pytz

def populateAdjustmentType(optionsAdjustment_dict, squareOff_dict, Configuration, symbol):
    df_positions_tracking = squareOff_dict['df_positions_tracking']
    optionsAdjustment_dict[AdjustmentTypeEnum] =  None
    isCallAdjustmentSquareOffDone = optionsAdjustment_dict['isCallAdjustmentSquareOffDone']
    isPutAdjustmentSquareOffDone = optionsAdjustment_dict['isPutAdjustmentSquareOffDone']
    isFutBuyFreshPositionTaken = optionsAdjustment_dict['isFutBuyFreshPositionTaken']
    isFutSellFreshPositionTaken = optionsAdjustment_dict['isFutSellFreshPositionTaken']
    isFutBuyAdjustmentSquareOffDone = optionsAdjustment_dict['isFutBuyAdjustmentSquareOffDone']
    isFutSellAdjustmentSquareOffDone = optionsAdjustment_dict['isFutSellAdjustmentSquareOffDone']
    currentDay = utils.getCurrentDay().upper()
    CALL_PRICE_SL_PCT_THRESHOLD = float(Configuration['CALL_PRICE_SL_PCT_THRESHOLD_'+symbol])
    PUT_PRICE_SL_PCT_THRESHOLD = float(Configuration['PUT_PRICE_SL_PCT_THRESHOLD_'+symbol])
    CALL_PRICE_TGT_PCT_THRESHOLD = float(Configuration['CALL_PRICE_TGT_PCT_THRESHOLD_'+symbol])
    PUT_PRICE_TGT_PCT_THRESHOLD = float(Configuration['PUT_PRICE_TGT_PCT_THRESHOLD_'+symbol])
    FUT_PRICE_ENTRY_PCT_THRESHOLD = float(Configuration['FUT_PRICE_ENTRY_PCT_THRESHOLD_' + symbol])
    FUT_PRICE_SL_PCT_THRESHOLD = float(Configuration['FUT_PRICE_SL_PCT_THRESHOLD_' + symbol])
    IS_FUTURES_ACTIVE = Configuration['IS_FUTURES_ACTIVE']
    CURRENT_PRICE_CALL = 0.0
    SL_PRICE_CALL = 0.0
    TGT_PRICE_CALL = 0.0
    ENTRY_PRICE_CALL = 0.0
    CURRENT_PRICE_PUT = 0.0
    SL_PRICE_PUT = 0.0
    TGT_PRICE_PUT = 0.0
    ENTRY_PRICE_PUT = 0.0


    ###########################################  FETCH PRICES CALL #####################################################
    if not isCallAdjustmentSquareOffDone:
        CURRENT_PRICE_CALL = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL) &
                                    (df_positions_tracking['instrument_type'] == InstrumentTypeEnum.CALL)]['current_price'].iloc[0]
        ENTRY_PRICE_CALL = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL) &
                                    (df_positions_tracking['instrument_type'] == InstrumentTypeEnum.CALL)]['entry_price'].iloc[0]
        SL_PRICE_CALL = utils.xPercentageOfYIncrementedValue(CALL_PRICE_SL_PCT_THRESHOLD, ENTRY_PRICE_CALL)
        TGT_PRICE_CALL = utils.xPercentageOfYDecrementedValue(CALL_PRICE_TGT_PCT_THRESHOLD, ENTRY_PRICE_CALL)
        optionsAdjustment_dict['CURRENT_PRICE_CALL'] = CURRENT_PRICE_CALL

    ###########################################  FETCH PRICES PUT #####################################################
    if not isPutAdjustmentSquareOffDone:
        CURRENT_PRICE_PUT = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL) &
                                    (df_positions_tracking['instrument_type'] == InstrumentTypeEnum.PUT)]['current_price'].iloc[0]
        ENTRY_PRICE_PUT = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL) &
                                    (df_positions_tracking['instrument_type'] == InstrumentTypeEnum.PUT)]['entry_price'].iloc[0]
        SL_PRICE_PUT = utils.xPercentageOfYIncrementedValue(PUT_PRICE_SL_PCT_THRESHOLD, ENTRY_PRICE_PUT)
        TGT_PRICE_PUT = utils.xPercentageOfYDecrementedValue(PUT_PRICE_TGT_PCT_THRESHOLD, ENTRY_PRICE_PUT)
        optionsAdjustment_dict['CURRENT_PRICE_PUT'] = CURRENT_PRICE_PUT

    #########################################  FUTURES PRICES ##########################################################
    FUTURE_PRICE_INITIAL = squareOff_dict['future_price_initial']
    FUTURE_PRICE_CURRENT = squareOff_dict['future_price_current']

    # MOCK DATA BANKNIFTY
    # FUTURE_PRICE_CURRENT = 36300.0    # FUT SELL or FUT BUY SQUAREOFF
    # FUTURE_PRICE_CURRENT = 36600.0     # FUT BUY or FUT SELL SQUAREOFF

    # MOCK DATA NIFTY
    # FUTURE_PRICE_CURRENT = 17000.0    # FUT SELL or FUT BUY SQUAREOFF
    # FUTURE_PRICE_CURRENT = 18000.0     # FUT BUY or FUT SELL SQUAREOFF
    # MOCK RESULTS NIFTY-> -22.5,-122.5

    FUTURE_PRICE_INCREMENTED_VALUE = utils.xPercentageOfYIncrementedValue(FUT_PRICE_ENTRY_PCT_THRESHOLD,
                                                                          FUTURE_PRICE_INITIAL)
    FUTURE_PRICE_DECREMENTED_VALUE = utils.xPercentageOfYDecrementedValue(FUT_PRICE_ENTRY_PCT_THRESHOLD,
                                                                          FUTURE_PRICE_INITIAL)

    ####################################### MOCK DELTA FOR LOCAL TESTING ###############################################
    if utils.isMockEnv(Configuration):
        CURRENT_PRICE_PUT = BasicCache().get("CURRENT_PRICE_PUT")
        CURRENT_PRICE_CALL = BasicCache().get("CURRENT_PRICE_CALL")
        isCallAdjustmentSquareOffDone = BasicCache().get("isCallAdjustmentSquareOffDone")
        isPutAdjustmentSquareOffDone = BasicCache().get("isPutAdjustmentSquareOffDone")
    ####################################################################################################################

    ######################################   ADJUSTMENT RULES ###########################################################
    # RULE NO 1: FOR BANKNIFTY: CALL ADJUSTMENT : SL -> 135% of ENTRY PRICE
    # RULE NO 2: FOR BANKNIFTY: PUT ADJUSTMENT : SL -> 135% of ENTRY PRICE
    # RULE NO 3: FOR BANKNIFTY: FUTURE FRESH SELL -> INITIAL FUTURE PRICE DOWN BY 0.3%
    # RULE NO 4: FOR BANKNIFTY: FUTURE FRESH BUY -> INITIAL FUTURE PRICE UP BY 0.3%
    # RULE NO 5: FOR BANKNIFTY: FUTURE SELL SL -> 100.25% of ENTRY PRICE
    # RULE NO 6: FOR BANKNIFTY: FUTURE BUY SL -> 99.75% of ENTRY PRICE
    ####################################################################################################################
    if not isCallAdjustmentSquareOffDone and (CURRENT_PRICE_CALL >= SL_PRICE_CALL or CURRENT_PRICE_CALL <= TGT_PRICE_CALL):
        optionsAdjustment_dict[AdjustmentTypeEnum] = AdjustmentTypeEnum.CALL_ADJUSTMENT_SQUAREOFF
        optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = True
        optionsAdjustment_dict["isCallAdjustmentSquareOffDone"] = True
        logging.info("{}: CALL ADJUSTMENT SQUAREOFF INITIATED : CURRENT_PRICE_CALL :: {}, ENTRY_PRICE_CALL : {}".format(Configuration['SCHEMA_NAME'],
                                                                                                                        CURRENT_PRICE_CALL, ENTRY_PRICE_CALL))
    elif not isPutAdjustmentSquareOffDone and (CURRENT_PRICE_PUT >= SL_PRICE_PUT or CURRENT_PRICE_PUT <= TGT_PRICE_PUT):
        optionsAdjustment_dict[AdjustmentTypeEnum] = AdjustmentTypeEnum.PUT_ADJUSTMENT_SQUAREOFF
        optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = True
        optionsAdjustment_dict["isPutAdjustmentSquareOffDone"] = True
        logging.info("{}: PUT ADJUSTMENT SQUAREOFF INITIATED : CURRENT_PRICE_PUT :: {}, ENTRY_PRICE_PUT : {}".format(Configuration['SCHEMA_NAME'],
                                                                                                                     CURRENT_PRICE_PUT, ENTRY_PRICE_PUT))

    elif not isFutSellFreshPositionTaken and \
            not (isFutSellFreshPositionTaken and isFutSellAdjustmentSquareOffDone) and \
            not (isFutBuyFreshPositionTaken and not isFutBuyAdjustmentSquareOffDone) and \
            FUTURE_PRICE_CURRENT < FUTURE_PRICE_DECREMENTED_VALUE and \
            IS_FUTURES_ACTIVE == 'Y':
        optionsAdjustment_dict[AdjustmentTypeEnum] = AdjustmentTypeEnum.FUT_SELL_FRESH_POSITION
        optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = True
        optionsAdjustment_dict["isFutSellFreshPositionTaken"] = True
        logging.info("{}: isFutSellFreshPositionTaken INITIATED : FUTURE_PRICE_INITIAL :: {}, "
                     "FUTURE_PRICE_CURRENT : {},FUTURE_PRICE_DECREMENTED_VALUE :: {}".format(Configuration['SCHEMA_NAME'],
                     FUTURE_PRICE_INITIAL, FUTURE_PRICE_CURRENT, FUTURE_PRICE_DECREMENTED_VALUE))

    elif not isFutBuyFreshPositionTaken and \
            not (isFutBuyFreshPositionTaken and isFutBuyAdjustmentSquareOffDone) and \
            not (isFutSellFreshPositionTaken and not isFutSellAdjustmentSquareOffDone) and \
            FUTURE_PRICE_CURRENT > FUTURE_PRICE_INCREMENTED_VALUE and \
            IS_FUTURES_ACTIVE == 'Y':
        optionsAdjustment_dict[AdjustmentTypeEnum] = AdjustmentTypeEnum.FUT_BUY_FRESH_POSITION
        optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = True
        optionsAdjustment_dict["isFutBuyFreshPositionTaken"] = True
        logging.info("{}: isFutBuyFreshPositionTaken INITIATED : FUTURE_PRICE_INITIAL :: {}, "
                     "FUTURE_PRICE_CURRENT : {},FUTURE_PRICE_INCREMENTED_VALUE :: {}".format(Configuration['SCHEMA_NAME'],
                     FUTURE_PRICE_INITIAL, FUTURE_PRICE_CURRENT, FUTURE_PRICE_INCREMENTED_VALUE))

    ######################################### SQUAREOFF FUTURES POSITIONS - FUTURES SELL ###############################
    elif (isFutSellFreshPositionTaken and not isFutSellAdjustmentSquareOffDone):
        ENTRY_PRICE_FUTURES = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL) &
                                    (df_positions_tracking['instrument_type'] == InstrumentTypeEnum.FUTURES)]['entry_price'].iloc[0]

        # MOCK DATA
        #FUTURE_PRICE_CURRENT = 36600.0

        FUTURE_PRICE_SELL_STOPLOSS = utils.xPercentageOfYIncrementedValue(FUT_PRICE_SL_PCT_THRESHOLD, ENTRY_PRICE_FUTURES)

        if FUTURE_PRICE_CURRENT > FUTURE_PRICE_SELL_STOPLOSS:
            optionsAdjustment_dict[AdjustmentTypeEnum] = AdjustmentTypeEnum.FUT_SELL_ADJUSTMENT_SQUAREOFF
            optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = True
            optionsAdjustment_dict["isFutSellAdjustmentSquareOffDone"] = True
            logging.info("{}: isFutSellAdjustmentSquareOffDone INITIATED : ENTRY_PRICE_FUTURES :: {}, "
                         "FUTURE_PRICE_CURRENT : {},FUTURE_PRICE_SELL_STOPLOSS :: {}".format(Configuration['SCHEMA_NAME'],
                         ENTRY_PRICE_FUTURES, FUTURE_PRICE_CURRENT, FUTURE_PRICE_SELL_STOPLOSS))

    ######################################### SQUAREOFF FUTURES POSITIONS - FUTURES BUY ################################
    elif (isFutBuyFreshPositionTaken and not isFutBuyAdjustmentSquareOffDone):
        ENTRY_PRICE_FUTURES = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.BUY) &
                                    (df_positions_tracking['instrument_type'] == InstrumentTypeEnum.FUTURES)]['entry_price'].iloc[0]

        # MOCK DATA
        #FUTURE_PRICE_CURRENT = 36300.0

        FUTURE_PRICE_BUY_STOPLOSS = utils.xPercentageOfYDecrementedValue(FUT_PRICE_SL_PCT_THRESHOLD, ENTRY_PRICE_FUTURES)

        if FUTURE_PRICE_CURRENT < FUTURE_PRICE_BUY_STOPLOSS:
            optionsAdjustment_dict[AdjustmentTypeEnum] = AdjustmentTypeEnum.FUT_BUY_ADJUSTMENT_SQUAREOFF
            optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = True
            optionsAdjustment_dict["isFutBuyAdjustmentSquareOffDone"] = True
            logging.info("{}: isFutBuyAdjustmentSquareOffDone INITIATED : ENTRY_PRICE_FUTURES :: {}, "
                         "FUTURE_PRICE_CURRENT : {},FUTURE_PRICE_BUY_STOPLOSS :: {}".format(Configuration['SCHEMA_NAME'],
                         ENTRY_PRICE_FUTURES, FUTURE_PRICE_CURRENT, FUTURE_PRICE_BUY_STOPLOSS))


    # MOCK TESTING FOR ADJUSTMENT
    # optionsAdjustment_dict[AdjustmentTypeEnum] = AdjustmentTypeEnum.CALL_ADJUSTMENT_SQUAREOFF
    # optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = True
    # optionsAdjustment_dict["isCallAdjustmentSquareOffDone"] = True
    # logging.info("CALL ADJUSTMENT SQUAREOFF INITIATED : CURRENT_PRICE_CALL :: {}, ENTRY_PRICE_CALL : {}".format(
    #     CURRENT_PRICE_CALL, ENTRY_PRICE_CALL))

    return optionsAdjustment_dict

def populateGreeks(df_Put_Call, NUM_LOTS, squareOff_dict, symbol, Configuration):
    ######################################  WAP BID ASK PRICES WITH QUANTITY #########################################
    df_Put_Call = OrderGenOptionsHelper.populateWAPBidAskPrices(df_Put_Call, NUM_LOTS, symbol, Configuration)
    expiry_date = squareOff_dict['expiry_date']
    spot_value = squareOff_dict['spot_value']

    ########################################### TIME TO EXPIRY CALCULATIONS ###########################################
    time_to_expiry_options_252, time_to_expiry_options_annualized_252, \
    time_to_expiry_options_365, time_to_expiry_options_annualized_365 = OrderGenOptionsHelper.populateTimeToExpiry(
        expiry_date, Configuration)
    df_Put_Call['DAYS_IN_YEAR'] = '252'

    ########################################  SET EXECUTION PRICES FROM BID ASK POST DECISION ############################
    df_Put_Call['ORDER_TYPE_CALL'] = 'NA'
    df_Put_Call['ORDER_TYPE_PUT'] = 'NA'
    df_Put_Call = populateExecutionPricesWAPBeforeAdjustment(df_Put_Call)  # FOR OPTIONS POPULATE EXECUTION PRICES

    ######################################## GREEKS WITH ASSUMPTION FOR SELL #############################################
    df_Put_Call['ORDER_TYPE_CALL'] = 'SELL'
    df_Put_Call['ORDER_TYPE_PUT'] = 'SELL'
    df_Put_Call = populateIVAndGreeks(df_Put_Call, False, time_to_expiry_options_252,
                                                            time_to_expiry_options_365, spot_value, Configuration,
                                                            squareOff_dict)
    df_Put_Call['DELTA_PUT_SELL'] = df_Put_Call['DELTA_PUT']
    df_Put_Call['DELTA_CALL_SELL'] = df_Put_Call['DELTA_CALL']

    ######################################## GREEKS WITH ASSUMPTION FOR BUY #############################################
    df_Put_Call['ORDER_TYPE_CALL'] = 'BUY'
    df_Put_Call['ORDER_TYPE_PUT'] = 'BUY'
    df_Put_Call = populateIVAndGreeks(df_Put_Call, False, time_to_expiry_options_252,
                                                            time_to_expiry_options_365, spot_value, Configuration,
                                                            squareOff_dict)
    df_Put_Call['DELTA_PUT_BUY'] = df_Put_Call['DELTA_PUT']
    df_Put_Call['DELTA_CALL_BUY'] = df_Put_Call['DELTA_CALL']

    return df_Put_Call

def populateExecutionPricesWAPBeforeAdjustment(df_Put_Call):
    for index, row in df_Put_Call.iterrows():

        # SET CALL PRICES
        if row['ORDER_TYPE_CALL'] == 'SELL':
            df_Put_Call.at[index, 'CURRENT_PRICE_CALL'] = row['BID_PRICE_CALL']
        elif row['ORDER_TYPE_CALL'] == 'BUY':
            df_Put_Call.at[index, 'CURRENT_PRICE_CALL'] = row['ASK_PRICE_CALL']
        else:
            df_Put_Call.at[index, 'CURRENT_PRICE_CALL'] = row['LAST_PRICE_CALL']

        # SET PUT PRICES
        if row['ORDER_TYPE_PUT'] == 'SELL':
            df_Put_Call.at[index, 'CURRENT_PRICE_PUT'] = row['BID_PRICE_PUT']
        elif row['ORDER_TYPE_PUT'] == 'BUY':
            df_Put_Call.at[index, 'CURRENT_PRICE_PUT'] = row['ASK_PRICE_PUT']
        else:
            df_Put_Call.at[index, 'CURRENT_PRICE_PUT'] = row['LAST_PRICE_PUT']

    return df_Put_Call

def populateIVAndGreeks(df_Put_Call, isSquareOffJob, time_to_expiry_options_252, time_to_expiry_options_365,
                        spot_value, Configuration,squareOff_dict):
    #MF_STEP_SIZE = orderGen_dict['MF_STEP_SIZE']
    DAYS_IN_YEAR = int(df_Put_Call['DAYS_IN_YEAR'].iloc[0])
    if DAYS_IN_YEAR == 252:
        time_to_expiry_options_annualized = time_to_expiry_options_252/DAYS_IN_YEAR
    else:
        time_to_expiry_options_annualized = time_to_expiry_options_365 / DAYS_IN_YEAR


    df_Put_Call['IV_PUT'] = df_Put_Call.apply(lambda row: OrderGenOptionsHelper.populateIV('PUT', row['STRIKE_PRICE'], spot_value,
                                                                     time_to_expiry_options_annualized,
                                                                     row['CURRENT_PRICE_PUT'],
                                                                     Configuration)[0], axis=1)

    df_Put_Call['IV_CALL'] = df_Put_Call.apply(lambda row: OrderGenOptionsHelper.populateIV('CALL', row['STRIKE_PRICE'], spot_value,
                                                                      time_to_expiry_options_annualized,
                                                                      row['CURRENT_PRICE_CALL'],
                                                                      Configuration)[1], axis=1)
    df_Put_Call['IV_PUT_OPT'] = df_Put_Call.apply(lambda row: OrderGenOptionsHelper.populateIV('PUT', row['STRIKE_PRICE'], spot_value,
                                                                         time_to_expiry_options_annualized,
                                                                         row['CURRENT_PRICE_PUT'],
                                                                         Configuration)[2], axis=1)

    df_Put_Call['IV_CALL_OPT'] = df_Put_Call.apply(lambda row: OrderGenOptionsHelper.populateIV('CALL', row['STRIKE_PRICE'], spot_value,
                                                                          time_to_expiry_options_annualized,
                                                                          row['CURRENT_PRICE_CALL'],
                                                                          Configuration)[3], axis=1)
    # df_Put_Call = df_Put_Call.drop(['ORDER_TYPE_PUT', 'ORDER_TYPE_CALL'], axis=1)
    # IV DIFF PCT AND ORDER TYPES
    df_Put_Call = df_Put_Call.apply(lambda row: OrderGenOptionsHelper.calculateIVDiffPct(row, isSquareOffJob, Configuration), axis=1)
    # Add Delta Put
    df_Put_Call['DELTA_PUT'] = df_Put_Call.apply(lambda row: OrderGenOptionsHelper.populateDelta('PUT', row['STRIKE_PRICE'], spot_value,
                                                                           time_to_expiry_options_annualized,
                                                                           row['IV_PUT'], row['ORDER_TYPE_PUT'],float(row['MULTI_FACTOR']),
                                                                           Configuration,)[0], axis=1)
    df_Put_Call['DELTA_CALL'] = df_Put_Call.apply(lambda row: OrderGenOptionsHelper.populateDelta('CALL', row['STRIKE_PRICE'], spot_value,
                                                                            time_to_expiry_options_annualized,
                                                                            row['IV_CALL'], row['ORDER_TYPE_CALL'],float(row['MULTI_FACTOR']),
                                                                            Configuration)[1], axis=1)
    # df_Put_Call['NET_DELTA'] = df_Put_Call['DELTA_PUT'] + df_Put_Call['DELTA_CALL']
    #
    # # GAMMA VALUES POPULATION
    # df_Put_Call['GAMMA_PUT'] = df_Put_Call.apply(
    #     lambda row: populateGamma(row['ORDER_TYPE_PUT'], row['STRIKE_PRICE'], spot_value,
    #                               time_to_expiry_options_annualized, row['IV_PUT'],float(row['MULTI_FACTOR']),
    #                               Configuration), axis=1)
    # df_Put_Call['GAMMA_CALL'] = df_Put_Call.apply(
    #     lambda row: populateGamma(row['ORDER_TYPE_CALL'], row['STRIKE_PRICE'], spot_value,
    #                               time_to_expiry_options_annualized, row['IV_CALL'], float(row['MULTI_FACTOR']),
    #                               Configuration), axis=1)
    # df_Put_Call['NET_GAMMA'] = df_Put_Call['GAMMA_PUT'] + df_Put_Call['GAMMA_CALL']
    #
    # ############################################# VEGA/THETA CALCULATIONS ###############################################
    # df_Put_Call['THETA_PUT'] = df_Put_Call.apply(lambda row: populateTheta('PUT', row['STRIKE_PRICE'], spot_value,
    #                                                                        time_to_expiry_options_annualized,
    #                                                                        row['IV_PUT'], row['ORDER_TYPE_PUT'],float(row['MULTI_FACTOR']),
    #                                                                        Configuration )[0], axis=1)
    # df_Put_Call['THETA_CALL'] = df_Put_Call.apply(lambda row: populateTheta('CALL', row['STRIKE_PRICE'], spot_value,
    #                                                                         time_to_expiry_options_annualized,
    #                                                                         row['IV_CALL'], row['ORDER_TYPE_CALL'],float(row['MULTI_FACTOR']),
    #                                                                         Configuration)[1], axis=1)
    # df_Put_Call = df_Put_Call.apply(lambda row: populateIndividualTheta(row),axis=1)
    # df_Put_Call['NET_THETA'] = df_Put_Call['THETA_INDIVIDUAL'].sum()
    # df_Put_Call['NET_THETA_ONE_MF'] = df_Put_Call['NET_THETA'] / df_Put_Call['MULTI_FACTOR']
    # df_Put_Call = df_Put_Call.apply(lambda row: populateExecutionThetaPnlPending(row, Configuration, orderGen_dict), axis=1)
    #
    # df_Put_Call['VEGA_PUT'] = df_Put_Call.apply(lambda row: populateVega('PUT', row['STRIKE_PRICE'], spot_value,
    #                                                                        time_to_expiry_options_annualized,
    #                                                                        row['IV_PUT'], row['ORDER_TYPE_PUT'],
    #                                                                      float(row['MULTI_FACTOR']),
    #                                                                        Configuration), axis=1)
    # df_Put_Call['VEGA_CALL'] = df_Put_Call.apply(lambda row: populateVega('CALL', row['STRIKE_PRICE'], spot_value,
    #                                                                         time_to_expiry_options_annualized,
    #                                                                         row['IV_CALL'], row['ORDER_TYPE_CALL'],
    #                                                                       float(row['MULTI_FACTOR']),
    #                                                                         Configuration), axis=1)
    # df_Put_Call = df_Put_Call.apply(lambda row: populateIndividualVega(row), axis=1)
    # df_Put_Call['NET_VEGA'] = df_Put_Call['VEGA_INDIVIDUAL'].sum()
    #####################################################################################################################

    return df_Put_Call

def populateExecutionPricesWAP(row):
    if row['ORDER_TYPE_HEDGED'] == 'SELL':
        row['EXECUTION_PRICE_' + row['OPTION_TYPE_HEDGED']] = row['BID_PRICE_' + row['OPTION_TYPE_HEDGED']]
        row['EXECUTION_PRICE_' + row['OPTION_TYPE_HEDGED'] + '_ACTUAL'] = row['BID_PRICE_' + row['OPTION_TYPE_HEDGED']]
        row['CURRENT_PRICE_' + row['OPTION_TYPE_HEDGED']] = row['ASK_PRICE_' + row['OPTION_TYPE_HEDGED']]
        row['CURRENT_PRICE_' + row['OPTION_TYPE_HEDGED'] + '_ACTUAL'] = row['ASK_PRICE_' + row['OPTION_TYPE_HEDGED']]
    else:
        row['EXECUTION_PRICE_'+row['OPTION_TYPE_HEDGED']] = row['ASK_PRICE_'+row['OPTION_TYPE_HEDGED']]
        row['EXECUTION_PRICE_'+row['OPTION_TYPE_HEDGED']+'_ACTUAL'] = row['ASK_PRICE_'+row['OPTION_TYPE_HEDGED']]
        row['CURRENT_PRICE_'+row['OPTION_TYPE_HEDGED']] = row['BID_PRICE_'+row['OPTION_TYPE_HEDGED']]
        row['CURRENT_PRICE_'+row['OPTION_TYPE_HEDGED'] + '_ACTUAL'] = row['BID_PRICE_'+row['OPTION_TYPE_HEDGED']]

    return row

def populateFieldsAfterAdjustment(df_Put_Call, optionsAdjustment_dict, Configuration):
    expiry_date = optionsAdjustment_dict['expiry_date']
    # UPDATE ENTRY PRICE/EXIT PRICE/DELTA
    for index, row in df_Put_Call.iterrows():

        # POPULATE DELTA
        df_Put_Call.at[index, 'DELTA'] = row['DELTA_' + row['INSTRUMENT_TYPE'] + '_' + row['TRANSACTION_TYPE']]

        df_Put_Call.at[index, 'IS_SQUARE_OFF'] = False

        # ADD PRICES
        if row['TRANSACTION_TYPE'] == 'BUY':
            df_Put_Call.at[index, 'ENTRY_PRICE_' + row['INSTRUMENT_TYPE']] = row['ASK_PRICE_' + row['INSTRUMENT_TYPE']]
            df_Put_Call.at[index, 'CURRENT_PRICE'] = row['ASK_PRICE_' + row['INSTRUMENT_TYPE']]

        if row['TRANSACTION_TYPE'] == 'SELL':
            df_Put_Call.at[index, 'ENTRY_PRICE_' + row['INSTRUMENT_TYPE']] = row['BID_PRICE_' + row['INSTRUMENT_TYPE']]
            df_Put_Call.at[index, 'CURRENT_PRICE'] = row['BID_PRICE_' + row['INSTRUMENT_TYPE']]

        # EXPIRY DATE/ MONEYNESS/ SYMBOL
        order_manifest = str(row['STRIKE_PRICE'])+'_'+row['INSTRUMENT_TYPE']+'_'+row['TRANSACTION_TYPE']+'_'+row['INSTRUCTION_TYPE']
        #######################################  ORDER MANIFEST - ADD TRADE DATE ###########################################
        if utils.isBackTestingEnv(Configuration):
            trade_date = ""
            pass
        else:
            trade_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%b%d').upper()
        order_manifest = trade_date + '_' + order_manifest
        ####################################################################################################################
        df_Put_Call.at[index, 'EXPIRY_DATE'] = expiry_date
        df_Put_Call.at[index, 'MONEYNESS'] = row['LEVEL_' + row['INSTRUMENT_TYPE']]
        df_Put_Call.at[index, 'SYMBOL'] = optionsAdjustment_dict['symbol']
        df_Put_Call.at[index, 'CONTRACT_TYPE'] = optionsAdjustment_dict['contract_type']
        df_Put_Call.at[index, 'ORDER_MANIFEST'] = order_manifest
        df_Put_Call.at[index, 'LOT_SIZE'] = Configuration['LOT_SIZE_'+optionsAdjustment_dict['symbol']]
        df_Put_Call.at[index, 'NUM_LOTS'] = int(row['MULTI_FACTOR'])
        df_Put_Call.at[index, 'UNREALIZED_PNL'] = 0.0
        df_Put_Call.at[index, 'UNREALIZED_PNL_GROUP'] = 0.0
        df_Put_Call.at[index, 'REALIZED_PNL'] = 0.0
        df_Put_Call.at[index, 'REALIZED_PNL_GROUP'] = 0.0
        df_Put_Call.at[index, 'NET_PNL_OVERALL'] = 0.0
        df_Put_Call.at[index, 'ORDER_TYPE'] = 'MARKET'
        df_Put_Call.at[index, 'MARGIN_OVERALL'] = float(Configuration['TOTAL_INITIAL_MARGIN'])
        optionsAdjustment_dict = populateParams(optionsAdjustment_dict, row , order_manifest)
        df_Put_Call.at[index, 'PARAMS'] = optionsAdjustment_dict['params']


        df_Put_Call.at[index, 'REALIZED_PNL'] = None
        df_Put_Call.at[index, 'REALIZED_PNL_GROUP'] = None
        df_Put_Call.at[index, 'EXIT_PRICE'] = None

    return df_Put_Call

def populateFieldsAfterAdjustmentSquareOff(df_Put_Call, optionsAdjustment_dict, Configuration):
    # UPDATE ENTRY PRICE/EXIT PRICE/DELTA
    for index, row in df_Put_Call.iterrows():

        # ADD PRICES
        # ITS SQUAREOFF SO ADDING BID IN CASE OF BUY
        if row['TRANSACTION_TYPE'].value == 'BUY':
            df_Put_Call.at[index, 'EXIT_PRICE'] = row['BID_PRICE_' + row['INSTRUMENT_TYPE'].value]
            df_Put_Call.at[index, 'CURRENT_PRICE'] = row['BID_PRICE_' + row['INSTRUMENT_TYPE'].value]

        if row['TRANSACTION_TYPE'].value == 'SELL':
            df_Put_Call.at[index, 'EXIT_PRICE'] = row['ASK_PRICE_' + row['INSTRUMENT_TYPE'].value]
            df_Put_Call.at[index, 'CURRENT_PRICE'] = row['ASK_PRICE_' + row['INSTRUMENT_TYPE'].value]

        # EXPIRY DATE/ MONEYNESS/ SYMBOL
        order_manifest = str(int(row['STRIKE_PRICE']))+'_'+row['INSTRUMENT_TYPE'].value+'_'+row['TRANSACTION_TYPE'].value+'_'+row['INSTRUCTION_TYPE']
        #######################################  ORDER MANIFEST - ADD TRADE DATE ###########################################
        if utils.isBackTestingEnv(Configuration):
            trade_date = ""
            pass
        else:
            trade_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%b%d').upper()
        order_manifest = trade_date + '_' + order_manifest
        ####################################################################################################################
        df_Put_Call.at[index, 'ORDER_MANIFEST'] = order_manifest
        optionsAdjustment_dict = populateParamsSquareOff(optionsAdjustment_dict, row , order_manifest)
        df_Put_Call.at[index, 'PARAMS'] = optionsAdjustment_dict['params']

        df_Put_Call.at[index, 'REALIZED_PNL'] = None
        df_Put_Call.at[index, 'REALIZED_PNL_GROUP'] = None
        df_Put_Call.at[index, 'EXIT_PRICE'] = None

    return df_Put_Call

def populateFieldsAfterAdjustmentFuturesFresh(df_Put_Call, optionsAdjustment_dict, Configuration):
    # UPDATE ENTRY PRICE/EXIT PRICE/DELTA
    for index, row in df_Put_Call.iterrows():

        # EXPIRY DATE/ MONEYNESS/ SYMBOL
        order_manifest = str(int(row['STRIKE_PRICE']))+'_'+row['INSTRUMENT_TYPE'].value+'_'+row['TRANSACTION_TYPE'].value+'_'+row['INSTRUCTION_TYPE']
        #######################################  ORDER MANIFEST - ADD TRADE DATE ###########################################
        if utils.isBackTestingEnv(Configuration):
            trade_date = ""
            pass
        else:
            trade_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%b%d').upper()
        order_manifest = trade_date + '_' + order_manifest
        ####################################################################################################################
        df_Put_Call.at[index, 'ORDER_MANIFEST'] = order_manifest
        optionsAdjustment_dict = populateParamsSquareOff(optionsAdjustment_dict, row , order_manifest)
        df_Put_Call.at[index, 'PARAMS'] = optionsAdjustment_dict['params']

        #REALIZED PNL
        if row['REALIZED_PNL'] is not None and row['REALIZED_PNL'] is not 0:
            pass
        else:
            df_Put_Call.at[index, 'REALIZED_PNL'] = None

        # REALIZED PNL GROUP
        if row['REALIZED_PNL_GROUP'] is not None and row['REALIZED_PNL_GROUP'] is not 0:
            pass
        else:
            df_Put_Call.at[index, 'REALIZED_PNL_GROUP'] = None
        df_Put_Call.at[index, 'MONEYNESS'] = row['INSTRUMENT_TYPE'].value

    return df_Put_Call

def populateParams(optionsAdjustment_dict, row , order_manifest):
    params_dict = json.loads(optionsAdjustment_dict['params'])
    #TRACKING PARAMS
    params_dict['isAllAdjustmentsDone'] = optionsAdjustment_dict['isAllAdjustmentsDone']
    params_dict['isCallAdjustmentDone'] = optionsAdjustment_dict['isCallAdjustmentDone']
    params_dict['isPutAdjustmentDone'] = optionsAdjustment_dict['isPutAdjustmentDone']

    # ENTRY PARAMS
    params_dict['entry_delta_' + row['INSTRUMENT_TYPE'].lower()] = row[
        'DELTA_' + row['INSTRUMENT_TYPE'] + '_' + row['TRANSACTION_TYPE']]
    params_dict['entry_net_delta'] = 0.0
    params_dict['entry_net_delta_overall'] = 0.0
    params_dict['entry_iv_' + row['INSTRUMENT_TYPE'].lower()] = row['IV_' + row['INSTRUMENT_TYPE']]
    params_dict['entry_iv_diff_pct'] = row['IV_DIFF_PCT']
    params_dict['entry_underlying'] = optionsAdjustment_dict["spot_value"]
    params_dict['entry_time_to_expiry_options'] = optionsAdjustment_dict["time_to_expiry_options"]
    params_dict['entry_gamma_' + row['INSTRUMENT_TYPE'].lower()] = 0.0
    params_dict['entry_net_gamma'] = 0.0
    params_dict['entry_theta_' + row['INSTRUMENT_TYPE'].lower()] = 0.0
    params_dict['entry_net_theta'] = 0.0
    params_dict['entry_vega_' + row['INSTRUMENT_TYPE'].lower()] = 0.0
    params_dict['entry_net_vega'] = 0.0
    params_dict['order_manifest'] = order_manifest
    params_dict['entry_atm_put_price'] = 0.0
    params_dict['entry_atm_call_price'] = 0.0
    params_dict['entry_atm_avg_price'] = 0.0

    optionsAdjustment_dict['params'] = json.dumps(params_dict)
    return optionsAdjustment_dict

def populateParamsSquareOff(optionsAdjustment_dict, row , order_manifest):
    params_dict = json.loads(optionsAdjustment_dict['params'])
    #TRACKING PARAMS
    params_dict['isCallAdjustmentSquareOffDone'] = optionsAdjustment_dict['isCallAdjustmentSquareOffDone']
    params_dict['isPutAdjustmentSquareOffDone'] = optionsAdjustment_dict['isPutAdjustmentSquareOffDone']
    params_dict['isFutBuyFreshPositionTaken'] = optionsAdjustment_dict['isFutBuyFreshPositionTaken']
    params_dict['isFutSellFreshPositionTaken'] = optionsAdjustment_dict['isFutSellFreshPositionTaken']
    params_dict['isFutBuyAdjustmentSquareOffDone'] = optionsAdjustment_dict['isFutBuyAdjustmentSquareOffDone']
    params_dict['isFutSellAdjustmentSquareOffDone'] = optionsAdjustment_dict['isFutSellAdjustmentSquareOffDone']
    optionsAdjustment_dict['params'] = json.dumps(params_dict)
    return optionsAdjustment_dict

def reverseTransactionType(df_Put_Call):
    for index, row in df_Put_Call.iterrows():
        if row['INSTRUCTION_TYPE'] == 'SQUAREOFF_ALL' and row['TRANSACTION_TYPE'] == 'BUY':
            df_Put_Call.at[index, 'TRANSACTION_TYPE'] = 'SELL'

        if row['INSTRUCTION_TYPE'] == 'SQUAREOFF_ALL' and row['TRANSACTION_TYPE'] == 'SELL':
            df_Put_Call.at[index, 'TRANSACTION_TYPE'] = 'BUY'

    return df_Put_Call


def populateAdditionalFields(row, optionsHedging_dict, LOT_SIZE):
    row['MARGIN_TXN'] = 0.0
    row['EXPIRY_DATE'] = optionsHedging_dict['expiry_date']

    row['LOT_SIZE'] = LOT_SIZE
    row['QUANTITY_'+row['OPTION_TYPE_HEDGED']] = row['MULTI_FACTOR_HEDGED'] * float(LOT_SIZE)
    row['BID_PRICE_'+row['OPTION_TYPE_HEDGED']] = OrderGenOptionsHelper.calculateBidAskPriceByMF(row, row['OPTION_TYPE_HEDGED'])[0]
    row['ASK_PRICE_'+row['OPTION_TYPE_HEDGED']] = OrderGenOptionsHelper.calculateBidAskPriceByMF(row, row['OPTION_TYPE_HEDGED'])[1]

    return row

def populateSellDF(SP_SELL, df_Level):
    df_Put_Call = df_Level[df_Level['STRIKE_PRICE'] == float(SP_SELL)]
    df_Put_Call = df_Put_Call.append([df_Put_Call] * 1, ignore_index=True)
    df_Put_Call.reset_index(drop=True, inplace=True)

    # SET ORDER TYPE AND OPTION TYPE
    df_Put_Call['ORDER_TYPE'] = 'SELL'
    df_Put_Call = df_Put_Call.apply(lambda row: setOptionTypeSell(row), axis=1)

    return df_Put_Call


def populateBuyDF(SP_BUY, df_Level, OPTION_TYPE_BUY):
    df_Put_Call = df_Level[df_Level['STRIKE_PRICE'] == float(SP_BUY)]
    df_Put_Call.reset_index(drop=True, inplace=True)

    # SET ORDER TYPE AND OPTION TYPE
    df_Put_Call['ORDER_TYPE'] = 'BUY'
    df_Put_Call = df_Put_Call.apply(lambda row: setOptionTypeBuy(row, OPTION_TYPE_BUY), axis=1)

    return df_Put_Call

def setOptionTypeSell(row):
    row['INSTRUMENT_TYPE'] = None
    row['ORDER_TYPE_PUT'] = 'SELL'
    row['ORDER_TYPE_CALL'] = 'SELL'

    # SET OPTION TYPE BASED ON INDEX, 0th index Set Call
    if row.name == 0:
        row['INSTRUMENT_TYPE'] = 'CALL'
        row['ORDER_TYPE_CALL'] = 'SELL'

    if row.name == 1: # 1st index set PUT
        row['INSTRUMENT_TYPE'] = 'PUT'
        row['ORDER_TYPE_PUT'] = 'SELL'

    # ORDER MANIFEST
    row["ORDER_MANIFEST"] = str(row['STRIKE_PRICE'])+'_'+row['INSTRUMENT_TYPE']+'_'+row['ORDER_TYPE_'+row['INSTRUMENT_TYPE']]

    return row

def setOptionTypeBuy(row, OPTION_TYPE_BUY):
    row['INSTRUMENT_TYPE'] = None
    row['ORDER_TYPE_PUT'] = 'BUY'
    row['ORDER_TYPE_CALL'] = 'BUY'

    # SET OPTION TYPE BASED ON INDEX, 0th index Set Call
    if row.name == 0:
        row['INSTRUMENT_TYPE'] = OPTION_TYPE_BUY
        row['ORDER_TYPE_'+OPTION_TYPE_BUY] = 'BUY'

    # if row.name == 1: # 1st index set PUT
    #     row['INSTRUMENT_TYPE'] = 'PUT'
    #     row['ORDER_TYPE_PUT'] = 'SELL'

    # ORDER MANIFEST
    row["ORDER_MANIFEST"] = str(row['STRIKE_PRICE'])+'_'+row['INSTRUMENT_TYPE']+'_'+row['ORDER_TYPE_'+row['INSTRUMENT_TYPE']]

    return row

# def populateIVAndGreeks(Configuration, df_Put_Call_Hedged, expiry_date, optionsHedging_dict, spot_value):
#     time_to_expiry_options_252, time_to_expiry_options_annualized_252, \
#     time_to_expiry_options_365, time_to_expiry_options_annualized_365 = OrderGenOptionsHelper.populateTimeToExpiry(
#         expiry_date, Configuration)
#     df_Put_Call_Hedged['DAYS_IN_YEAR'] = '252'
#
#     ######################################## ADD IVs FOR ANALYSIS #####################################################
#     df_Put_Call_Hedged = populateIVAndGreeksHedged(df_Put_Call_Hedged, time_to_expiry_options_252, time_to_expiry_options_365,
#                         spot_value, Configuration)
#
#     df_Put_Call_Hedged['NET_DELTA'] = 'NA'
#     df_Put_Call_Hedged['NET_GAMMA'] = 'NA'
#     df_Put_Call_Hedged['NET_THETA'] = 'NA'
#     df_Put_Call_Hedged['NET_VEGA'] = 'NA'
#     return df_Put_Call_Hedged

def populateIVAndGreeksHedged(df_Put_Call, time_to_expiry_options_252, time_to_expiry_options_365,
                        spot_value, Configuration):
    # MF_STEP_SIZE = orderGen_dict['MF_STEP_SIZE']
    DAYS_IN_YEAR = int(df_Put_Call['DAYS_IN_YEAR'].iloc[0])
    if DAYS_IN_YEAR == 252:
        time_to_expiry_options_annualized = time_to_expiry_options_252 / DAYS_IN_YEAR
    else:
        time_to_expiry_options_annualized = time_to_expiry_options_365 / DAYS_IN_YEAR

    # POPULATE GREEKS FOR EACH ROW
    df_Put_Call = df_Put_Call.apply(
        lambda row: populateIVAndGreeksHedgedPutCall(row, time_to_expiry_options_252,
                              time_to_expiry_options_365, spot_value, Configuration, time_to_expiry_options_annualized), axis=1)


    return df_Put_Call


def populateIVAndGreeksHedgedPutCall(row, time_to_expiry_options_252,
                                  time_to_expiry_options_365, spot_value, Configuration, time_to_expiry_options_annualized):

    indexNumber = 0 if row['OPTION_TYPE_HEDGED'] == 'PUT' else 1

    # Add IV Put
    row['IV_'+row['OPTION_TYPE_HEDGED']] = OrderGenOptionsHelper.populateIV(row['OPTION_TYPE_HEDGED'], row['STRIKE_PRICE'], spot_value,
                                                         time_to_expiry_options_annualized,
                                                         row['EXECUTION_PRICE_'+row['OPTION_TYPE_HEDGED']+'_ACTUAL'],
                                                         Configuration)[indexNumber]

    row['IV_'+row['OPTION_TYPE_HEDGED']+'_OPT'] = 0.0

    # df_Put_Call = df_Put_Call.drop(['ORDER_TYPE_PUT', 'ORDER_TYPE_CALL'], axis=1)
    # IV DIFF PCT AND ORDER TYPES
    row['IV_DIFF_PCT_OPT'] = 0.0
    row['IV_DIFF_PCT'] = 0.0

    ##################################################### ADD DELTA ###################################################
    row['DELTA_' + row['OPTION_TYPE_HEDGED']] = OrderGenOptionsHelper.populateDelta(row['OPTION_TYPE_HEDGED'], row['STRIKE_PRICE'], spot_value,
                                                        time_to_expiry_options_annualized,
                                                        row['IV_' + row['OPTION_TYPE_HEDGED']],
                                                        row['ORDER_TYPE_HEDGED'],
                                                        float(row['MULTI_FACTOR_HEDGED']),
                                                        Configuration, )[indexNumber]

    ################################################### GAMMA VALUES POPULATION ########################################
    row['GAMMA_' + row['OPTION_TYPE_HEDGED']] = OrderGenOptionsHelper.populateGamma(row['ORDER_TYPE_HEDGED'], row['STRIKE_PRICE'],
                                                        spot_value,
                                                        time_to_expiry_options_annualized, row['IV_' + row['OPTION_TYPE_HEDGED']],
                                                        float(row['MULTI_FACTOR_HEDGED']),
                                                        Configuration)

    ############################################# VEGA/THETA CALCULATIONS ###############################################
    row['THETA_' + row['OPTION_TYPE_HEDGED']] = OrderGenOptionsHelper.populateTheta(row['OPTION_TYPE_HEDGED'], row['STRIKE_PRICE'], spot_value,
                                                        time_to_expiry_options_annualized,
                                                        row['IV_' + row['OPTION_TYPE_HEDGED']],
                                                        row['ORDER_TYPE_HEDGED'],
                                                        float(row['MULTI_FACTOR_HEDGED']),
                                                        Configuration)[indexNumber]
    row['VEGA_' + row['OPTION_TYPE_HEDGED']] = OrderGenOptionsHelper.populateVega(row['OPTION_TYPE_HEDGED'], row['STRIKE_PRICE'], spot_value,
                                                       time_to_expiry_options_annualized,
                                                       row['IV_' + row['OPTION_TYPE_HEDGED']],
                                                       row['ORDER_TYPE_HEDGED'],
                                                       float(row['MULTI_FACTOR_HEDGED']),
                                                       Configuration)
    #####################################################################################################################

    return row


def placeDealsAndPopulateDBOptions(position_group_id, optionsAdjustment_dict, Configuration, df_Adjustment,
                                                         symbol, expiry_date, time_to_expiry_options_252, time_to_expiry_options_365,
                                                         spot_value, expiry_date_futures):
    optionsAdjustment_dict['position_group_id'] = position_group_id

    ################################## ADJUSTMENT SCENARIOS STARTED #############################################

    ################################## CASE 1 - SQUAREOFF ALL  #########################################################
    # df_Adjustment_Squareoff_All = df_Adjustment[df_Adjustment['INSTRUCTION_TYPE'] == 'SQUAREOFF_ALL']
    # if df_Adjustment_Squareoff_All is not None and len(df_Adjustment_Squareoff_All) > 0:
    #     squareoffPositions(df_Adjustment_Squareoff_All, optionsAdjustment_dict, Configuration, symbol, expiry_date, position_group_id,
    #                                 expiry_date_futures)

    ################################## CASE 2 - FRESH NEW ##############################################################
    df_Adjustment_Fresh_New = df_Adjustment[df_Adjustment['INSTRUCTION_TYPE'] == 'FRESH']
    df_Adjustment_Fresh_New.columns = [x.lower() for x in df_Adjustment_Fresh_New.columns]
    if df_Adjustment_Fresh_New is not None and len(df_Adjustment_Fresh_New) > 0:
        freshNewPositions(df_Adjustment_Fresh_New, optionsAdjustment_dict, Configuration, symbol, expiry_date,
                           position_group_id, expiry_date_futures)

    ################################## CASE 3 - SQUAREOFF ADJUSTMENT ###################################################
    df_Adjustment_Squareoff_Adjustment = df_Adjustment[df_Adjustment['INSTRUCTION_TYPE'] == 'ADJUSTMENT_SQUAREOFF']
    df_Adjustment_Squareoff_Adjustment.columns = [x.lower() for x in df_Adjustment_Squareoff_Adjustment.columns]
    if df_Adjustment_Squareoff_Adjustment is not None and len(df_Adjustment_Squareoff_Adjustment) > 0:
        df_Adjustment_Squareoff_All = squareoffAdjustment(df_Adjustment_Squareoff_Adjustment, optionsAdjustment_dict, Configuration, symbol, expiry_date,
                           position_group_id, expiry_date_futures)
        optionsAdjustment_dict['df_Adjustment_Squareoff_All'] = df_Adjustment_Squareoff_All

    ######################################### ADJUSTMENT SCENARIOS ENDED ##################################################

    #################################### LOGGING ORDER GENERATION #######################################################
    logging.info( "######################################################################################################")
    print("######################################################################################################")
    logging.info("ORDER GEN ADJUSTMENT INITIATED for {}, STRIKE PRICE : {}".format(optionsAdjustment_dict['symbol'], str(df_Adjustment['STRIKE_PRICE'].tolist())))
    print("ORDER GEN ADJUSTMENT INITIATED for {}, STRIKE PRICE : {}".format(optionsAdjustment_dict['symbol'], str(df_Adjustment['STRIKE_PRICE'].tolist())))
    print("######################################################################################################")
    logging.info("######################################################################################################")

    logging.info('Hedged Position Taken: Inserted in OOPS_POSITIONS AND TXN Table for Symbol {} and TransID {}'.format(symbol, position_group_id))

    return optionsAdjustment_dict


def filterExistingHedgedPositions(df_positions_existing):
    # ADD EXECUTION PRICES
    df_positions_existing_hedged = df_positions_existing[df_positions_existing['ORDER_TYPE'] == 'BUY']
    df_positions_existing_hedged = df_positions_existing_hedged[
        ['EXECUTION_PRICE', 'EXECUTION_PRICE_ACTUAL', 'QUANTITY', 'MULTI_FACTOR', "STRIKE_PRICE",
         "INSTRUMENT_TYPE", "ORDER_TYPE", "SYMBOL", "EXECUTION_PARAMS", "OOPS_TRANS_ID", "OOPS_ORDER_ID",
         "CONTRACT_EXPIRY_DATE"]]
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

def squareoffAdjustment(df_Adjustment_Squareoff_Adjustment, optionsAdjustment_dict, Configuration, symbol, expiry_date, position_group_id,
                                    expiry_date_futures):
    order_group_id = str(int(UniqueKeyGenerator().getUniqueKeyDateTime()))
    Configuration['order_group_id'] = order_group_id
    optionsAdjustment_dict['order_group_id'] = order_group_id

    schema_name = optionsAdjustment_dict['schema_name']
    ################################ STEP 1. PLACE HEDGED TRADES USING OM ###############################################
    df_Adjustment_Squareoff_All = placeOrderSquareOff(Configuration, df_Adjustment_Squareoff_Adjustment, symbol, expiry_date, position_group_id,
                                          expiry_date_futures)
    ####################################################################################################################
    # Convert all columns to uppercase
    df_Adjustment_Squareoff_All.columns = [x.upper() for x in df_Adjustment_Squareoff_All.columns]
    df_Adjustment_Squareoff_All['EXIT_PRICE'] = df_Adjustment_Squareoff_All['CURRENT_PRICE']
    # POPULATE PNL DETAILS
    df_Adjustment_Squareoff_All, optionsAdjustment_dict = calculatePnlAdjustmentSquareOff(df_Adjustment_Squareoff_All,
                                                                                          optionsAdjustment_dict)


    # Transformation
    spot_value = optionsAdjustment_dict['spot_value']
    df_Adjustment_Squareoff_All = transformationSquareOffPositions(df_Adjustment_Squareoff_All, spot_value,
                                                                             optionsAdjustment_dict)

    ###############################  INSERT POSITIONS TXN/ UPDATE POSITIONS TABLE ################################
    # INSERT IN TRACKING AUDIT TABLE
    SquareOffOptionsHelper.insertSquareOffPositionsTracking(df_Adjustment_Squareoff_All, schema_name)
    updatePositionsPostSquareOffBasedOnPositionId(df_Adjustment_Squareoff_All, schema_name)
    updateAllPositionsByPositionsGroupId(df_Adjustment_Squareoff_All, schema_name)
    insertFreshOrders(df_Adjustment_Squareoff_All, schema_name)

    return df_Adjustment_Squareoff_All

def freshNewPositions(df_Adjustment_Fresh_New, optionsAdjustment_dict, Configuration, symbol, expiry_date, position_group_id,
                                    expiry_date_futures):
    schema_name = optionsAdjustment_dict['schema_name']
    order_group_id = str(int(UniqueKeyGenerator().getUniqueKeyDateTime()))
    Configuration['order_group_id'] = order_group_id
    optionsAdjustment_dict['order_group_id'] = order_group_id
    ################################ STEP 1. PLACE HEDGED TRADES USING OM ###############################################
    df_Adjustment_Fresh_New = placeOrder(Configuration, df_Adjustment_Fresh_New, symbol, expiry_date, position_group_id,
                                          expiry_date_futures)
    ####################################################################################################################

    # Transformation
    spot_value = optionsAdjustment_dict['spot_value']
    df_Adjustment_Fresh_Transformed = transformationFreshPositions(df_Adjustment_Fresh_New, spot_value,
                                                                   optionsAdjustment_dict)

    ###############################  INSERT POSITIONS TXN/ UPDATE POSITIONS TABLE ################################
    # INSERT IN TRACKING AUDIT TABLE
    SquareOffOptionsHelper.insertSquareOffPositionsTracking(df_Adjustment_Fresh_Transformed, schema_name)

    ######################################  START : INSERT POSITIONS TABLE #############################################
    insertFreshPositions(df_Adjustment_Fresh_Transformed, schema_name)
    insertFreshOrders(df_Adjustment_Fresh_Transformed, schema_name)
    ######################################  END: INSERT POSITIONS TABLE ################################################


def calculatePnlAdjustmentSquareOff(df_Adjustment_Squareoff_All, optionsAdjustment_dict):

    REALIZED_PNL_GROUP = 0.0
    for index, row in df_Adjustment_Squareoff_All.iterrows():
        ######################################## POPULATE PNL FIELDS ###################################################

        df_Adjustment_Squareoff_All.at[index, 'CURRENT_DELTA'] = 0.0
        df_Adjustment_Squareoff_All.at[index, 'CURRENT_NET_DELTA'] = 0.0
        df_Adjustment_Squareoff_All.at[index, 'CURRENT_NET_DELTA_OVERALL'] = 0.0


        if row['TRANSACTION_TYPE'].value == 'SELL':
            REALIZED_PNL = (float(row['QUANTITY']) * float(row['ENTRY_PRICE'])) - \
                           (float(row['QUANTITY']) * float(row['EXIT_PRICE']))
        else:
            REALIZED_PNL = (float(row['QUANTITY']) * float(row['EXIT_PRICE'])) - \
                           (float(row['QUANTITY']) * float(row['ENTRY_PRICE']))

        df_Adjustment_Squareoff_All.at[index, 'REALIZED_PNL'] = REALIZED_PNL

        # ADD REALIZED PNL TO GROUP
        REALIZED_PNL_GROUP = REALIZED_PNL_GROUP + REALIZED_PNL

    # NET PNL OVERALL UPDATE
    #optionsAdjustment_dict['NET_PNL_OVERALL'] = optionsAdjustment_dict['NET_PNL_OVERALL'] + REALIZED_PNL_GROUP

    # REALIZED_PNL_OVERALL
    if optionsAdjustment_dict['REALIZED_PNL_OVERALL'] is not None:
        optionsAdjustment_dict['REALIZED_PNL_OVERALL'] = optionsAdjustment_dict['REALIZED_PNL_OVERALL'] + REALIZED_PNL_GROUP
    else:
        optionsAdjustment_dict['REALIZED_PNL_OVERALL'] = REALIZED_PNL_GROUP

    df_Adjustment_Squareoff_All['REALIZED_PNL_GROUP'] = REALIZED_PNL_GROUP
    df_Adjustment_Squareoff_All['REALIZED_PNL_OVERALL'] = optionsAdjustment_dict['REALIZED_PNL_OVERALL']

    # NET PNL OVERALL
    # MOCK
    # optionsAdjustment_dict['REALIZED_PNL_OVERALL'] = -32.50
    if optionsAdjustment_dict['isCallAdjustmentSquareOffDone'] and optionsAdjustment_dict['isPutAdjustmentSquareOffDone'] and\
            optionsAdjustment_dict['isFutBuyAdjustmentSquareOffDone'] and optionsAdjustment_dict['isFutSellAdjustmentSquareOffDone']:
        df_Adjustment_Squareoff_All['NET_PNL_OVERALL'] = optionsAdjustment_dict['REALIZED_PNL_OVERALL']
    else:
        df_Adjustment_Squareoff_All['NET_PNL_OVERALL'] = optionsAdjustment_dict['NET_PNL_OVERALL']

    return df_Adjustment_Squareoff_All, optionsAdjustment_dict


def updatePositionsPostSquareOffOptions(df_positions_Squareoff_Hedged):
    for index, row_position in df_positions_Squareoff_Hedged.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsPostSquareOffJobOptions(row_position)

def updatePositionsPostSquareOffBasedOnPositionId(df_positions_Squareoff_Hedged, schema_name):
    for index, row_position in df_positions_Squareoff_Hedged.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsPostSquareOffBasedOnPositionId(row=row_position, schema_name=schema_name)

def updateAllPositionsByPositionsGroupId(df_positions_Squareoff_Hedged, schema_name):
    for index, row_position in df_positions_Squareoff_Hedged.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updateAllPositionsByPositionsGroupId(row=row_position, schema_name=schema_name)


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

def calculatePnlFuturesFreshAddition(row, optionsHedging_dict):

    row['UNREALIZED_PNL'] = 'NA'
    row['UNREALIZED_PNL_TXN'] = row['UNREALIZED_PNL']
    row['UNREALIZED_PNL_OVERALL'] = 'NA'

    ############################### REALIZED PNL OVERALL FROM PUT EXISTING SELL - FETCH LATEST FROM DB #################
    position_group_id = optionsHedging_dict['position_group_id']
    symbol = optionsHedging_dict['symbol']
    df_positions_existing = PositionsDAO.getActivePositionsByTransIdAndSymbol(position_group_id, symbol)
    df_positions_existing.columns = [x.upper() for x in df_positions_existing.columns]
    optionsHedging_dict['df_positions_existing'] = df_positions_existing
    df_positions_existing_put_sell = df_positions_existing[(df_positions_existing['INSTRUMENT_TYPE'] == 'PUT') &
                                                           (df_positions_existing['ORDER_TYPE'] == 'SELL')]
    REALIZED_PNL_OVERALL_EXISTING = df_positions_existing_put_sell['REALIZED_PNL_OVERALL'].iloc[0]
    REALIZED_PNL_OPTIONS_EXISTING = df_positions_existing_put_sell['REALIZED_PNL_OPTIONS'].iloc[0]
    REALIZED_PNL_FUTURES_EXISTING = df_positions_existing_put_sell['REALIZED_PNL_FUTURES'].iloc[0]
    #####################################################################################################################

    row['REALIZED_PNL_OVERALL'] = REALIZED_PNL_OVERALL_EXISTING
    row['REALIZED_PNL_OPTIONS'] = REALIZED_PNL_OPTIONS_EXISTING
    row['REALIZED_PNL_FUTURES'] = REALIZED_PNL_FUTURES_EXISTING

    # NET_PNL_OVERALL
    row['NET_PNL_OVERALL'] = df_positions_existing['NET_PNL_OVERALL'].iloc[0]

    return row

def calculatePnlFuturesSquareOff(row, optionsHedging_dict, isPartialSquareoff):
    ######################################## REALIZED PNL ##############################################################
    if not isPartialSquareoff:
        row['REALIZED_PNL_OPTIONS'] = (float(row['QUANTITY_' + row['INSTRUMENT_TYPE']]) * float(row['EXECUTION_PRICE_'+row['INSTRUMENT_TYPE']+'_ACTUAL'])) - (
                                        float(row['QUANTITY_POSITION']) * float(row['EXECUTION_PRICE_ACTUAL_POSITION']))
    else:  # Partial Square OFF, SO ONLY QUANTITY FUTURES
        row['REALIZED_PNL_OPTIONS'] = (float(row['QUANTITY_' + row['INSTRUMENT_TYPE']]) * float(row['EXECUTION_PRICE_'+row['INSTRUMENT_TYPE']+'_ACTUAL'])) - (
                                        float(row['QUANTITY_' + row['INSTRUMENT_TYPE']]) * float(row['EXECUTION_PRICE_ACTUAL_POSITION']))

    row['REALIZED_PNL_TXN'] = row['REALIZED_PNL_OPTIONS']

    ############################### STEP 1:  REALIZED PNL OVERALL FROM PUT EXISTING SELL - FETCH LATEST FROM DB ########
    position_group_id = optionsHedging_dict['position_group_id']
    symbol = optionsHedging_dict['symbol']
    df_positions_existing = PositionsDAO.getActivePositionsByTransIdAndSymbol(position_group_id, symbol)
    df_positions_existing.columns = [x.upper() for x in df_positions_existing.columns]
    optionsHedging_dict['df_positions_existing'] = df_positions_existing
    df_positions_existing_put_sell = df_positions_existing[(df_positions_existing['INSTRUMENT_TYPE'] == 'PUT') &
                                                                (df_positions_existing['ORDER_TYPE'] == 'SELL')]
    REALIZED_PNL_OVERALL_EXISTING = df_positions_existing_put_sell['REALIZED_PNL_OVERALL'].iloc[0]
    REALIZED_PNL_OPTIONS_EXISTING = df_positions_existing_put_sell['REALIZED_PNL_OPTIONS'].iloc[0]
    REALIZED_PNL_FUTURES_EXISTING = df_positions_existing_put_sell['REALIZED_PNL_FUTURES'].iloc[0]
    #####################################################################################################################

    ############################### STEP 2: REALIZED PNL OVERALL - UPDATE WITH CURRENT TXN #############################
    if REALIZED_PNL_OVERALL_EXISTING == "NA":
        row['REALIZED_PNL_OVERALL'] = float(row['REALIZED_PNL_OPTIONS'])
    else:
        row['REALIZED_PNL_OVERALL'] = float(row['REALIZED_PNL_OPTIONS']) + float(REALIZED_PNL_OVERALL_EXISTING)

    # REALIZED PNL FUTURES
    if REALIZED_PNL_OPTIONS_EXISTING == "NA":
        row['REALIZED_PNL_OPTIONS'] = float(row['REALIZED_PNL_OPTIONS'])
    else:
        row['REALIZED_PNL_OPTIONS'] = float(row['REALIZED_PNL_OPTIONS']) + float(REALIZED_PNL_OPTIONS_EXISTING)

    # REALIZED PNL OPTIONS
    row['REALIZED_PNL_FUTURES'] = REALIZED_PNL_FUTURES_EXISTING
    ####################################################################################################################

    ############################# STEP 3: REALIZED PNL OVERALL - UPDATE IN DB FOR STEP 1 NEXT TXN ######################
    # if not isPartialSquareoff:
    #     row_position_pnl = {}
    #     row_position_pnl['position_group_id'] = position_group_id
    #     row_position_pnl['symbol'] = symbol
    #     row_position_pnl['realized_pnl_overall'] = row['REALIZED_PNL_OVERALL']
    #     row_position_pnl['realized_pnl_options'] = row['REALIZED_PNL_OPTIONS']
    #     row_position_pnl['realized_pnl_futures'] = row['REALIZED_PNL_FUTURES']
    #     PositionsDAO.updatePositionsRealizedPnlOverall(row_position_pnl)
    ####################################################################################################################

    ################################################ QUANTITY/ MULTI-FACTOR #############################################
    row['QUANTITY'] = float(row['QUANTITY_POSITION']) - float(row['QUANTITY_'+row['INSTRUMENT_TYPE']])
    row['MULTI_FACTOR'] = float(row['MULTI_FACTOR_POSITION']) - float(row['MULTI_FACTOR'])


    # IS PARTIAL SQUARE OFF UNREALIZED
    if not isPartialSquareoff:
        row['UNREALIZED_PNL'] = 0.0
        row['UNREALIZED_PNL_TXN'] = 0.0
        row['UNREALIZED_PNL_OVERALL'] = 0.0
        row['NET_DELTA'] = optionsHedging_dict['NET_DELTA']
        row['NET_DELTA_OVERALL'] = optionsHedging_dict['NET_DELTA_OVERALL']
    else:
        row['UNREALIZED_PNL'] = (float(row['QUANTITY']) * float(row['EXECUTION_PRICE_'+row['INSTRUMENT_TYPE']+'_ACTUAL'])) - (
                                        float(row['QUANTITY']) * float(row['EXECUTION_PRICE_ACTUAL_POSITION']))
        row['UNREALIZED_PNL_TXN'] = row['UNREALIZED_PNL']
        row['UNREALIZED_PNL_OVERALL'] = "NA"
        row['NET_DELTA'] = optionsHedging_dict['NET_DELTA']
        row['NET_DELTA_OVERALL'] = optionsHedging_dict['NET_DELTA_OVERALL']

    # NET_PNL_OVERALL
    row['NET_PNL_OVERALL'] = df_positions_existing_put_sell['NET_PNL_OVERALL'].iloc[0]

    return row

def insertFreshPositions(df_positions, schema_name):

    for index, row in df_positions.iterrows():
        PositionsDAO.insert(row=row, schema_name=schema_name)
    print('Inserted in POSITIONS Table')

def insertFreshOrders(df_positions, schema_name):
    for index, row in df_positions.iterrows():
        OrdersDAO.insert(row=row, schema_name=schema_name)
    print('Inserted in ORDERS Table')

def updatePositionsExistingOrderGenJob(df_positions,symbol):
    for index, row_position in df_positions.iterrows():
        PositionsDAO.updatePositionsExistingOrderGenJob(row_position=row_position, symbol=symbol)
    print('Inserted in OOPS_POSITIONS_OPTIONS Table')

def addTxnDetails(row):
    row['QUANTITY_TXN_'+row['OPTION_TYPE_HEDGED']] = row['QUANTITY_'+row['OPTION_TYPE_HEDGED']]
    row['MULTI_FACTOR_TXN'] = row['MULTI_FACTOR']
    return row

def transformationSquareOffPositions(df_Adjustment_Squareoff_All, spot_value, optionsAdjustment_dict):
    df_Adjustment_Squareoff_All_Transformed = pd.DataFrame()

    # Convert all columns to lowercase
    df_Adjustment_Squareoff_All.columns = [x.lower() for x in df_Adjustment_Squareoff_All.columns]

    ##############################################  CalculateNetDelta ##################################################

    for index, row in df_Adjustment_Squareoff_All.iterrows():

        # CREATE POSITION DICTIONARY
        position_dict = populatePositionDictSquareOff(row, optionsAdjustment_dict, spot_value)

        # ADD IT TO DATAFRAME
        df_Adjustment_Squareoff_All_Transformed = df_Adjustment_Squareoff_All_Transformed.append(position_dict, ignore_index = True)

    return df_Adjustment_Squareoff_All_Transformed

def populatePositionDictSquareOff(row, optionsAdjustment_dict, spot_value):
    params_dict = json.loads(optionsAdjustment_dict['params'])

    # COMMON FIELDS
    position_dict = {
        'order_id': str(int(UniqueKeyGenerator().getUniqueKeyDateTime())),
        'order_group_id': optionsAdjustment_dict['order_group_id'],
        'broker_order_id': row['broker_order_id'],
        'broker_order_status': row['broker_order_status'],
        'is_success': True,
        'signal_id': row['signal_id'],
        'signal_group_id': row['signal_group_id'],
        'position_id': row['position_id'],
        'position_group_id': row['position_group_id'],
        'position_tracking_id': UniqueKeyGenerator().getUniqueKeyDateTime(),
        'position_tracking_group_id': row['position_tracking_group_id'],
        'moneyness': row['moneyness'],
        'symbol': row['symbol'],
        'expiry_date': row['expiry_date'],
        'strike_price': int(row['strike_price']),
        'instrument_type': row['instrument_type'],
        'transaction_type': row['transaction_type'],
        'order_type': row['order_type'],
        'contract_type': row['contract_type'],
        'num_lots': int(row['num_lots']),
        'lot_size': row['lot_size'],
        'quantity': row['quantity'],
        'is_active': row['is_active'],
        'is_square_off': row['is_square_off'],
        'margin_overall': 'NA',
        'entry_price': row['entry_price'],
        'exit_price': row['exit_price'],
        'current_price': row['current_price'],
        'execution_price': row['current_price'],
        'params': row['params'],
        'unrealized_pnl': row['unrealized_pnl'],
        'realized_pnl': row['realized_pnl'],
        'unrealized_pnl_group': row['unrealized_pnl_group'],
        'realized_pnl_group': row['realized_pnl_group'],
        'net_pnl_overall': row['net_pnl_overall'],
        'realized_pnl_overall': row['realized_pnl_overall'],
        'entry_delta': row['delta'],
        'entry_net_delta': row['delta'],
        'entry_net_delta_overall': 'NA',
        'current_delta': row['delta'],
        'current_net_delta': 'NA',
        'current_net_delta_overall': 'NA',
        'entry_gamma': 'NA',
        'entry_net_gamma': 'NA',
        'current_gamma': 'NA',
        'current_net_gamma': 'NA',
        'entry_iv': 'NA',
        'entry_iv_diff_pct': 'NA',
        'current_iv': 'NA',
        'current_iv_diff_pct': 'NA',
        'net_delta_threshold': 'NA',
        'entry_theta': 'NA',
        'current_theta': 'NA',
        'entry_net_theta': 'NA',
        'current_net_theta': 'NA',
        'entry_vega': 'NA',
        'current_vega': 'NA',
        'entry_net_vega': 'NA',
        'current_net_vega': 'NA',
        'contract_expiry_date': row['expiry_date'],
        'entry_time_to_expiry': 'NA',
        'current_time_to_expiry': 'NA',
        'order_manifest': row['order_manifest'],
        'time_value_options': 'NA',
        'entry_underlying': params_dict['entry_underlying'],
        'current_underlying': spot_value,
        'expected_theta_pnl_pending': 'NA',
        'current_theta_pnl_pending': 'NA',
        'net_pnl_threshold': 'NA',
        'entry_atm_put_price': params_dict['entry_atm_put_price'],
        'entry_atm_call_price': params_dict['entry_atm_call_price'],
        'entry_atm_avg_price': params_dict['entry_atm_avg_price'],
        'entry_atm_price_diff': params_dict['entry_atm_price_diff'],
        'entry_vix': params_dict['entry_vix'],
        'current_price_pnl_pct': -1*utils.get_percentage_change(row['current_price'], row['entry_price']),
        }
    return position_dict

def transformationFreshPositions(df_Adjustment_Fresh, spot_value, optionsAdjustment_dict):
    df_Adjustment_Fresh_Transformed = pd.DataFrame()

    # Convert all columns to lowercase
    df_Adjustment_Fresh.columns = [x.lower() for x in df_Adjustment_Fresh.columns]

    ##############################################  CalculateNetDelta ##################################################

    for index, row in df_Adjustment_Fresh.iterrows():

        row['realized_pnl_overall'] = optionsAdjustment_dict['REALIZED_PNL_OVERALL']
        row['net_pnl_overall'] = optionsAdjustment_dict['NET_PNL_OVERALL']
        row['realized_pnl_group'] = optionsAdjustment_dict['REALIZED_PNL_GROUP']
        row['unrealized_pnl_group'] = optionsAdjustment_dict['UNREALIZED_PNL_GROUP']
        row['margin_overall'] = optionsAdjustment_dict['MARGIN_OVERALL']

        # CREATE POSITION DICTIONARY
        position_dict = populatePositionDictFresh(row, optionsAdjustment_dict, spot_value)

        # ADD IT TO DATAFRAME
        df_Adjustment_Fresh_Transformed = df_Adjustment_Fresh_Transformed.append(position_dict, ignore_index = True)

    return df_Adjustment_Fresh_Transformed

def populatePositionDictFresh(row, optionsAdjustment_dict, spot_value):


    # COMMON FIELDS
    position_dict = {
        'signal_id': row['signal_id'],
        'signal_group_id': row['signal_group_id'],
        'position_id': row['position_id'],
        'order_id': str(int(UniqueKeyGenerator().getUniqueKeyDateTime())),
        'order_group_id' : optionsAdjustment_dict['order_group_id'],
        'position_group_id': row['position_group_id'],
        'position_tracking_id': UniqueKeyGenerator().getUniqueKeyDateTime(),
        'position_tracking_group_id': row['position_tracking_group_id'],
        'moneyness': row['moneyness'],
        'symbol': row['symbol'],
        'expiry_date': row['expiry_date'],
        'strike_price': int(row['strike_price']),
        'instrument_type': row['instrument_type'],
        'transaction_type': row['transaction_type'],
        'order_type': row['order_type'],
        'contract_type': row['contract_type'],
        'num_lots': int(row['num_lots']),
        'lot_size': row['lot_size'],
        'quantity': row['quantity'],
        'is_active': row['is_active'],
        'is_success': True,
        'is_square_off': row['is_square_off'],
        'margin_overall': row['margin_overall'],
        'entry_price': row['entry_price'],
        'exit_price': None,
        'current_price': row['current_price'],
        'execution_price': row['current_price'],
        'params': row['params'],
        'unrealized_pnl': row['unrealized_pnl'],
        'realized_pnl': row['realized_pnl'],
        'unrealized_pnl_group': row['unrealized_pnl_group'],
        'realized_pnl_group': row['realized_pnl_group'],
        'net_pnl_overall': row['net_pnl_overall'],
        'realized_pnl_overall': row['realized_pnl_overall'],
        'entry_delta': row['delta'],
        'entry_net_delta': row['delta'],
        'entry_net_delta_overall': 'NA',
        'current_delta': row['delta'],
        'current_net_delta': 'NA',
        'current_net_delta_overall': 'NA',
        'entry_gamma': 'NA',
        'entry_net_gamma': 'NA',
        'current_gamma': 'NA',
        'current_net_gamma': 'NA',
        'entry_iv': 'NA',
        'entry_iv_diff_pct': 'NA',
        'current_iv': 'NA',
        'current_iv_diff_pct': 'NA',
        'net_delta_threshold': 'NA',
        'entry_theta': 'NA',
        'current_theta': 'NA',
        'entry_net_theta': 'NA',
        'current_net_theta': 'NA',
        'entry_vega': 'NA',
        'current_vega': 'NA',
        'entry_net_vega': 'NA',
        'current_net_vega': 'NA',
        'contract_expiry_date': row['expiry_date'],
        'entry_time_to_expiry': 'NA',
        'current_time_to_expiry': 'NA',
        'order_manifest': row['order_manifest'],
        'time_value_options': 'NA',
        'entry_underlying': 'NA',
        'current_underlying': spot_value,
        'expected_theta_pnl_pending': 'NA',
        'current_theta_pnl_pending': 'NA',
        'net_pnl_threshold': 'NA',
        'entry_atm_put_price': 0.0,
        'entry_atm_call_price': 0.0,
        'entry_atm_avg_price': 0.0,
        'entry_atm_price_diff': 0.0,
        'entry_vix': 0.0,
        'current_price_pnl_pct' : 0.0,
        'broker_order_id': row['broker_order_id'],
        'broker_order_status': row['broker_order_status']
        }
    return position_dict


def placeOrder(Configuration, df_Put_Call, symbol, expiry_date, position_group_id,
                                    expiry_date_futures):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']:
        omAdaptor = OMAdaptor()
        orderType = "HEDGING"
        tradeType = Configuration['TRADE_TYPE']
        df_Put_Call.columns = [x.upper() for x in df_Put_Call.columns]
        df_Put_Call = omAdaptor.placeOrders(df_Put_Call, Configuration, symbol, orderType, tradeType, expiry_date,
                                            position_group_id, expiry_date_futures, isFresh=False, isHedging=True,
                                            isSquareOff=False)
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

def placeOrderSquareOff(Configuration, df_Put_Call, symbol, expiry_date, position_group_id,
                                    expiry_date_futures):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']:
        omAdaptor = OMAdaptor()
        orderType = "SQUAREOFF"
        tradeType = Configuration['TRADE_TYPE']
        df_Put_Call.columns = [x.upper() for x in df_Put_Call.columns]
        df_Put_Call = omAdaptor.placeOrders(df_Put_Call, Configuration, symbol, orderType, tradeType, expiry_date,
                                            position_group_id, expiry_date_futures, isFresh=False, isHedging=False,
                                            isSquareOff=True)
        df_Put_Call.columns = [x.lower() for x in df_Put_Call.columns]

    else:  # Default Broker related fields
        df_Put_Call['broker_order_id'] = "NA"
        df_Put_Call['broker_order_status'] = "NA"

    return df_Put_Call

def addMarginInfo(orderGen_dict, Configuration, df_Put_Call, symbol):
    LOT_SIZE = Configuration['LOT_SIZE_' + symbol]

    # POPULATE PREMIUM
    df_Put_Call = df_Put_Call.apply(lambda row: populatePremium(row, LOT_SIZE), axis=1)

    # POPULATE MARGIN
    df_Put_Call['MARGIN_FUTURES'] = 0.0
    df_Put_Call['MARGIN_PUT'] = 0.0
    df_Put_Call['MARGIN_CALL'] = 0.0
    #df_Put_Call['MARGIN_TXN'] = 0.0 # CHECK PARITY RULES, ALREADY DEFINED THERE
    # marginService = MarginService()
    #
    # df_Put_Call['MARGIN_FUTURES'] = marginService.caculateMarginUsedFutures(symbol, MF_STEP_SIZE)
    # df_Put_Call['MARGIN_PUT'] = df_Put_Call.apply(lambda row: populateMargin(symbol, 'PUT', row, marginService),axis=1)
    # df_Put_Call['MARGIN_CALL'] = df_Put_Call.apply(lambda row: populateMargin(symbol, 'CALL', row, marginService),axis=1)
    # df_Put_Call['MARGIN_TXN'] = df_Put_Call.apply(lambda row: (float(row['MARGIN_FUTURES']) +float(row['MARGIN_CALL']) +float(row['MARGIN_PUT']))
    #                                                                     -(float(row['PREMIUM_PUT']) +float(row['PREMIUM_CALL'])), axis=1).astype(float)

    return df_Put_Call


def populatePremium(row, LOT_SIZE):

    if row['ORDER_TYPE_HEDGED'] == 'BUY':
        row['PREMIUM_'+row['OPTION_TYPE_HEDGED']]= float(row['EXECUTION_PRICE_'+row['OPTION_TYPE_HEDGED']+'_ACTUAL']) * float(LOT_SIZE) * float(row['MULTI_FACTOR_HEDGED'])*-1
    else:
        row['PREMIUM_' + row['OPTION_TYPE_HEDGED']] = float(row['EXECUTION_PRICE_'+row['OPTION_TYPE_HEDGED']+'_ACTUAL']) * float(LOT_SIZE) * float(row['MULTI_FACTOR_HEDGED'])

    return row

def getNetDeltaThreshold(Configuration, df_positions_tracking, symbol):

    df_buy_positions_existing = df_positions_tracking[df_positions_tracking['order_type'] == 'BUY']


    ######################################## GET NET DELTA THRESHOLD ###################################################
    # MIN IS 1, CALCULATED BY CURRENT NET GAMMA * PCT(30)
    # GAMMA_DELTA_PCT_THRESHOLD SHOULD BE 60% FOR FIRST BUY
    if df_buy_positions_existing is None or len(df_buy_positions_existing) == 0:
        GAMMA_DELTA_PCT_THRESHOLD = float(Configuration['GAMMA_DELTA_PCT_THRESHOLD_1_' + symbol])
    else:
        GAMMA_DELTA_PCT_THRESHOLD = float(Configuration['GAMMA_DELTA_PCT_THRESHOLD_2_' + symbol])

    # MOCK DATA
    #GAMMA_DELTA_PCT_THRESHOLD = 1000.0
    ####################################################################################################################

    ####################################################################################################################
    # GET RESTRICT DELTA THRESHOLD FACTOR AND DIVIDE BY MF
    # THEN MIN OF NET_DELTA_THRESHOLD AND MF/RESTRICTION FACTOR SHOULD BE DELTA THRESHOLD
    # GET SELL OPTIONS CURRENT MULTI FACTOR
    # RESTRICT_DELTA_THRESHOLD_BY_FACTOR = float(Configuration['RESTRICT_DELTA_THRESHOLD_BY_FACTOR_' + symbol])
    # MF_STEP_SIZE = df_positions_tracking[df_positions_tracking['order_type'] == 'SELL']['multi_factor'].iloc[0]
    #
    # if MF_STEP_SIZE > RESTRICT_DELTA_THRESHOLD_BY_FACTOR:
    #     NET_DELTA_THRESHOLD_RESTRICTED = math.floor(MF_STEP_SIZE / RESTRICT_DELTA_THRESHOLD_BY_FACTOR)
    # else:
    #     NET_DELTA_THRESHOLD_RESTRICTED = MF_STEP_SIZE
    ####################################################################################################################

    NET_DELTA_THRESHOLD_MIN = getNetDeltaMinThreshold(Configuration, symbol)
    CURRENT_NET_GAMMA = abs(float(df_positions_tracking['current_net_gamma'].iloc[0]))
    CURRENT_NET_GAMMA_CALIBRATED = CURRENT_NET_GAMMA * GAMMA_DELTA_PCT_THRESHOLD
    NET_DELTA_THRESHOLD_MAX = max(NET_DELTA_THRESHOLD_MIN, CURRENT_NET_GAMMA_CALIBRATED)

    # COMMENTING 1/3rd RULE RESTRICTION IN CALCULATION GAMMA DELTA THRESHOLD IN OOPS3
    #NET_DELTA_THRESHOLD_MAX = min(NET_DELTA_THRESHOLD_RESTRICTED, NET_DELTA_THRESHOLD_MAX)
    return NET_DELTA_THRESHOLD_MAX

def getNetDeltaMinThreshold(Configuration, symbol):

    # MIN DELTA THRESHOLD DEPENDING ON INTIAL MARGIN
    DELTA_INITIAL_MARGIN_THRESHOLD = float(Configuration['DELTA_INITIAL_MARGIN_THRESHOLD'])
    TOTAL_INITIAL_MARGIN = float(Configuration['TOTAL_INITIAL_MARGIN'])

    if TOTAL_INITIAL_MARGIN < DELTA_INITIAL_MARGIN_THRESHOLD:
        return float(Configuration['NET_DELTA_THRESHOLD_MIN_1_' + symbol])
    else:
        return float(Configuration['NET_DELTA_THRESHOLD_MIN_2_' + symbol])