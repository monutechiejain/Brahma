import pandas as pd
from helper import OrderGenOptionsHelper, SquareOffOptionsHelper
from rules import SquareOffRuleEngine
from dao import PositionsDAO, PositionsTrackingDAO
from common import utils
from entities.Enums import InstrumentTypeEnum, TransactionTypeEnum
import logging
import json
import time

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',15)
pd.set_option('display.max_rows',200)


def squareOffOptions(**kwargs):
    symbol = kwargs['symbol']

    Configuration = kwargs['Configuration']
    position_group_id = kwargs['position_group_id']
    contract_type = kwargs['contract_type']
    schema_name = kwargs['schema_name']
    squareOff_dict = {}
    squareOff_dict['isRunJobWithoutDelay'] = False
    squareOff_dict['isSquareOff'] = False
    squareOff_dict['symbol'] = symbol
    squareOff_dict['position_group_id'] = position_group_id
    squareOff_dict['contract_type'] = contract_type
    squareOff_dict['LOT_SIZE'] = Configuration['LOT_SIZE_'+symbol]
    squareOff_dict['TOTAL_INITIAL_MARGIN'] = float(Configuration['TOTAL_INITIAL_MARGIN'])
    squareOff_dict['utilised'] = 0.0
    squareOff_dict['schema_name'] = schema_name
    params = {}
    params['isCallAdjustmentSquareOffDone'] = False
    params['isPutAdjustmentSquareOffDone'] = False
    params['isFutBuyFreshPositionTaken'] = False
    params['isFutSellFreshPositionTaken'] = False
    params['isFutBuyAdjustmentSquareOffDone'] = False
    params['isFutSellAdjustmentSquareOffDone'] = False

    ########################################### GET EXISTING POSITIONS/ EXPIRY DATE/ POSITIONS TARCKING #################
    df_positions_existing = PositionsDAO.getActivePositionsByPositionGroupIdAndSymbol(schema_name, position_group_id, symbol)
    df_positions_taken_today = PositionsDAO.getPositionsByPositionGroupIdAndSymbol(schema_name, position_group_id,
                                                                                      symbol)

    df_positions_tracking_last_itr = PositionsTrackingDAO.getActivePositionsByLatestPositionTrackingGroupIdAndSymbol(schema_name, position_group_id, symbol)
    df_positions_tracking_last_itr.sort_values(by=['ID'], ascending=False, inplace=True)

    ########################################### REALIZED PNL OVERALL/PARAMS ############################################
    squareOff_dict['REALIZED_PNL_OVERALL'] = None
    squareOff_dict['params'] = json.dumps(params)
    if df_positions_tracking_last_itr is not None and len(df_positions_tracking_last_itr) > 0:
        squareOff_dict['REALIZED_PNL_OVERALL'] = df_positions_tracking_last_itr['REALIZED_PNL_OVERALL'].iloc[0]

        # ADJUSTMENT PARAMS
        tracking_params = json.loads(df_positions_tracking_last_itr['PARAMS'].iloc[0])
        params['isCallAdjustmentSquareOffDone'] = tracking_params['isCallAdjustmentSquareOffDone']
        params['isPutAdjustmentSquareOffDone'] = tracking_params['isPutAdjustmentSquareOffDone']
        params['isFutBuyFreshPositionTaken'] = tracking_params['isFutBuyFreshPositionTaken']
        params['isFutSellFreshPositionTaken'] = tracking_params['isFutSellFreshPositionTaken']
        params['isFutBuyAdjustmentSquareOffDone'] = tracking_params['isFutBuyAdjustmentSquareOffDone']
        params['isFutSellAdjustmentSquareOffDone'] = tracking_params['isFutSellAdjustmentSquareOffDone']
        logging.info("ADJUSTMENT CURRENT STATUS: {}".format(params))
        squareOff_dict['params'] = json.dumps(params)

    if df_positions_existing is None or len(df_positions_existing) == 0:
        return

    df_positions_existing.columns = [x.upper() for x in df_positions_existing.columns]
    df_put_existing = df_positions_existing[df_positions_existing['INSTRUMENT_TYPE'] == InstrumentTypeEnum.PUT]
    df_call_existing = df_positions_existing[df_positions_existing['INSTRUMENT_TYPE'] == InstrumentTypeEnum.CALL]
    df_futures_existing = df_positions_existing[df_positions_existing['INSTRUMENT_TYPE'] == InstrumentTypeEnum.FUTURES]
    df_put_taken_today = df_positions_taken_today[df_positions_taken_today['INSTRUMENT_TYPE'] == InstrumentTypeEnum.PUT]
    squareOff_dict['df_put_taken_today'] = df_put_taken_today
    squareOff_dict['MF_EXISTING'] = df_positions_existing[(df_positions_existing['TRANSACTION_TYPE'] == TransactionTypeEnum.SELL)]['NUM_LOTS'].iloc[0]

    ###################################################  EXPIRY DATE OPTIONS ############################################
    expiry_date = df_positions_existing['EXPIRY_DATE'].iloc[0]

    expiry_date_options, expiry_date_futures = OrderGenOptionsHelper.callExpiryDateAPI(Configuration, contract_type)
    expiry_date_options = expiry_date

    # ADD TO DICT
    squareOff_dict['df_positions_existing'] = df_positions_existing
    squareOff_dict['expiry_date_futures'] = expiry_date_futures
    squareOff_dict['df_positions_tracking_last_itr'] = df_positions_tracking_last_itr
    squareOff_dict['df_put_existing'] = df_put_existing
    squareOff_dict['df_call_existing'] = df_call_existing
    squareOff_dict['df_futures_existing'] = df_futures_existing


    ################################################ MDS CALL TO GET OPTION CHAIN #######################################
    mdsAdaptor = utils.getMDSAdaptor(Configuration)
    df_Call, df_Put, spot_value, future_value = mdsAdaptor.getStrikePricesAPI(Configuration, symbol, expiry_date)

    ##################### SCENARIO IF SPOT PRICES NOT UPDATING, CONSIDER FUTURE VALUE AS SPOT ##########################
    if Configuration['IS_FUTURE_CONSIDER_AS_SPOT_'+symbol] == 'Y':
        spot_value = future_value
    ####################################################################################################################

    squareOff_dict['expiry_date'] = expiry_date
    squareOff_dict['expiry_date_futures'] = expiry_date_futures
    squareOff_dict['spot_value'] = spot_value
    squareOff_dict['future_price_current'] = future_value
    squareOff_dict['future_price_initial'] = json.loads(df_put_taken_today['PARAMS'].iloc[0])['future_price_initial']

    ############################################ ADD LEVELS TO PUT CALL AND MERGE - REQUIRED FOR HEDGING ###############
    df_Level, CURRENT_SP_ATM = OrderGenOptionsHelper.addLevelPutCall(df_Call, df_Put, spot_value, Configuration)

    squareOff_dict['df_Level'] = df_Level
    squareOff_dict['CURRENT_SP_ATM'] = CURRENT_SP_ATM
    squareOff_dict['SP_ATM'] = CURRENT_SP_ATM

    if len(df_positions_existing) == 0: return  # NO EXISTING POSITIONS IN DB

    ######################################## ADD BID ASK IF NOT IN OPTION CHAIN ########################################
    ts_start = time.time()
    squareOff_dict['position_group_id'] = position_group_id
    df_positions_existing = SquareOffOptionsHelper.addBidAskPricesMDS(df_positions_existing, expiry_date, symbol, Configuration, squareOff_dict,
                                                           isValidationRequired = False, isSquareOffJob = True)
    ts_end = time.time()
    time_taken = ts_end - ts_start
    logging.info("{}: Function:squareOffOptions, Market Depth Call, Time Taken : {}".format(Configuration['SCHEMA_NAME'], time_taken))



    ###################################### WAP BID ASK PRICES WITH QUANTITY ############################################
    df_positions_existing['LOT_SIZE'] = 0.0
    df_positions_existing['BID_PRICE'] = 0.0
    df_positions_existing['ASK_PRICE'] = 0.0
    if not (len(df_put_existing) == 0 and len(df_call_existing) == 0):
        df_positions_existing = SquareOffOptionsHelper.populateWAPBidAskPrices(df_positions_existing, symbol, Configuration, squareOff_dict)

    ############################### POPULATE EXECUTION PRICES WAP AND REVERSE BID ASK AS IT IS SQUAREOFF ################
    df_positions_existing = SquareOffOptionsHelper.populateCurrentPricesWAP(df_positions_existing, future_value)

    ######################################## ADD IVs FOR ANALYSIS #####################################################
    ########################################### TIME TO EXPIRY CALCULATIONS ###########################################
    time_to_expiry_options_252, time_to_expiry_options_annualized_252, \
    time_to_expiry_options_365, time_to_expiry_options_annualized_365 = OrderGenOptionsHelper.populateTimeToExpiry(expiry_date, Configuration)
    df_positions_existing['DAYS_IN_YEAR'] = '252'
    squareOff_dict['time_to_expiry_options'] = time_to_expiry_options_252
    ts_start = time.time()
    df_positions_existing = SquareOffOptionsHelper.populateVIXGreeksIV(symbol, Configuration, df_positions_existing, squareOff_dict)
    ts_end = time.time()
    time_taken = ts_end - ts_start
    logging.info("{}: Function:squareOffOptions, VIX and Greeks API Called Successfully, Time Taken : {}".format(Configuration['SCHEMA_NAME'], time_taken))

    #################################################  SQUAREOFF RULE ENGINE ############################################
    df_positions_existing = df_positions_existing  # Set df positions options to existing then later add futures df
    squareOff_dict['df_positions_existing'] = df_positions_existing
    squareOff_dict = SquareOffRuleEngine.decisonMaker(squareOff_dict, Configuration)
    df_positions_existing = squareOff_dict['df_positions_existing']

    #######################################  NET DELTA UPDATE  ##########################################################
    squareOff_dict['df_positions_existing'] = df_positions_existing
    squareOff_dict = SquareOffOptionsHelper.populateNetDelta(squareOff_dict, Configuration)

    ###################################### ADD FUTURES - CALL MDS, CALCULATE WAP #######################################
    if len(df_futures_existing) > 0 and (len(df_put_existing) > 0 or len(df_call_existing) > 0) : # FUTURES ALONG WITH PUT OR CALL
        df_futures_existing['ASK_PRICE'] = future_value
        df_futures_existing['BID_PRICE'] = future_value
        df_futures_existing['CURRENT_PRICE'] = future_value
        df_futures_existing['IS_SQUARE_OFF'] = False
        df_positions_existing.columns = [x.upper() for x in df_positions_existing.columns]
        df_positions_existing = df_positions_existing.append(df_futures_existing, ignore_index=True)
        df_positions_existing.fillna('NA', inplace=True)
        squareOff_dict['df_positions_existing'] = df_positions_existing

    if len(df_futures_existing) > 0 and len(df_put_existing) == 0 and len(df_call_existing) == 0 : # FUTURES WITH NO PUT OR CALL
        df_positions_existing.columns = [x.upper() for x in df_positions_existing.columns]
        df_positions_existing['ASK_PRICE'] = future_value
        df_positions_existing['BID_PRICE'] = future_value
        df_positions_existing['CURRENT_PRICE'] = future_value
        df_positions_existing['IS_SQUARE_OFF'] = False
        df_positions_existing.fillna('NA', inplace=True)
        squareOff_dict['df_positions_existing'] = df_positions_existing

    ################################################ CALCULATE PNL #####################################################
    squareOff_dict = SquareOffOptionsHelper.placeDealsAndCalculatePnl(squareOff_dict, Configuration, symbol,
                                                                      expiry_date_options, expiry_date_futures,
                                                                      position_group_id, expiry_date, spot_value)

    ################################################ PLACE DEALS AND POPULATE DB #######################################
    ts_start = time.time()
    squareOff_dict = SquareOffOptionsHelper.populateDBOptions(squareOff_dict, symbol,
                                                                           time_to_expiry_options_252,
                                                                           time_to_expiry_options_365,
                                                                           spot_value, Configuration)
    ts_end = time.time()
    time_taken = ts_end - ts_start
    logging.info("{}: Function:squareOffOptions, Rows Added to Tracking Table, Time Taken : {}".format(Configuration['SCHEMA_NAME'], time_taken))

    return squareOff_dict

























