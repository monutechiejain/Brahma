
from helper import OptionsAdjustmentHelper
from entities.Enums import AdjustmentTypeEnum, InstrumentTypeEnum, TransactionTypeEnum
import pandas as pd
from common import utils
from common.UniqueKeyGenerator import UniqueKeyGenerator
import numpy as np
import json
from helper import OrderGenOptionsHelper
from dao import ClientConfigurationDAO


def decisonMaker(optionsAdjustment_dict, squareOff_dict, Configuration, symbol):
    isOptionsAdjustmentAllowed = False

    # POPULATE POSITIONS TRACKING PARAMS
    params = json.loads(optionsAdjustment_dict['params'])
    isCallAdjustmentSquareOffDone = params['isCallAdjustmentSquareOffDone']
    isPutAdjustmentSquareOffDone = params['isPutAdjustmentSquareOffDone']
    isFutBuyFreshPositionTaken = params['isFutBuyFreshPositionTaken']
    isFutSellFreshPositionTaken = params['isFutSellFreshPositionTaken']
    isFutBuyAdjustmentSquareOffDone = params['isFutBuyAdjustmentSquareOffDone']
    isFutSellAdjustmentSquareOffDone = params['isFutSellAdjustmentSquareOffDone']

    optionsAdjustment_dict['isCallAdjustmentSquareOffDone'] = isCallAdjustmentSquareOffDone
    optionsAdjustment_dict['isPutAdjustmentSquareOffDone'] = isPutAdjustmentSquareOffDone
    optionsAdjustment_dict['isFutBuyFreshPositionTaken'] = isFutBuyFreshPositionTaken
    optionsAdjustment_dict['isFutSellFreshPositionTaken'] = isFutSellFreshPositionTaken
    optionsAdjustment_dict['isFutBuyAdjustmentSquareOffDone'] = isFutBuyAdjustmentSquareOffDone
    optionsAdjustment_dict['isFutSellAdjustmentSquareOffDone'] = isFutSellAdjustmentSquareOffDone

    df_positions_tracking = squareOff_dict['df_positions_tracking']
    optionsAdjustment_dict['isOptionsAdjustmentAllowed'] = False
    position_group_id = optionsAdjustment_dict['position_group_id']
    signal_id = df_positions_tracking['signal_id'].iloc[0]
    signal_group_id = df_positions_tracking['signal_group_id'].iloc[0]
    position_tracking_group_id = squareOff_dict['position_tracking_group_id']
    expiry_date = squareOff_dict['expiry_date']


    #########################################   GET NUM_LOTS ###########################################################
    NUM_LOTS = float(squareOff_dict["MF_EXISTING"])

    ######################################   ADJUSTMENT RULES ##########################################################
    # RULE NO 1: FOR PUT: IF SL HIT EXIT
    # RULE NO 2: FOR CALL : IF SL HIT EXIT
    optionsAdjustment_dict = OptionsAdjustmentHelper.populateAdjustmentType(optionsAdjustment_dict,
                                                                            squareOff_dict, Configuration, symbol)

    #################################### EXIT IF NO ADJUSTMENTS REQUIRED ###############################################
    if not optionsAdjustment_dict["isOptionsAdjustmentAllowed"]:
        return optionsAdjustment_dict

    df_Adjustment = pd.DataFrame()

    ######################### RULE NO 1: CALL SIDE ADJUSTMENT SQUAREOFF, CALL SL HIT ####################################
    if optionsAdjustment_dict[AdjustmentTypeEnum] == AdjustmentTypeEnum.CALL_ADJUSTMENT_SQUAREOFF:
        ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'ORDER_GENERATION_ACTIVE', 'Y')  # LOCK Added
        instrument_type = InstrumentTypeEnum.CALL
        optionsAdjustment_dict = populateAdjustmentSquareOff(Configuration, NUM_LOTS, df_Adjustment, squareOff_dict, expiry_date,
                                                  df_positions_tracking, isOptionsAdjustmentAllowed,
                                                  optionsAdjustment_dict, position_group_id,
                                                  position_tracking_group_id, signal_group_id,
                                                  signal_id, symbol, instrument_type)
    ######################## RULE NO 2: PUT SIDE ADJUSTMENT SQUAREOFF, PUT SL HIT #######################################
    if optionsAdjustment_dict[AdjustmentTypeEnum] == AdjustmentTypeEnum.PUT_ADJUSTMENT_SQUAREOFF:
        ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'ORDER_GENERATION_ACTIVE', 'Y')  # LOCK Added
        instrument_type = InstrumentTypeEnum.PUT
        optionsAdjustment_dict = populateAdjustmentSquareOff(Configuration, NUM_LOTS, df_Adjustment, squareOff_dict, expiry_date,
                                                  df_positions_tracking, isOptionsAdjustmentAllowed,
                                                  optionsAdjustment_dict, position_group_id,
                                                  position_tracking_group_id, signal_group_id,
                                                  signal_id, symbol, instrument_type)

    ######################## RULE NO 3: FUTURE SELL FRESH ADJUSTMENT, MARKET DOWN FROM 9:20 THRESHOLD ##################
    if optionsAdjustment_dict[AdjustmentTypeEnum] == AdjustmentTypeEnum.FUT_SELL_FRESH_POSITION:
        ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'ORDER_GENERATION_ACTIVE', 'Y')  # LOCK Added
        instrument_type = InstrumentTypeEnum.FUTURES
        transaction_type = TransactionTypeEnum.SELL
        instruction_type = "FRESH"
        optionsAdjustment_dict = populateAdjustmentFuturesFresh(Configuration, NUM_LOTS, df_Adjustment, squareOff_dict, expiry_date,
                                                  df_positions_tracking, isOptionsAdjustmentAllowed,
                                                  optionsAdjustment_dict, position_group_id,
                                                  position_tracking_group_id, signal_group_id,
                                                  signal_id, symbol, instrument_type, transaction_type, instruction_type)

    ######################## RULE NO 4: FUTURE SELL FRESH ADJUSTMENT SQUAREOFF ##########################################
    if optionsAdjustment_dict[AdjustmentTypeEnum] == AdjustmentTypeEnum.FUT_SELL_ADJUSTMENT_SQUAREOFF:
        ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'ORDER_GENERATION_ACTIVE','Y')  # LOCK Added
        instrument_type = InstrumentTypeEnum.FUTURES
        transaction_type = None
        instruction_type = "ADJUSTMENT_SQUAREOFF"
        optionsAdjustment_dict = populateAdjustmentFuturesSquareoff(Configuration, NUM_LOTS, df_Adjustment,
                                                                squareOff_dict, expiry_date,
                                                                df_positions_tracking, isOptionsAdjustmentAllowed,
                                                                optionsAdjustment_dict, position_group_id,
                                                                position_tracking_group_id, signal_group_id,
                                                                signal_id, symbol, instrument_type,
                                                                transaction_type, instruction_type)

    ######################## RULE NO 5: FUTURE BUY FRESH ADJUSTMENT, MARKET UP FROM 9:20 THRESHOLD #####################
    if optionsAdjustment_dict[AdjustmentTypeEnum] == AdjustmentTypeEnum.FUT_BUY_FRESH_POSITION:
        ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'ORDER_GENERATION_ACTIVE',
                                                   'Y')  # LOCK Added
        instrument_type = InstrumentTypeEnum.FUTURES
        transaction_type = TransactionTypeEnum.BUY
        instruction_type = "FRESH"
        optionsAdjustment_dict = populateAdjustmentFuturesFresh(Configuration, NUM_LOTS, df_Adjustment,
                                                                squareOff_dict, expiry_date,
                                                                df_positions_tracking, isOptionsAdjustmentAllowed,
                                                                optionsAdjustment_dict, position_group_id,
                                                                position_tracking_group_id, signal_group_id,
                                                                signal_id, symbol, instrument_type,
                                                                transaction_type, instruction_type)

    ######################## RULE NO 6: FUTURE BUY FRESH ADJUSTMENT SQUAREOFF ##########################################
    if optionsAdjustment_dict[AdjustmentTypeEnum] == AdjustmentTypeEnum.FUT_BUY_ADJUSTMENT_SQUAREOFF:
        ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'ORDER_GENERATION_ACTIVE',
                                                   'Y')  # LOCK Added
        instrument_type = InstrumentTypeEnum.FUTURES
        transaction_type = None
        instruction_type = "ADJUSTMENT_SQUAREOFF"
        optionsAdjustment_dict = populateAdjustmentFuturesSquareoff(Configuration, NUM_LOTS, df_Adjustment,
                                                                    squareOff_dict, expiry_date,
                                                                    df_positions_tracking,
                                                                    isOptionsAdjustmentAllowed,
                                                                    optionsAdjustment_dict, position_group_id,
                                                                    position_tracking_group_id, signal_group_id,
                                                                    signal_id, symbol, instrument_type,
                                                                    transaction_type, instruction_type)

    return optionsAdjustment_dict

def populateAdjustment(Configuration, NUM_LOTS, df_Adjustment, squareOff_dict,expiry_date,
                                                  df_positions_tracking, isOptionsAdjustmentAllowed,
                                                  optionsAdjustment_dict, position_group_id,
                                                  position_tracking_group_id, signal_group_id,
                                                  signal_id, symbol, optionType):
    df_Level = squareOff_dict['df_Level']
    ######################################  WAP BID ASK PRICES WITH QUANTITY #########################################
    df_Level = OrderGenOptionsHelper.addBidAskPricesMDS(df_Level, expiry_date, symbol, Configuration,
                                                        squareOff_dict,
                                                        isValidationRequired=False)

    df_Level = OptionsAdjustmentHelper.populateGreeks(df_Level, NUM_LOTS, squareOff_dict, symbol, Configuration)
    df_Level.reset_index(drop=True, inplace=True)

    squareOff_dict['df_Level'] = df_Level

    # STEP 1: FETCH STRIKE FOR LEG 3 FROM EXISTING POSITIONS, ADD  BUY FOR SP GREATER/LESSER THAN LEG 3 SP
    SP_LEG_3 = int(df_positions_tracking[df_positions_tracking['order_manifest'].str.contains(optionType+"_SELL_LEG_3")][
                                            'strike_price'].iloc[0])

    # POPULATE SP LEG 5, IN CASE OF CALL ITS GREATER, IN CASE OF PUT ITS LESSER THAN LEG 3 SP
    if optionType == 'CALL':
        SP_LEG_5 = SP_LEG_3+int(Configuration['SP_STEP_SIZE_'+symbol])
    else:
        SP_LEG_5 = SP_LEG_3 - int(Configuration['SP_STEP_SIZE_' + symbol])

    QUANTITY = int(NUM_LOTS * float(Configuration['LOT_SIZE_' + symbol]))
    df_Adjustment['STRIKE_PRICE'] = [SP_LEG_5]
    df_Adjustment['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
    df_Adjustment['INSTRUMENT_TYPE'] = optionType
    df_Adjustment['TRANSACTION_TYPE'] = 'BUY'
    df_Adjustment['ENTRY_PRICE'] = 0.0  # populate later from df_Level
    df_Adjustment['EXIT_PRICE'] = 0.0  # populate later from df_Level
    df_Adjustment['ENTRY_PARAMS'] = json.dumps({})
    df_Adjustment['DELTA'] = 0.0  # populate later from df_Level
    df_Adjustment['MULTI_FACTOR'] = NUM_LOTS
    df_Adjustment['QUANTITY'] = QUANTITY
    df_Adjustment['INSTRUCTION_TYPE'] = "LEG_5_ADJUSTMENT"
    df_Adjustment['IS_ACTIVE'] = True
    df_Adjustment['SIGNAL_ID'] = signal_id
    df_Adjustment['SIGNAL_GROUP_ID'] = signal_group_id
    df_Adjustment['POSITION_GROUP_ID'] = position_group_id
    df_Adjustment['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
    df_Adjustment.reset_index(drop=True, inplace=True)

    # STEP 8: MERGE WITH df_Level AND ADD EXIT AND ENTRY PRICES
    isOptionsAdjustmentAllowed = True
    df_Adjustment = pd.merge(df_Adjustment, df_Level, how='inner', on=['STRIKE_PRICE', 'MULTI_FACTOR'])
    df_Adjustment = OptionsAdjustmentHelper.populateFieldsAfterAdjustment(df_Adjustment, optionsAdjustment_dict,
                                                                          Configuration)

    df_Adjustment = df_Adjustment.replace({np.nan: None})
    optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = isOptionsAdjustmentAllowed
    optionsAdjustment_dict["df_Adjustment"] = df_Adjustment
    return optionsAdjustment_dict

def populateAdjustmentSquareOff(Configuration, NUM_LOTS, df_Adjustment, squareOff_dict,expiry_date,
                                                  df_positions_tracking, isOptionsAdjustmentAllowed,
                                                  optionsAdjustment_dict, position_group_id,
                                                  position_tracking_group_id, signal_group_id,
                                                  signal_id, symbol, instrument_type):
    df_Level = squareOff_dict['df_Level']
    ######################################  WAP BID ASK PRICES WITH QUANTITY #########################################
    df_Level = OrderGenOptionsHelper.addBidAskPricesMDS(df_Level, expiry_date, symbol, Configuration,
                                                        squareOff_dict,
                                                        isValidationRequired=False)


    #df_Level = OptionsAdjustmentHelper.populateGreeks(df_Level, NUM_LOTS, squareOff_dict, symbol, Configuration)
    df_Level.reset_index(drop=True, inplace=True)

    squareOff_dict['df_Level'] = df_Level

    df_Adjustment = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL) &
                                (df_positions_tracking['instrument_type'] == instrument_type)]
    MF_STEP_SIZE = df_positions_tracking[(df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL) &
                                (df_positions_tracking['instrument_type'] == instrument_type)]['num_lots'].iloc[0]
    df_Adjustment.columns = [x.upper() for x in df_Adjustment.columns]

    #df_Adjustment['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
    df_Adjustment['EXIT_PRICE'] = 0.0  # populate later from df_Level
    df_Adjustment['DELTA'] = 0.0  # populate later from df_Level
    df_Adjustment['MULTI_FACTOR'] = df_Adjustment['NUM_LOTS']

    df_Adjustment['INSTRUCTION_TYPE'] = "ADJUSTMENT_SQUAREOFF"
    df_Adjustment['IS_SQUARE_OFF'] = True
    df_Adjustment['IS_ACTIVE'] = False

    # STEP 8: MERGE WITH df_Level AND ADD EXIT AND ENTRY PRICES
    isOptionsAdjustmentAllowed = True
    # DROP COLUMNS IF REQUIRED
    df_Adjustment = pd.merge(df_Adjustment, df_Level, how='inner', on=['STRIKE_PRICE'])

    ######################################  WAP BID ASK PRICES WITH QUANTITY ###########################################
    df_Adjustment = OrderGenOptionsHelper.populateWAPBidAskPrices(df_Adjustment, MF_STEP_SIZE, symbol, Configuration)

    ######################################  POPULATE ADDITIONAL FIELDS #################################################
    df_Adjustment = OptionsAdjustmentHelper.populateFieldsAfterAdjustmentSquareOff(df_Adjustment, optionsAdjustment_dict,
                                                                          Configuration)

    # DROP COLUMNS IF REQUIRED
    df_Adjustment = df_Adjustment.drop(['CURRENT_TIME_TO_EXPIRY'], axis=1)
    df_Adjustment.reset_index(drop=True, inplace=True)

    optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = isOptionsAdjustmentAllowed
    optionsAdjustment_dict["df_Adjustment"] = df_Adjustment
    return optionsAdjustment_dict

def populateAdjustmentFuturesFresh(Configuration, NUM_LOTS, df_Adjustment, squareOff_dict,expiry_date,
                                                  df_positions_tracking, isOptionsAdjustmentAllowed,
                                                  optionsAdjustment_dict, position_group_id,
                                                  position_tracking_group_id, signal_group_id,
                                                  signal_id, symbol, instrument_type, transaction_type, instruction_type):
    df_Adjustment = df_positions_tracking.copy()
    df_Adjustment.columns = [x.upper() for x in df_Adjustment.columns]
    df_Adjustment = df_Adjustment.sort_values(by=['POSITION_TRACKING_ID'], ascending=False)
    df_Adjustment = df_Adjustment.head(1)
    QUANTITY = int(NUM_LOTS * float(Configuration['LOT_SIZE_' + symbol]))
    df_Adjustment['STRIKE_PRICE'] = int(squareOff_dict['future_price_current'])
    df_Adjustment['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
    df_Adjustment['INSTRUMENT_TYPE'] = instrument_type
    df_Adjustment['TRANSACTION_TYPE'] = transaction_type
    df_Adjustment['ENTRY_PRICE'] = squareOff_dict['future_price_current']
    df_Adjustment['CURRENT_PRICE'] = squareOff_dict['future_price_current']
    df_Adjustment['EXIT_PRICE'] = 0.0  # populate later from df_Level
    df_Adjustment['DELTA'] = 0.0  # populate later from df_Level
    df_Adjustment['NUM_LOTS'] = NUM_LOTS
    df_Adjustment['QUANTITY'] = QUANTITY
    df_Adjustment['INSTRUCTION_TYPE'] = instruction_type
    df_Adjustment['IS_ACTIVE'] = True
    df_Adjustment['SIGNAL_ID'] = signal_id
    df_Adjustment['SIGNAL_GROUP_ID'] = signal_group_id
    df_Adjustment['POSITION_GROUP_ID'] = position_group_id
    df_Adjustment['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
    df_Adjustment['ORDER_TYPE'] = "MARKET"
    df_Adjustment['CONTRACT_TYPE'] = squareOff_dict['contract_type']
    df_Adjustment['EXPIRY_DATE'] = squareOff_dict['expiry_date_futures']
    df_Adjustment['MARGIN_OVERALL'] = Configuration['TOTAL_INITIAL_MARGIN']


    #df_Adjustment['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
    df_Adjustment['EXIT_PRICE'] = 0.0  # populate later from df_Level

    df_Adjustment['IS_SQUARE_OFF'] = False
    df_Adjustment['IS_ACTIVE'] = True

    # STEP 8: MERGE WITH df_Level AND ADD EXIT AND ENTRY PRICES
    isOptionsAdjustmentAllowed = True

    ######################################  POPULATE ADDITIONAL FIELDS #################################################
    df_Adjustment = OptionsAdjustmentHelper.populateFieldsAfterAdjustmentFuturesFresh(df_Adjustment, optionsAdjustment_dict,
                                                                          Configuration)

    # DROP COLUMNS IF REQUIRED
    df_Adjustment.reset_index(drop=True, inplace=True)

    optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = isOptionsAdjustmentAllowed
    optionsAdjustment_dict["df_Adjustment"] = df_Adjustment
    return optionsAdjustment_dict

def populateAdjustmentFuturesSquareoff(Configuration, NUM_LOTS, df_Adjustment, squareOff_dict,expiry_date,
                                                  df_positions_tracking, isOptionsAdjustmentAllowed,
                                                  optionsAdjustment_dict, position_group_id,
                                                  position_tracking_group_id, signal_group_id,
                                                  signal_id, symbol, instrument_type, transaction_type, instruction_type):
    df_Adjustment = squareOff_dict['df_futures_existing']
    df_Adjustment.columns = [x.upper() for x in df_Adjustment.columns]
    df_Adjustment['STRIKE_PRICE'] = int(squareOff_dict['future_price_current'])
    df_Adjustment['INSTRUMENT_TYPE'] = instrument_type
    df_Adjustment['CURRENT_PRICE'] = squareOff_dict['future_price_current']
    df_Adjustment['EXIT_PRICE'] = squareOff_dict['future_price_current']
    df_Adjustment['DELTA'] = 0.0  # populate later from df_Level
    df_Adjustment['NUM_LOTS'] = NUM_LOTS
    df_Adjustment['INSTRUCTION_TYPE'] = instruction_type
    df_Adjustment['IS_ACTIVE'] = True
    df_Adjustment['SIGNAL_ID'] = signal_id
    df_Adjustment['SIGNAL_GROUP_ID'] = signal_group_id
    df_Adjustment['POSITION_GROUP_ID'] = position_group_id
    df_Adjustment['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
    df_Adjustment['ORDER_TYPE'] = "MARKET"
    df_Adjustment['CONTRACT_TYPE'] = squareOff_dict['contract_type']
    df_Adjustment['EXPIRY_DATE'] = squareOff_dict['expiry_date_futures']
    df_Adjustment['MARGIN_OVERALL'] = Configuration['TOTAL_INITIAL_MARGIN']
    df_Adjustment['UNREALIZED_PNL'] = 0.0
    df_Adjustment['UNREALIZED_PNL_GROUP'] = 0.0
    df_Adjustment['ENTRY_DELTA'] = 0.0
    df_Adjustment['ENTRY_NET_DELTA'] = 0.0
    df_Adjustment['CURRENT_DELTA'] = 0.0

    df_Adjustment['IS_SQUARE_OFF'] = True
    df_Adjustment['IS_ACTIVE'] = False

    # STEP 8: MERGE WITH df_Level AND ADD EXIT AND ENTRY PRICES
    isOptionsAdjustmentAllowed = True

    ######################################  POPULATE ADDITIONAL FIELDS #################################################
    df_Adjustment = OptionsAdjustmentHelper.populateFieldsAfterAdjustmentFuturesFresh(df_Adjustment, optionsAdjustment_dict,
                                                                          Configuration)

    # DROP COLUMNS IF REQUIRED
    df_Adjustment.reset_index(drop=True, inplace=True)

    optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = isOptionsAdjustmentAllowed
    optionsAdjustment_dict["df_Adjustment"] = df_Adjustment
    return optionsAdjustment_dict


# def populateCallShift(Configuration, NUM_LOTS, df_Adjustment, df_Level, df_positions_tracking,
#                       isOptionsAdjustmentAllowed, optionsAdjustment_dict, position_group_id, position_tracking_group_id,
#                       signal_group_id, signal_id, symbol, isIronFlyMainAdjustmentsDone,
#                                                                       isStrategyBecomeInvertedFly):
#     isOptionsAdjustmentAllowed = False
#
#     # STEP 1: FETCH PUT DELTA FROM EXISTING POSITIONS, SHIFT CALL POSITIONS SAME AS CURRENT PUT DELTA
#     QUANTITY = int(NUM_LOTS * float(Configuration['LOT_SIZE_' + symbol]))
#     df_Adjustment['STRIKE_PRICE'] = df_positions_tracking['strike_price'].astype(int)
#     df_Adjustment['POSITION_ID'] = df_positions_tracking['position_id']
#     df_Adjustment['INSTRUMENT_TYPE'] = df_positions_tracking.apply(lambda row: row['instrument_type'].value, axis=1)
#     df_Adjustment['TRANSACTION_TYPE'] = df_positions_tracking.apply(lambda row: row['transaction_type'].value, axis=1)
#     df_Adjustment['ENTRY_PRICE'] = df_positions_tracking['entry_price']
#     df_Adjustment['EXIT_PRICE'] = 0.0  # populate later from df_Level
#     df_Adjustment['DELTA'] = 0.0  # populate later from df_Level
#     df_Adjustment['MULTI_FACTOR'] = NUM_LOTS
#     df_Adjustment['QUANTITY'] = QUANTITY
#     df_Adjustment['INSTRUCTION_TYPE'] = "SQUAREOFF_ALL"
#     df_Adjustment['IS_ACTIVE'] = False
#     df_Adjustment['SIGNAL_ID'] = signal_id
#     df_Adjustment['SIGNAL_GROUP_ID'] = signal_group_id
#     df_Adjustment['POSITION_GROUP_ID'] = position_group_id
#     df_Adjustment['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
#     PUT_SELL_SP = int(df_positions_tracking[(df_positions_tracking['instrument_type'] == InstrumentTypeEnum.PUT) &
#                                             (df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL)][
#                           'strike_price'].iloc[0])
#     PUT_BUY_SP = int(df_positions_tracking[(df_positions_tracking['instrument_type'] == InstrumentTypeEnum.PUT) &
#                                            (df_positions_tracking['transaction_type'] == TransactionTypeEnum.BUY)][
#                          'strike_price'].iloc[0])
#     PUT_SELL_SP_DELTA_CALL_SHIFT = df_Level[df_Level['STRIKE_PRICE'] == PUT_SELL_SP]['DELTA_PUT_SELL'].iloc[0]
#     PUT_BUY_SP_DELTA_CALL_SHIFT = abs(df_Level[df_Level['STRIKE_PRICE'] == PUT_BUY_SP]['DELTA_PUT_BUY'].iloc[0])
#     ####################################### MOCK DELTA FOR LOCAL TESTING ###############################################
#     from testing.TestGenericOrderGen import MOCK_PROPERTIES
#     if utils.isMockPrdEnv(Configuration) or \
#             utils.isLocalPrdEnv(Configuration) or \
#             utils.isLocalDevEnv(Configuration) or \
#             utils.isMockDevEnv(Configuration):
#         PUT_SELL_SP_DELTA_CALL_SHIFT = MOCK_PROPERTIES["PUT_SELL_SP_DELTA_CALL_SHIFT"] * NUM_LOTS
#         PUT_BUY_SP_DELTA_CALL_SHIFT = MOCK_PROPERTIES["PUT_BUY_SP_DELTA_CALL_SHIFT"] * NUM_LOTS
#     ####################################################################################################################
#
#     # STEP 2: REMOVE PUT ROWS AS WE NEED TO ADJUST ONLY CALL SIDE,
#     #         CALL ROWS WITH EXISTING POSITION WHICH WE NEED TO SQUAREOFF
#     df_Adjustment_Original = df_Adjustment.copy()
#     df_Adjustment_Original['ORDER_MANIFEST'] = df_positions_tracking['order_manifest']
#     df_Adjustment = df_Adjustment[df_Adjustment['INSTRUMENT_TYPE'] == 'CALL']
#
#     # STEP 3: FETCH CALL SELL WITH DELTA LESS THAN PUT_SELL_SP_DELTA
#     # CALL SELL AND PUT BUY DELTA WILL BE ALWAYS NEGATIVE, SO TAKE CARE IN COMPARISONS
#     df_Call_Sell = df_Level[df_Level['DELTA_CALL_SELL'].abs() < abs(PUT_SELL_SP_DELTA_CALL_SHIFT)]
#     df_Call_Sell = df_Call_Sell.head(1)
#
#     # STEP 4: CHECK FOR IRON FLY AND INVERTED FLY
#     optionsAdjustment_dict = OptionsAdjustmentHelper.checkForIronFlyOrInvertedFly('CALL', df_Adjustment_Original, df_Call_Sell,
#                                                                Configuration, symbol, optionsAdjustment_dict)
#
#     # STEP 5: IF STRATEGY BECOME INVERTED FLY, NO MORE MAIN ADJUSTMENTS
#     if optionsAdjustment_dict['isStrategyBecomeInvertedFly']:
#         optionsAdjustment_dict['df_Adjustment'] = None
#         # TODO IF INVERTED FLY , DO WE NEED TO START ADDING SPREADS
#         optionsAdjustment_dict['isOptionsAdjustmentAllowed'] = False
#         return optionsAdjustment_dict
#
#     # STEP 6: POPULATE CALL SELL WITH DELTA LESS THAN PUT_SELL_SP_DELTA
#     df_Call_Adjustment_Sell = df_Call_Sell[['STRIKE_PRICE']]
#     df_Call_Adjustment_Sell['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
#     df_Call_Adjustment_Sell['INSTRUMENT_TYPE'] = 'CALL'
#     df_Call_Adjustment_Sell['TRANSACTION_TYPE'] = 'SELL'
#     df_Call_Adjustment_Sell['ENTRY_PRICE'] = 0.0  # populate later from df_Level
#     df_Call_Adjustment_Sell['EXIT_PRICE'] = 0.0  # populate later from df_Level
#     df_Call_Adjustment_Sell['DELTA'] = 0.0  # populate later from df_Level
#     df_Call_Adjustment_Sell['MULTI_FACTOR'] = NUM_LOTS
#     df_Call_Adjustment_Sell['QUANTITY'] = QUANTITY
#     df_Call_Adjustment_Sell['INSTRUCTION_TYPE'] = "FRESH_NEW"
#     df_Call_Adjustment_Sell['IS_ACTIVE'] = True
#     df_Call_Adjustment_Sell['SIGNAL_ID'] = signal_id
#     df_Call_Adjustment_Sell['SIGNAL_GROUP_ID'] = signal_group_id
#     df_Call_Adjustment_Sell['POSITION_GROUP_ID'] = position_group_id
#     df_Call_Adjustment_Sell['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
#     df_Call_Adjustment_Sell.reset_index(drop=True, inplace=True)
#     df_Adjustment = pd.concat([df_Adjustment, df_Call_Adjustment_Sell], ignore_index=True)
#
#     # STEP 7: FETCH CALL BUY WITH DELTA CLOSE TO PUT_BUY_SP_DELTA
#     idx = df_Level['DELTA_CALL_BUY'].sub(PUT_BUY_SP_DELTA_CALL_SHIFT).abs().idxmin()
#     df_Call_Buy = df_Level.loc[[idx]]
#     df_Call_Adjustment_Buy = df_Call_Buy[['STRIKE_PRICE']]
#     df_Call_Adjustment_Buy['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
#     df_Call_Adjustment_Buy['INSTRUMENT_TYPE'] = 'CALL'
#     df_Call_Adjustment_Buy['TRANSACTION_TYPE'] = 'BUY'
#     df_Call_Adjustment_Buy['ENTRY_PRICE'] = 0.0  # populate later from df_Level
#     df_Call_Adjustment_Buy['EXIT_PRICE'] = 0.0  # populate later from df_Level
#     df_Call_Adjustment_Buy['DELTA'] = 0.0  # populate later from df_Level
#     df_Call_Adjustment_Buy['MULTI_FACTOR'] = NUM_LOTS
#     df_Call_Adjustment_Buy['QUANTITY'] = QUANTITY
#     df_Call_Adjustment_Buy['INSTRUCTION_TYPE'] = "FRESH_NEW"
#     df_Call_Adjustment_Buy['IS_ACTIVE'] = True
#     df_Call_Adjustment_Buy['SIGNAL_ID'] = signal_id
#     df_Call_Adjustment_Buy['SIGNAL_GROUP_ID'] = signal_group_id
#     df_Call_Adjustment_Buy['POSITION_GROUP_ID'] = position_group_id
#     df_Call_Adjustment_Buy['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
#     df_Call_Adjustment_Buy.reset_index(drop=True, inplace=True)
#     df_Adjustment = pd.concat([df_Adjustment, df_Call_Adjustment_Buy], ignore_index=True)
#
#     # STEP 8: MERGE WITH df_Level AND ADD EXIT AND ENTRY PRICES
#     isOptionsAdjustmentAllowed = True
#     df_Adjustment = pd.merge(df_Adjustment, df_Level, how='inner', on=['STRIKE_PRICE', 'MULTI_FACTOR'])
#     df_Adjustment = OptionsAdjustmentHelper.reverseTransactionType(df_Adjustment)
#     df_Adjustment = OptionsAdjustmentHelper.populateFieldsAfterAdjustment(df_Adjustment, optionsAdjustment_dict,
#                                                                           Configuration)
#
#     df_Adjustment = df_Adjustment.replace({np.nan: None})
#     optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = isOptionsAdjustmentAllowed
#     optionsAdjustment_dict["df_Adjustment"] = df_Adjustment
#     return optionsAdjustment_dict

# def populatePutShift(Configuration, NUM_LOTS, df_Adjustment, df_Level,
#             df_positions_tracking, isOptionsAdjustmentAllowed,
#             optionsAdjustment_dict, position_group_id,
#             position_tracking_group_id, signal_group_id,
#             signal_id, symbol, isIronFlyMainAdjustmentsDone,
#             isStrategyBecomeInvertedFly):
#     isOptionsAdjustmentAllowed = False
#
#     # STEP 1: FETCH CALL DELTA FROM EXISTING POSITIONS, SHIFT PUT POSITIONS SAME AS CURRENT CALL DELTA
#     QUANTITY = int(NUM_LOTS * float(Configuration['LOT_SIZE_' + symbol]))
#     df_Adjustment['STRIKE_PRICE'] = df_positions_tracking['strike_price'].astype(int)
#     df_Adjustment['POSITION_ID'] = df_positions_tracking['position_id']
#     df_Adjustment['INSTRUMENT_TYPE'] = df_positions_tracking.apply(lambda row: row['instrument_type'].value, axis=1)
#     df_Adjustment['TRANSACTION_TYPE'] = df_positions_tracking.apply(lambda row: row['transaction_type'].value, axis=1)
#     df_Adjustment['ENTRY_PRICE'] = df_positions_tracking['entry_price']
#     df_Adjustment['EXIT_PRICE'] = 0.0  # populate later from df_Level
#     df_Adjustment['DELTA'] = 0.0  # populate later from df_Level
#     df_Adjustment['MULTI_FACTOR'] = NUM_LOTS
#     df_Adjustment['QUANTITY'] = QUANTITY
#     df_Adjustment['INSTRUCTION_TYPE'] = "SQUAREOFF_ALL"
#     df_Adjustment['IS_ACTIVE'] = False
#     df_Adjustment['SIGNAL_ID'] = signal_id
#     df_Adjustment['SIGNAL_GROUP_ID'] = signal_group_id
#     df_Adjustment['POSITION_GROUP_ID'] = position_group_id
#     df_Adjustment['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
#     CALL_SELL_SP = int(df_positions_tracking[(df_positions_tracking['instrument_type'] == InstrumentTypeEnum.CALL) &
#                                             (df_positions_tracking['transaction_type'] == TransactionTypeEnum.SELL)][
#                           'strike_price'].iloc[0])
#     CALL_BUY_SP = int(df_positions_tracking[(df_positions_tracking['instrument_type'] == InstrumentTypeEnum.CALL) &
#                                            (df_positions_tracking['transaction_type'] == TransactionTypeEnum.BUY)][
#                          'strike_price'].iloc[0])
#     CALL_SELL_SP_DELTA_PUT_SHIFT = df_Level[df_Level['STRIKE_PRICE'] == CALL_SELL_SP]['DELTA_CALL_SELL'].iloc[0]
#     CALL_BUY_SP_DELTA_PUT_SHIFT = abs(df_Level[df_Level['STRIKE_PRICE'] == CALL_BUY_SP]['DELTA_CALL_BUY'].iloc[0])
#     ####################################### MOCK DELTA FOR LOCAL TESTING ###############################################
#     if utils.isMockPrdEnv(Configuration) or \
#             utils.isLocalPrdEnv(Configuration) or \
#             utils.isLocalDevEnv(Configuration) or \
#             utils.isMockDevEnv(Configuration):
#         CALL_SELL_SP_DELTA_PUT_SHIFT = MOCK_PROPERTIES["CALL_SELL_SP_DELTA_PUT_SHIFT"] * NUM_LOTS
#         CALL_BUY_SP_DELTA_PUT_SHIFT = MOCK_PROPERTIES["CALL_BUY_SP_DELTA_PUT_SHIFT"] * NUM_LOTS
#     ####################################################################################################################
#
#     # STEP 2: REMOVE PUT ROWS AS WE NEED TO ADJUST ONLY PUT SIDE,
#     #         CALL ROWS WITH EXISTING POSITION WHICH WE NEED TO SQUAREOFF
#     df_Adjustment_Original = df_Adjustment.copy()
#     df_Adjustment_Original['ORDER_MANIFEST'] = df_positions_tracking['order_manifest']
#     df_Adjustment = df_Adjustment[df_Adjustment['INSTRUMENT_TYPE'] == 'PUT']
#
#     # STEP 3: FETCH CALL SELL WITH DELTA LESS THAN PUT_SELL_SP_DELTA
#     # CALL SELL AND PUT BUY DELTA WILL BE ALWAYS NEGATIVE, SO TAKE CARE IN COMPARISONS
#     df_Put_Sell = df_Level[df_Level['DELTA_PUT_SELL'].abs() < abs(CALL_SELL_SP_DELTA_PUT_SHIFT)]
#     df_Put_Sell = df_Put_Sell.tail(1)
#
#     # STEP 4: CHECK FOR IRON FLY AND INVERTED FLY
#     optionsAdjustment_dict = OptionsAdjustmentHelper.checkForIronFlyOrInvertedFly('PUT', df_Adjustment_Original,
#                                                                                   df_Put_Sell,
#                                                                                   Configuration, symbol,
#                                                                                   optionsAdjustment_dict)
#
#     # STEP 5: IF STRATEGY BECOME INVERTED FLY, NO MORE MAIN ADJUSTMENTS
#     if optionsAdjustment_dict['isStrategyBecomeInvertedFly']:
#         optionsAdjustment_dict['df_Adjustment'] = None
#         # TODO IF INVERTED FLY , DO WE NEED TO START ADDING SPREADS
#         optionsAdjustment_dict['isOptionsAdjustmentAllowed'] = False
#         return optionsAdjustment_dict
#
#     # STEP 6: POPULATE PUT SELL WITH DELTA LESS THAN CALL_SELL_SP_DELTA
#     df_Put_Adjustment_Sell = df_Put_Sell[['STRIKE_PRICE']]
#     df_Put_Adjustment_Sell['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
#     df_Put_Adjustment_Sell['INSTRUMENT_TYPE'] = 'PUT'
#     df_Put_Adjustment_Sell['TRANSACTION_TYPE'] = 'SELL'
#     df_Put_Adjustment_Sell['ENTRY_PRICE'] = 0.0  # populate later from df_Level
#     df_Put_Adjustment_Sell['EXIT_PRICE'] = 0.0  # populate later from df_Level
#     df_Put_Adjustment_Sell['DELTA'] = 0.0  # populate later from df_Level
#     df_Put_Adjustment_Sell['MULTI_FACTOR'] = NUM_LOTS
#     df_Put_Adjustment_Sell['QUANTITY'] = QUANTITY
#     df_Put_Adjustment_Sell['INSTRUCTION_TYPE'] = "FRESH_NEW"
#     df_Put_Adjustment_Sell['IS_ACTIVE'] = True
#     df_Put_Adjustment_Sell['SIGNAL_ID'] = signal_id
#     df_Put_Adjustment_Sell['SIGNAL_GROUP_ID'] = signal_group_id
#     df_Put_Adjustment_Sell['POSITION_GROUP_ID'] = position_group_id
#     df_Put_Adjustment_Sell['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
#     df_Put_Adjustment_Sell.reset_index(drop=True, inplace=True)
#     df_Adjustment = pd.concat([df_Adjustment, df_Put_Adjustment_Sell], ignore_index=True)
#
#     # STEP 7: FETCH PUT BUY WITH DELTA CLOSE TO CALL_BUY_SP_DELTA
#     idx = df_Level['DELTA_PUT_BUY'].add(CALL_BUY_SP_DELTA_PUT_SHIFT).abs().idxmin()
#     df_Put_Buy = df_Level.loc[[idx]]
#     df_Put_Adjustment_Buy = df_Put_Buy[['STRIKE_PRICE']]
#     df_Put_Adjustment_Buy['POSITION_ID'] = UniqueKeyGenerator().getUniqueKeyDateTime()
#     df_Put_Adjustment_Buy['INSTRUMENT_TYPE'] = 'PUT'
#     df_Put_Adjustment_Buy['TRANSACTION_TYPE'] = 'BUY'
#     df_Put_Adjustment_Buy['ENTRY_PRICE'] = 0.0  # populate later from df_Level
#     df_Put_Adjustment_Buy['EXIT_PRICE'] = 0.0  # populate later from df_Level
#     df_Put_Adjustment_Buy['DELTA'] = 0.0  # populate later from df_Level
#     df_Put_Adjustment_Buy['MULTI_FACTOR'] = NUM_LOTS
#     df_Put_Adjustment_Buy['QUANTITY'] = QUANTITY
#     df_Put_Adjustment_Buy['INSTRUCTION_TYPE'] = "FRESH_NEW"
#     df_Put_Adjustment_Buy['IS_ACTIVE'] = True
#     df_Put_Adjustment_Buy['SIGNAL_ID'] = signal_id
#     df_Put_Adjustment_Buy['SIGNAL_GROUP_ID'] = signal_group_id
#     df_Put_Adjustment_Buy['POSITION_GROUP_ID'] = position_group_id
#     df_Put_Adjustment_Buy['POSITION_TRACKING_GROUP_ID'] = position_tracking_group_id
#     df_Put_Adjustment_Buy.reset_index(drop=True, inplace=True)
#     df_Adjustment = pd.concat([df_Adjustment, df_Put_Adjustment_Buy], ignore_index=True)
#
#     # STEP 8: MERGE WITH df_Level AND ADD EXIT AND ENTRY PRICES
#     isOptionsAdjustmentAllowed = True
#     df_Adjustment = pd.merge(df_Adjustment, df_Level, how='inner', on=['STRIKE_PRICE', 'MULTI_FACTOR'])
#     df_Adjustment = OptionsAdjustmentHelper.reverseTransactionType(df_Adjustment)
#     df_Adjustment = OptionsAdjustmentHelper.populateFieldsAfterAdjustment(df_Adjustment, optionsAdjustment_dict,
#                                                                           Configuration)
#
#     df_Adjustment = df_Adjustment.replace({np.nan: None})
#     optionsAdjustment_dict["isOptionsAdjustmentAllowed"] = isOptionsAdjustmentAllowed
#     optionsAdjustment_dict["df_Adjustment"] = df_Adjustment
#     optionsAdjustment_dict["isIronFlyMainAdjustmentsDone"] = isIronFlyMainAdjustmentsDone
#     optionsAdjustment_dict["isStrategyBecomeInvertedFly"] = isStrategyBecomeInvertedFly
#     return optionsAdjustment_dict













