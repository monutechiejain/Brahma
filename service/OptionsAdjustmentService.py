from helper import OrderGenOptionsHelper, OptionsAdjustmentHelper
from rules import OptionsAdjustmentRuleEngine
import logging
from dao import PositionsDAO
from datetime import datetime
import pytz

def orderGenOptionsAdjustment(**kwargs):
    symbol = kwargs['symbol']
    contract_type = kwargs['contract_type']
    Configuration = kwargs['Configuration']
    optionsAdjustment_dict = {}
    optionsAdjustment_dict['isRunJobWithoutDelay'] = False
    optionsAdjustment_dict['symbol'] = symbol
    position_group_id = kwargs['position_group_id']
    optionsAdjustment_dict['position_group_id'] = position_group_id
    optionsAdjustment_dict['isDealAllowed'] = False
    optionsAdjustment_dict['contract_type'] = contract_type

    squareOff_dict = kwargs['squareOff_dict']
    spot_value = squareOff_dict['spot_value']
    optionsAdjustment_dict['df_positions_tracking'] = squareOff_dict['df_positions_tracking']
    optionsAdjustment_dict["spot_value"] = spot_value
    optionsAdjustment_dict['time_to_expiry_options'] = squareOff_dict['time_to_expiry_options']
    optionsAdjustment_dict['expiry_date'] = squareOff_dict['expiry_date']
    optionsAdjustment_dict['params'] = squareOff_dict['params']
    optionsAdjustment_dict['schema_name'] = squareOff_dict['schema_name']
    expiry_date_options, expiry_date_futures = OrderGenOptionsHelper.callExpiryDateAPI(Configuration, contract_type)

    # POPULATE PNL_OVERALL IN THE DICT
    df_positions_tracking = optionsAdjustment_dict['df_positions_tracking']
    NET_PNL_OVERALL = df_positions_tracking['net_pnl_overall'].iloc[0]
    REALIZED_PNL_OVERALL = df_positions_tracking['realized_pnl_overall'].iloc[0]
    REALIZED_PNL_GROUP = df_positions_tracking['realized_pnl_group'].iloc[0]
    UNREALIZED_PNL_GROUP = df_positions_tracking['unrealized_pnl_group'].iloc[0]
    MARGIN_OVERALL = df_positions_tracking['margin_overall'].iloc[0]
    optionsAdjustment_dict["NET_PNL_OVERALL"] = NET_PNL_OVERALL
    optionsAdjustment_dict["REALIZED_PNL_OVERALL"] = REALIZED_PNL_OVERALL
    optionsAdjustment_dict["REALIZED_PNL_GROUP"] = REALIZED_PNL_GROUP
    optionsAdjustment_dict["UNREALIZED_PNL_GROUP"] = UNREALIZED_PNL_GROUP
    optionsAdjustment_dict["MARGIN_OVERALL"] = MARGIN_OVERALL

    ####################################### 1. POPULATE ADJUSMENT DATAFRAME ###################################################
    optionsAdjustment_dict = OptionsAdjustmentRuleEngine.decisonMaker(optionsAdjustment_dict, squareOff_dict, Configuration, symbol)

    # ADJUSTMENT STATUS IN SQUAREOFF DICT - USED IN TRACKER TABLE
    squareOff_dict["isCallAdjustmentSquareOffDone"] = optionsAdjustment_dict["isCallAdjustmentSquareOffDone"]
    squareOff_dict["isPutAdjustmentSquareOffDone"] = optionsAdjustment_dict["isPutAdjustmentSquareOffDone"]
    squareOff_dict["isFutBuyFreshPositionTaken"] = optionsAdjustment_dict["isFutBuyFreshPositionTaken"]
    squareOff_dict["isFutSellFreshPositionTaken"] = optionsAdjustment_dict["isFutSellFreshPositionTaken"]
    squareOff_dict["isFutBuyAdjustmentSquareOffDone"] = optionsAdjustment_dict["isFutBuyAdjustmentSquareOffDone"]
    squareOff_dict["isFutSellAdjustmentSquareOffDone"] = optionsAdjustment_dict["isFutSellAdjustmentSquareOffDone"]

    if not optionsAdjustment_dict['isOptionsAdjustmentAllowed']:
        return optionsAdjustment_dict, squareOff_dict

    df_Adjustment = optionsAdjustment_dict['df_Adjustment']
    expiry_date = optionsAdjustment_dict['expiry_date']

    time_to_expiry_options_252, time_to_expiry_options_annualized_252, \
    time_to_expiry_options_365, time_to_expiry_options_annualized_365 = OrderGenOptionsHelper.populateTimeToExpiry(
        expiry_date, Configuration)
    df_Adjustment['DAYS_IN_YEAR'] = '252'
    df_Adjustment['current_time_to_expiry'] = time_to_expiry_options_252

    ######################################## 2. PLACE DEALS AND POPULATE DB ############################################
    optionsAdjustment_dict = OptionsAdjustmentHelper.placeDealsAndPopulateDBOptions(position_group_id, optionsAdjustment_dict, Configuration, df_Adjustment,
                                                         symbol, expiry_date, time_to_expiry_options_252, time_to_expiry_options_365,
                                                         spot_value, expiry_date_futures)

    ###################################### 3. CHECK IF ALL POSITIONS SQUAREOFF #########################################
    df_positions_existing = PositionsDAO.getActivePositionsBySymbolAndContractType(squareOff_dict['schema_name'], symbol, contract_type)
    if len(df_positions_existing) == 0:
        squareOff_dict['isSquareOff'] = True
        current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')
        squareOff_dict['current_time'] = current_time

        # UPDATE NET_PNL_OVERALL FOR TRACKER POST FINAL ADJUSTMENT
        df_positions_tracking = squareOff_dict['df_positions_tracking']
        df_positions_tracking['net_pnl_overall'] = optionsAdjustment_dict['REALIZED_PNL_OVERALL']
        squareOff_dict['df_positions_tracking'] = df_positions_tracking
        print("Function:orderGenOptionsAdjustment, {} : ALL POSITIONS SQUAREDOFF POST ADJUSTMENT..............".format(symbol))
        logging.info("Function:orderGenOptionsAdjustment, {} :  ALL POSITIONS SQUAREDOFF POST ADJUSTMENT..............".format(symbol))
        return optionsAdjustment_dict, squareOff_dict
    ####################################################################################################################


    return optionsAdjustment_dict, squareOff_dict