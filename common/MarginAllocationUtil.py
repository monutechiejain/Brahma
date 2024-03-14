from common import utils
import logging
from dao import PositionsDAO

def getMarginAllocation(Configuration, symbol, df_Put_Call, expiry_date, orderGen_dict, position_group_id):
    MARGIN_UTILISED_PCT_THRESHOLD = float(Configuration['MARGIN_UTILISED_PCT_THRESHOLD_' + symbol])

    ################################################# NIFTY ALLOCATION #################################################
    if symbol == 'NIFTY':
        return MARGIN_UTILISED_PCT_THRESHOLD

    ##########################################  BANKNIFTY ALLOCATION POST REALIGNMENT ##################################
    if orderGen_dict['order_gen_type'] == 'REALIGNED' and symbol == 'BANKNIFTY':
        # CHECK IF NIFTY ACTIVE POSITIONS
        df_positions_existing_NIFTY = PositionsDAO.getActivePositionsByTransIdAndSymbol(position_group_id, "NIFTY")
        if len(df_positions_existing_NIFTY) > 0:
            return MARGIN_UTILISED_PCT_THRESHOLD

    ################################################ BANKNIFTY ALLOCATION ###############################################
    MARGIN_UTILISED_PCT_THRESHOLD_1 = float(Configuration['MARGIN_UTILISED_PCT_THRESHOLD_1_' + symbol])
    MARGIN_UTILISED_PCT_THRESHOLD_2 = float(Configuration['MARGIN_UTILISED_PCT_THRESHOLD_2_' + symbol])
    MARGIN_UTILISED_PCT_THRESHOLD_3 = float(Configuration['MARGIN_UTILISED_PCT_THRESHOLD_3_' + symbol])
    MARGIN_UTILISED_IV_THRESHOLD_NTH_1 = float(Configuration['MARGIN_UTILISED_IV_THRESHOLD_NTH_1_' + symbol])
    MARGIN_UTILISED_IV_THRESHOLD_NTH_2 = float(Configuration['MARGIN_UTILISED_IV_THRESHOLD_NTH_2_' + symbol])
    MARGIN_UTILISED_IV_THRESHOLD_TH_1 = float(Configuration['MARGIN_UTILISED_IV_THRESHOLD_TH_1_' + symbol])
    MARGIN_UTILISED_IV_THRESHOLD_TH_2 = float(Configuration['MARGIN_UTILISED_IV_THRESHOLD_TH_2_' + symbol])

    df_Put_Call_Margin_Allocation = df_Put_Call.copy()
    # ############################################ GET IVS AND CALCULATE AVG #############################################
    IV_PUT = float(df_Put_Call_Margin_Allocation[df_Put_Call_Margin_Allocation['order_type'] == 'SELL']['iv_put'].iloc[0])
    IV_CALL = float(df_Put_Call_Margin_Allocation[df_Put_Call_Margin_Allocation['order_type'] == 'SELL']['iv_call'].iloc[0])
    AVG_IV = (IV_PUT+IV_CALL)/2

    # MOCK DATA
    #AVG_IV = 0.1299

    ################################################## MARGIN ALLOCATION RULES #########################################
    # 1. BASED ON IV
    # 2. BASED ON THURSDAYS or NON-THURSDAYS
    if not utils.checkIfThursday() and AVG_IV < MARGIN_UTILISED_IV_THRESHOLD_NTH_1:
        MARGIN_UTILISED_PCT_THRESHOLD =  MARGIN_UTILISED_PCT_THRESHOLD_1
    elif not utils.checkIfThursday() and MARGIN_UTILISED_IV_THRESHOLD_NTH_1 < AVG_IV < MARGIN_UTILISED_IV_THRESHOLD_NTH_2:
        MARGIN_UTILISED_PCT_THRESHOLD = MARGIN_UTILISED_PCT_THRESHOLD_2
    elif not utils.checkIfThursday() and AVG_IV > MARGIN_UTILISED_IV_THRESHOLD_NTH_2:
        MARGIN_UTILISED_PCT_THRESHOLD =  MARGIN_UTILISED_PCT_THRESHOLD_3
    elif utils.checkIfThursday() and AVG_IV < MARGIN_UTILISED_IV_THRESHOLD_TH_1:
        MARGIN_UTILISED_PCT_THRESHOLD =  MARGIN_UTILISED_PCT_THRESHOLD_1
    elif utils.checkIfThursday() and MARGIN_UTILISED_IV_THRESHOLD_TH_1 < AVG_IV < MARGIN_UTILISED_IV_THRESHOLD_TH_2:
        MARGIN_UTILISED_PCT_THRESHOLD = MARGIN_UTILISED_PCT_THRESHOLD_2
    elif utils.checkIfThursday() and AVG_IV > MARGIN_UTILISED_IV_THRESHOLD_TH_2:
        MARGIN_UTILISED_PCT_THRESHOLD =  MARGIN_UTILISED_PCT_THRESHOLD_3

    logging.info("Inside Margin Allocation : AVG_IV : {}, MARGIN_UTILISED_PCT_THRESHOLD: {}".format(AVG_IV, MARGIN_UTILISED_PCT_THRESHOLD))

    return MARGIN_UTILISED_PCT_THRESHOLD