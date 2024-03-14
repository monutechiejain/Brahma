
import math


def calculateNumLotsMarginAPI(Configuration, symbol, orderGen_dict):
    ########################################## ONLY FOR PROD ###########################################################
    # THIS FUNCTION IS USED IN ORDER GEN AND ADJUSTMENT RULE ENGINE AS WELL
    if not (Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']) and Configuration['IS_MARGIN_CALL_OVERRIDE'] == 'Y':
        orderGen_dict["MF_STEP_SIZE"] = float(Configuration['MF_STEP_SIZE_FRESH_' + symbol])
    else:
        TOTAL_INITIAL_MARGIN = float(Configuration['TOTAL_INITIAL_MARGIN'])
        orderGen_dict["MF_PER_LOT"] = float(Configuration['MF_PER_LOT_' + symbol])
        orderGen_dict["MF_STEP_SIZE"] = math.floor(TOTAL_INITIAL_MARGIN/orderGen_dict["MF_PER_LOT"])

    return orderGen_dict