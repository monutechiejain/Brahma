from common import utils as parent_utils, MarginUtil
import math
import logging
from dao import ClientConfigurationDAO

def decisonMaker(marginFreeUp_dict, Configuration, symbol):
    marginFreeUp_dict['isMarginFreeUpAllowed'] = False

    ################################################ GET AVAILABLE MARGIN CALL #########################################
    margin_profile_dict = MarginUtil.getMarginProfileDetails(Configuration)

    ###############################################  POPULATE AVAILABLE MARGIN #########################################
    live_balance = margin_profile_dict['live_balance']
    opening_balance = margin_profile_dict['live_balance'] + margin_profile_dict['utilised']
    marginFreeUp_dict['live_balance'] = margin_profile_dict['live_balance']
    marginFreeUp_dict['utilised'] = margin_profile_dict['utilised']

    MIN_MARGIN_CHK_PCT = float(Configuration['MIN_MARGIN_CHK_PCT'])
    MARGIN_PER_MF = float(Configuration['MF_PER_LOT_'+symbol])
    opening_balance_min_pct = parent_utils.xPercentageOfY(MIN_MARGIN_CHK_PCT, opening_balance)

    ############## IF LIVE BALANCE IS LESS THAN OPENING BALANCE MIN PCT - SQUAREOFF SOME PORTION OF ATM SP #############
    if live_balance < opening_balance_min_pct:
        free_up_margin_required = opening_balance_min_pct - live_balance
        num_lots_squareoff_required = int(math.ceil(free_up_margin_required/MARGIN_PER_MF))

        #############################################  SET PARAMETERS ##################################################
        marginFreeUp_dict['isMarginFreeUpAllowed'] = True
        marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] = num_lots_squareoff_required
        logging.info("MARGIN FREE UP CHECK INITIATED, live_balance: {}, opening_balance: {}, "
                     "opening_balance_min_pct: {}, num_lots_squareoff_required: {}".format(str(live_balance),
                                        str(opening_balance), str(opening_balance_min_pct), str(num_lots_squareoff_required)))


    #################################### PARTIAL SUAREOFF STARTS #######################################################
    if Configuration['IS_PARTIAL_SQUAREOFF_'+symbol] == 'Y':

        # FETCH CONFIG
        TOTAL_INITIAL_MARGIN = marginFreeUp_dict['TOTAL_INITIAL_MARGIN']
        MF_EXISTING = marginFreeUp_dict['MF_EXISTING']
        PARTIAL_SQUAREOFF_PCT_THRESHOLD = float(Configuration['PARTIAL_SQUAREOFF_PCT_THRESHOLD_'+symbol])

        MULTI_FACTOR_MARGIN_FREE_UP = int(math.floor(parent_utils.xPercentageOfY(PARTIAL_SQUAREOFF_PCT_THRESHOLD, MF_EXISTING)))
        MF_PENDING = MF_EXISTING - MULTI_FACTOR_MARGIN_FREE_UP
        if MF_PENDING >= 1:

            marginFreeUp_dict['isMarginFreeUpAllowed'] = True
            marginFreeUp_dict['MULTI_FACTOR_MARGIN_FREE_UP'] = MULTI_FACTOR_MARGIN_FREE_UP

            # RESET MANUAL OVERRIDE FLAG
            ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'IS_PARTIAL_SQUAREOFF_'+symbol, 'N')
            #ConfigurationDAO.updateConfiguration('TOTAL_INITIAL_MARGIN', str(int(MARGIN_PENDING)))
            logging.info("PARTIAL SQUAREOFF TRIGGERED FOR SYMBOL : {}, SQUARING OFF NUM_LOTS : {}, "
                         "PARTIAL_SQUAREOFF_PCT_THRESHOLD: {}, MULTI_FACTOR_MARGIN_FREE_UP : {}, MF_PENDING POST PARTIAL SQUAREOFF : {}".
                         format(symbol, MULTI_FACTOR_MARGIN_FREE_UP, PARTIAL_SQUAREOFF_PCT_THRESHOLD,MULTI_FACTOR_MARGIN_FREE_UP,
                                MF_PENDING))
        else:
            ClientConfigurationDAO.updateConfiguration(Configuration['SCHEMA_NAME'], 'IS_PARTIAL_SQUAREOFF_' + symbol, 'N')
            logging.info("PARTIAL SQUAREOFF CAN NOT BE TRIGGERED FOR SYMBOL : {} "
                         "AS WE NEED TO MAINTAIN A SET OF MINIMUM LOTS TO RUN STRATEGY, "
                         "MULTI_FACTOR_MARGIN_FREE_UP : {}, MF_PENDING POST PARTIAL SQUAREOFF : {}"
                         .format(symbol,MULTI_FACTOR_MARGIN_FREE_UP, MF_PENDING))


    return marginFreeUp_dict













