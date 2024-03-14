
from common import utils

def getMarginProfileDetails(Configuration):
    ################################################ GET AVAILABLE MARGIN CALL #########################################
    OM_URL = Configuration['OM_URL']
    marginProfileResponse = utils.getOMServiceCallHelper(Configuration).getMarginProfile(Configuration, OM_URL)

    ###############################################  POPULATE AVAILABLE MARGIN #########################################
    margin_profile_dict = {}
    margin_profile_dict['live_balance'] = float(marginProfileResponse['net'])
    margin_profile_dict['opening_balance'] = float(marginProfileResponse['available']['opening_balance'])+\
                      float(marginProfileResponse['available']['collateral'])
    margin_profile_dict['utilised'] = float(marginProfileResponse['utilised']['debits'])

    return margin_profile_dict