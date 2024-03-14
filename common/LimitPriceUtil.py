
import logging
from common import utils

#################################################### FETCH LIMIT PRICES ################################################
def getLimitPrice(order_type, bid_ask_price, Configuration):

    LIMIT_PRICE_HAIRCUT_PCT_THRESHOLD = float(Configuration['LIMIT_PRICE_HAIRCUT_PCT_THRESHOLD'])
    LIMIT_PRICE_HAIRCUT_MIN_VALUE = float(Configuration['LIMIT_PRICE_HAIRCUT_MIN_VALUE'])
    LIMIT_PRICE_MIN_DEFAULT = float(Configuration['LIMIT_PRICE_MIN_DEFAULT'])

    LIMIT_PRICE_HAIRCUT_PCT_THRESHOLD_VALUE = max(LIMIT_PRICE_HAIRCUT_MIN_VALUE,
                                                  utils.xPercentageOfY(LIMIT_PRICE_HAIRCUT_PCT_THRESHOLD, bid_ask_price))

    ###################################### CALCULATE LIMIT PRICE #######################################################
    # IN CASE OF SELL , PRICE SHOULD BE LESS THAN LTP
    # IN CASE OF BUY , PRICE SHOULD BE GREATER TAN LTP
    if order_type == 'SELL':
        LIMIT_PRICE = bid_ask_price - LIMIT_PRICE_HAIRCUT_PCT_THRESHOLD_VALUE
    else:
        LIMIT_PRICE = bid_ask_price + LIMIT_PRICE_HAIRCUT_PCT_THRESHOLD_VALUE

    # IF LIMIT PRICE LESS THAN 0, Default it to 0.1
    if LIMIT_PRICE <= 0:
        LIMIT_PRICE = LIMIT_PRICE_MIN_DEFAULT

    # ROUNDING
    LIMIT_PRICE = round(LIMIT_PRICE, 1)

    logging.info("{}: LIMIT PRICE UTIL: bid_ask_price : {}, LIMIT_PRICE : {}, order_type : {} ".format(Configuration['SCHEMA_NAME'], bid_ask_price, LIMIT_PRICE, order_type))

    return LIMIT_PRICE



