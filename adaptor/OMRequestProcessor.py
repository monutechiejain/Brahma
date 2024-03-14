import pandas as pd
import logging
from common import constants, LimitPriceUtil
from datetime import datetime
import json
import pytz
from helper import  QtyFreezeHelper

def populateOMRequest(Configuration, df_Put_Call_Order, expiry_date_futures, expiry_date_options, isFresh,
                      isHedging, isSquareOff, position_group_id, orderType, symbol, tradeType, isMarginFreeUp, isQtyFreeze):

    df_Order, order_manifest, isOrderTypeMarket = createOrderDataFrame(df_Put_Call_Order, symbol, expiry_date_options,
                                         expiry_date_futures, isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze, Configuration)
    isStaticHedging = False
    if orderType == "STATIC_HEDGING":
        isStaticHedging = True
    # margin_txn = row_order['MARGIN_TXN']
    margin_txn = 1.0

    ############################ MARGIN FREE UP/SQUAREOFF - DONT FAIL FOR NEGATIVE MARGIN in OM ########################
    if isMarginFreeUp or isSquareOff or isStaticHedging:
        margin_txn = -10000000.0

    order_dict = createOrderRequest(df_Order, order_manifest, Configuration, orderType,
                                         margin_txn, tradeType, position_group_id, isOrderTypeMarket)
    return df_Order, order_dict


def createOrderDataFrame(df_Put_Call_Order, symbol, expiry_date_options,
                                         expiry_date_futures, isFresh, isHedging, isSquareOff, isMarginFreeUp, isQtyFreeze, Configuration):
    order_dict = {}
    df_Order = pd.DataFrame()
    order_manifest = ""
    isOrderTypeMarket = True


    if isFresh:
        df_Order, order_manifest = createOrderDataFrameFresh(df_Put_Call_Order, expiry_date_futures, expiry_date_options, order_dict,
                                             symbol, Configuration)
        isOrderTypeMarket=False
    elif isHedging:
        df_Order, order_manifest = createOrderDataFrameHedged(df_Put_Call_Order, expiry_date_futures,
                                                             expiry_date_options, order_dict,
                                                             symbol, Configuration)
    elif isSquareOff and not isQtyFreeze:
        df_Order, order_manifest = createOrderDataFrameSquareoff(df_Put_Call_Order, expiry_date_futures,
                                                              expiry_date_options, order_dict,
                                                              symbol, Configuration)
        isOrderTypeMarket = False
    elif isMarginFreeUp:
        df_Order, order_manifest = createOrderDataFrameMarginFreeUp(df_Put_Call_Order, expiry_date_futures,
                                                              expiry_date_options, order_dict,
                                                              symbol)
    elif isSquareOff and isQtyFreeze:
        df_Order, order_manifest = createOrderDataFrameSquareoffQtyFreeze(df_Put_Call_Order, expiry_date_futures,
                                                              expiry_date_options, order_dict,
                                                              symbol, Configuration)
        isOrderTypeMarket = False
    else:
        raise AttributeError("Order can't be processed as all 3 attributes: isFresh/isHedging/isSquareOff are False !!")

    return df_Order, order_manifest, isOrderTypeMarket

def createOrderDataFrameHedged(df_Put_Call_Order, expiry_date_futures, expiry_date_options, order_dict, symbol, Configuration):
    # INITIALIZE ORDER LIST
    order_list = []
    order_manifest = ""

    ####################################################################################################################
    for index, row_order in df_Put_Call_Order.iterrows():
        order_manifest = row_order['ORDER_MANIFEST']
        strike_price = str(int(row_order['STRIKE_PRICE']))
        futures_symbol, put_symbol, call_symbol = populateTradingSymbol(symbol, expiry_date_options,
                                                                        expiry_date_futures, strike_price)

        # POPULATE SYMBOL
        if row_order['INSTRUMENT_TYPE'].value == 'PUT':
            trading_symbol = put_symbol
        elif row_order['INSTRUMENT_TYPE'].value == 'CALL':
            trading_symbol = call_symbol
        else:
            trading_symbol = futures_symbol

        # Add it to Order List
        order_list.extend(
            [{'TRADING_SYMBOL': trading_symbol, 'ORDER_TYPE': row_order['TRANSACTION_TYPE'].value,
              'QUANTITY': row_order['QUANTITY'],
              'INSTRUMENT_TYPE': row_order['INSTRUMENT_TYPE'].value,
              'TAG': populateTag(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE'].value),
                                 row_order['TRANSACTION_TYPE'].value),
              'TORDERID': populateTOrderId(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE'].value),
                                           row_order['TRANSACTION_TYPE'].value),
              'ORDER_TYPE_MARKET_LIMIT': 'LIMIT',
              'PRICE': LimitPriceUtil.getLimitPrice(row_order['TRANSACTION_TYPE'].value,
                                                    row_order['CURRENT_PRICE'],
                                                    Configuration)
              }])

        # ADD TRADING SYMBOL IN MASTER DATAFRAME
        df_Put_Call_Order.at[index, 'TRADING_SYMBOL'] = trading_symbol
        ################################################################################################################

    df_Order = pd.DataFrame(order_list)
    df_Order['QUANTITY'] = pd.to_numeric(df_Order['QUANTITY'], downcast='signed')
    return df_Order, order_manifest

def createOrderDataFrameSquareoff(df_Put_Call_Order, expiry_date_futures, expiry_date_options, order_dict, symbol, Configuration):
    # INITIALIZE ORDER LIST
    order_list = []
    order_manifest = ""

    ####################################################################################################################
    for index, row_order in df_Put_Call_Order.iterrows():
        order_manifest = row_order['ORDER_MANIFEST']
        strike_price = str(int(row_order['STRIKE_PRICE']))
        futures_symbol, put_symbol, call_symbol = populateTradingSymbol(symbol, expiry_date_options,
                                                                        expiry_date_futures, strike_price)

        # POPULATE SYMBOL
        if row_order['INSTRUMENT_TYPE'].value == 'PUT':
            trading_symbol = put_symbol
        elif row_order['INSTRUMENT_TYPE'].value == 'CALL':
            trading_symbol = call_symbol
        else:
            trading_symbol = futures_symbol

        # Add it to Order List
        order_list.extend([{'TRADING_SYMBOL': trading_symbol, 'ORDER_TYPE': reverseOrderType(row_order['TRANSACTION_TYPE'].value),
                             'QUANTITY': row_order['QUANTITY'],
                             'INSTRUMENT_TYPE': row_order['INSTRUMENT_TYPE'].value,
                             'TAG': populateTag(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE'].value),
                                                reverseOrderType(row_order['TRANSACTION_TYPE'].value)),
                             'TORDERID': populateTOrderId(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE'].value),
                                                          reverseOrderType(row_order['TRANSACTION_TYPE'].value)),
                            'ORDER_TYPE_MARKET_LIMIT': 'LIMIT',
                            'PRICE': LimitPriceUtil.getLimitPrice(reverseOrderType(row_order['TRANSACTION_TYPE'].value),
                                                                  row_order['CURRENT_PRICE'],
                                                                  Configuration)
                            }])

        # ADD TRADING SYMBOL IN MASTER DATAFRAME
        df_Put_Call_Order.at[index, 'TRADING_SYMBOL'] = trading_symbol
        ################################################################################################################

    df_Order = pd.DataFrame(order_list)
    df_Order['QUANTITY'] = pd.to_numeric(df_Order['QUANTITY'], downcast='signed')
    return df_Order, order_manifest

def createOrderDataFrameSquareoffQtyFreeze(df_Put_Call_Order, expiry_date_futures, expiry_date_options, order_dict, symbol, Configuration):
    # INITIALIZE ORDER LIST
    order_list = []
    order_manifest = ""

    ####################################################################################################################
    for index, row_order in df_Put_Call_Order.iterrows():
        order_manifest = row_order['ORDER_MANIFEST']
        strike_price = str(int(row_order['STRIKE_PRICE']))
        futures_symbol, put_symbol, call_symbol = populateTradingSymbol(symbol, expiry_date_options,
                                                                        expiry_date_futures, strike_price)

        # POPULATE SYMBOL
        if row_order['INSTRUMENT_TYPE'] == 'PUT':
            trading_symbol = put_symbol
        else:
            trading_symbol = call_symbol

        # POPULATE TAG
        tag = populateTagQtyFreeze(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE']),
                                                row_order['ORDER_TYPE'], index)
        tOrderId = populateTOrderIdQtyFreeze(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE']),
                                                          row_order['ORDER_TYPE'], index)

        # Add it to Order List
        order_list.extend([{'TRADING_SYMBOL': trading_symbol, 'ORDER_TYPE': reverseOrderType(row_order['ORDER_TYPE']),
                             'QUANTITY': row_order['QUANTITY'],
                             'INSTRUMENT_TYPE': row_order['INSTRUMENT_TYPE'],
                             'TAG': tag,
                             'TORDERID': tOrderId,
                             'ORDER_TYPE_MARKET_LIMIT': 'LIMIT',
                             'PRICE': LimitPriceUtil.getLimitPrice(reverseOrderType(row_order['ORDER_TYPE']),
                                                                  row_order['CURRENT_PRICE_ACTUAL'],
                                                                  Configuration)
                             }])

        # ADD TRADING SYMBOL/TAG IN MASTER DATAFRAME
        df_Put_Call_Order.at[index, 'TRADING_SYMBOL'] = trading_symbol
        df_Put_Call_Order.at[index, 'TAG'] = tag
        ################################################################################################################

    df_Order = pd.DataFrame(order_list)
    df_Order['QUANTITY'] = pd.to_numeric(df_Order['QUANTITY'], downcast='signed')
    return df_Order, order_manifest


def createOrderDataFrameMarginFreeUp(df_Put_Call_Order, expiry_date_futures, expiry_date_options, order_dict, symbol):
    # INITIALIZE ORDER LIST
    order_list = []
    order_manifest = ""

    ####################################################################################################################
    for index, row_order in df_Put_Call_Order.iterrows():
        order_manifest = row_order['ORDER_MANIFEST']
        strike_price = str(int(row_order['STRIKE_PRICE']))
        futures_symbol, put_symbol, call_symbol = populateTradingSymbol(symbol, expiry_date_options,
                                                                        expiry_date_futures, strike_price)

        # POPULATE SYMBOL
        if row_order['INSTRUMENT_TYPE'] == 'PUT':
            trading_symbol = put_symbol
        else:
            trading_symbol = call_symbol

        # Add it to Order List
        order_list.extend([{'TRADING_SYMBOL': trading_symbol, 'ORDER_TYPE': row_order['ORDER_TYPE'],
                             'QUANTITY': row_order['QUANTITY_'+row_order['INSTRUMENT_TYPE']],
                             'INSTRUMENT_TYPE': row_order['INSTRUMENT_TYPE'],
                             'TAG': populateTag(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE']), row_order['ORDER_TYPE']),
                             'TORDERID': populateTOrderId(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE']), row_order['ORDER_TYPE'])}])

        # ADD TRADING SYMBOL IN MASTER DATAFRAME
        df_Put_Call_Order.at[index, 'TRADING_SYMBOL'] = trading_symbol
    ####################################################################################################################

    df_Order = pd.DataFrame(order_list)
    df_Order['QUANTITY'] = pd.to_numeric(df_Order['QUANTITY'], downcast='signed')
    return df_Order, order_manifest

def getOptionTypeShort(instrument_type):
    if instrument_type == 'PUT':
        return 'PE'
    elif instrument_type == 'CALL':
        return 'CE'
    else:
        return 'FUT'

def createOrderDataFrameFresh(df_Put_Call_Order, expiry_date_futures, expiry_date_options, order_dict, symbol, Configuration):
    # INITIALIZE ORDER LIST
    order_list = []
    order_manifest = ""

    ####################################################################################################################
    for index, row_order in df_Put_Call_Order.iterrows():
        order_manifest = row_order['ORDER_MANIFEST']
        strike_price = str(int(row_order['STRIKE_PRICE']))
        futures_symbol, put_symbol, call_symbol = populateTradingSymbol(symbol, expiry_date_options,
                                                                        expiry_date_futures, strike_price)

        # POPULATE SYMBOL
        if row_order['INSTRUMENT_TYPE'] == 'PUT':
            trading_symbol = put_symbol
        else:
            trading_symbol = call_symbol

        # Add it to Order List
        order_list.extend([{'TRADING_SYMBOL': trading_symbol, 'ORDER_TYPE': row_order['ORDER_TYPE_'+row_order['INSTRUMENT_TYPE']],
                            'QUANTITY': row_order['QUANTITY_' + row_order['INSTRUMENT_TYPE']],
                            'INSTRUMENT_TYPE': row_order['INSTRUMENT_TYPE'],
                            'TAG': populateTag(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE']),
                                               row_order['ORDER_TYPE_'+row_order['INSTRUMENT_TYPE']]),
                            'TORDERID': populateTOrderId(strike_price, getOptionTypeShort(row_order['INSTRUMENT_TYPE']),
                                                         row_order['ORDER_TYPE_'+row_order['INSTRUMENT_TYPE']]),
                            'ORDER_TYPE_MARKET_LIMIT': 'LIMIT',
                            'PRICE': LimitPriceUtil.getLimitPrice(row_order['ORDER_TYPE_'+row_order['INSTRUMENT_TYPE']],
                                                                  row_order['ENTRY_PRICE_' + row_order['INSTRUMENT_TYPE']],
                                                                  Configuration)}])

        # ADD TRADING SYMBOL IN MASTER DATAFRAME
        df_Put_Call_Order.at[index, 'TRADING_SYMBOL'] = trading_symbol
    ####################################################################################################################

    df_Order = pd.DataFrame(order_list)
    df_Order['QUANTITY'] = pd.to_numeric(df_Order['QUANTITY'], downcast='signed')

    return df_Order, order_manifest


def createOrderRequest(df_Order, order_manifest, Configuration, orderType,
                       margin_txn, tradeType, position_group_id, isOrderTypeMarket=True):

    ######################### IF MARKET ORDER TYPE DEFAULT IT TO MARKET AND PRICE AS ZERO ##############################
    if Configuration['ORDER_TYPE_MARKET_LIMIT']=='MARKET':
        df_Order['ORDER_TYPE_MARKET_LIMIT'] = "MARKET"
        df_Order['PRICE'] = 0


    ############################################### ORDER DICT #########################################################
    order_dict = {}
    order_dict['group_id'] = Configuration['order_group_id']
    order_dict['group_tag'] = Configuration['SCHEMA_NAME']+'_'+str(position_group_id)
    order_digest_df = pd.DataFrame()
    order_digest_df['transaction_type'] = df_Order['ORDER_TYPE']
    order_digest_df['tradingsymbol'] = df_Order['TRADING_SYMBOL']
    order_digest_df['quantity'] = df_Order['QUANTITY'].astype(int)
    order_digest_df['orderid'] = df_Order['TAG']
    order_digest_df['product'] = tradeType

    if not Configuration['ORDER_TYPE_MARKET_LIMIT'] == 'MARKET':
        order_digest_df['price'] = df_Order['PRICE']

    order_digest_list = order_digest_df.to_dict('records')
    order_dict['order_digest'] = order_digest_list
    order_dict_json = json.dumps(order_dict)
    print(order_dict_json)
    logging.info('\n********************************************* BROKER PLACE ORDER : START  ******************************************************\n')
    logging.info('{}: ######################### {} ORDER BROKER API REQUEST ############################\n{}\n'.format(Configuration['SCHEMA_NAME'],
                                                                                                                       orderType, order_dict_json))
    return order_dict

def populateTradingSymbol(symbol, expiry_date_options, expiry_date_futures, strike_price):

    # 2 SCENARIOS - MONTHLY CONTRACT OR WEEKLY CONTRACT, symbols creation changes
    if expiry_date_options == expiry_date_futures:# NIFTY20FEBFUT, NIFTY20FEB12000PE, NIFTY20FEB12000CE
        expiry_year_month = datetime.strptime(expiry_date_futures, '%y%b%d').strftime('%y%b').upper()
        futures_symbol = symbol + expiry_year_month + 'FUT'
        put_symbol = symbol + expiry_year_month + strike_price + 'PE'
        call_symbol = symbol + expiry_year_month + strike_price + 'CE'
    else:# NIFTY20FEBFUT, NIFTY2021312000PE, NIFTY2021312000CE
        expiry_year = datetime.strptime(expiry_date_options, '%y%b%d').strftime('%y').upper()
        expiry_month = datetime.strptime(expiry_date_options, '%y%b%d').strftime('%b').upper()
        expiry_day = datetime.strptime(expiry_date_options, '%y%b%d').strftime('%d')
        futures_symbol = symbol + expiry_year + expiry_month + 'FUT'
        put_symbol = symbol + expiry_year + getWeeklyExpiryMonth(expiry_month) + expiry_day + strike_price + 'PE'
        call_symbol = symbol + expiry_year + getWeeklyExpiryMonth(expiry_month) + expiry_day + strike_price + 'CE'

    return futures_symbol, put_symbol, call_symbol

def getWeeklyExpiryMonth(expiry_month):
    return constants.months_trade_weekly_dict[expiry_month]

def populateTag(strike_price, option_type, order_type):
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    hour, minute,second = current_time.split(':')
    tag = strike_price+option_type+order_type+hour+minute+second
    return tag

def populateTOrderId(strike_price, option_type, order_type):
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    hour, minute, second = current_time.split(':')
    tOrderId = strike_price+option_type+order_type+hour+minute+second
    return tOrderId

def populateTagQtyFreeze(strike_price, option_type, order_type, index):
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
    hour, minute = current_time.split(':')
    tag = strike_price+'_'+option_type+'_'+order_type+'_'+str(index)
    return tag

def populateTOrderIdQtyFreeze(strike_price, option_type, order_type, index):
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
    hour, minute = current_time.split(':')
    tOrderId = strike_price+option_type+order_type+str(index)
    return tOrderId


def populateTagSquareOffException(tradingsymbol, symbol, order_type, index):
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
    hour, minute = current_time.split(':')
    strike_price = tradingsymbol[-7:][:-2]
    option_type = tradingsymbol[-2:]
    tag = strike_price+'_'+option_type+'_'+order_type+'_'+str(index)
    return tag

def reverseOrderType(orderType):
    if orderType == 'BUY':
        return 'SELL'
    else:
        return 'BUY'

def createOrderDataFrameSquareOffException(df_ActivePositions, Configuration, symbol):
    # INITIALIZE ORDER LIST
    order_list = []
    order_manifest = ""

    ################################################# CHECK QTY FREEZE SCENARIO ########################################
    df_Put_Call_Order, isQtyFreeze = QtyFreezeHelper.qtyFreezeSplitSquareOffException(df_ActivePositions, Configuration, symbol)

    ####################################################################################################################
    for index, row_order in df_Put_Call_Order.iterrows():
        order_manifest = populateTagSquareOffException(row_order['tradingsymbol'], symbol, row_order['order_type'], index)

        # Add it to Order List
        order_list.extend([{'TRADING_SYMBOL': row_order['tradingsymbol'], 'ORDER_TYPE': row_order['order_type'],
                             'QUANTITY': row_order['quantity'],
                             'TAG': order_manifest[0:19],# tag accepts only 20 characters
                             'TORDERID': order_manifest}])

    ####################################################################################################################

    df_Order = pd.DataFrame(order_list)
    df_Order['QUANTITY'] = pd.to_numeric(df_Order['QUANTITY'], downcast='signed')
    return df_Order, order_manifest, isQtyFreeze