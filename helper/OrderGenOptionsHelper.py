import pandas as pd
import requests
import numpy as np
import json
from common import utils
from datetime import datetime, date
import calendar
import pytz
from common import utils as parent_utils
from common.UniqueKeyGenerator import UniqueKeyGenerator
from dao import PositionsDAO, OrdersDAO, PositionsBackUpDAO
import logging
import time
import traceback
from dateutil import relativedelta
#from helper import OrderPlacementHelper
import warnings
from pandas.core.common import SettingWithCopyWarning
from config.cache.BasicCache import BasicCache
from adaptor import BTAdaptor, GreeksAdaptor
from helper import OrderPlacementHelper


warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


def getOptionsChain(symbol, contract_type, Configuration):
    ############################################# GET EXPIRY DATES #####################################################
    expiry_date_options, expiry_date_futures = callExpiryDateAPI(Configuration, contract_type)

    #TODO : MOCK EXPIRY DATE
    # if contract_type == 'WEEKLY':
    #     expiry_date = '19DEC26'

    ############################################# GET OPTION CHAIN #####################################################
    #df_Call, df_Put, spot_value, future_value = callOptionChain(symbol, expiry_date_options, Configuration)
    ############################################# GET STRIKE PRICES #####################################################
    mdsAdaptor = utils.getMDSAdaptor(Configuration)
    df_Call, df_Put, spot_value, future_value = mdsAdaptor.getStrikePricesAPI(Configuration, symbol, expiry_date_options)

    return df_Call, df_Put, spot_value, expiry_date_options, expiry_date_futures, future_value


def callExpiryDateAPI(Configuration, contract_type):
    expiry_date_url = Configuration["MDS_URL"] + '/nse/options/v2/expirydates'

    # MOCK EXPIRIES
    if utils.isMockPrdEnv(Configuration) or utils.isMockDevEnv(Configuration):
        expiry_date_options, expiry_date_futures = utils.getWeeklyExpiryDate_v2().strftime('%y%b%d').upper(), \
                                                   utils.getExpiryDate().strftime('%y%b%d').upper()
        logging.info("MOCK EXPIRY DATES: expiry_date_options: {}, expiry_date_futures : {}".format(expiry_date_options, expiry_date_futures))
        return expiry_date_options, expiry_date_futures

    # EXPIRY DATES FOR BACKTESTING
    if utils.isBackTestingEnv(Configuration):
        expiry_date_str = BasicCache().get('EXPIRY')
        expiry_date = datetime.strptime(expiry_date_str, '%Y%b%d')
        expiry_date = date(expiry_date.year, expiry_date.month, expiry_date.day)
        expiry_date = expiry_date.strftime('%y%b%d').upper()
        return expiry_date, BasicCache().get('EXPIRY_DATE_FUTURES')

    # CALL MDS TO GET EXPIRY DATES
    expiry_date_list = requests.get(expiry_date_url)
    expiry_date_options, expiry_date_futures = getExpiryDate(expiry_date_list, contract_type, Configuration)
    return expiry_date_options, expiry_date_futures

def getExpiryDate(expiry_date_list, contract_type, Configuration):
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%y%b%d').upper()
    current_date = datetime.strptime(current_date, '%y%b%d')
    current_date = date(current_date.year, current_date.month, current_date.day)

    # MOCK DATA
    # current_date = date(2020,3,27)
    #contract_type = 'MONTHLY'
    expiry_date_list_original = list(expiry_date_list.json())

    # Transformation
    expiry_date_list_str=[]
    for expiry_date_str in expiry_date_list_original:
        expiry_date = datetime.strptime(expiry_date_str, '%Y%b%d')
        expiry_date = date(expiry_date.year, expiry_date.month, expiry_date.day)
        expiry_date = expiry_date.strftime('%y%b%d').upper()
        expiry_date_list_str.append(expiry_date)

    # expiry_date_list_str = [
    # "20DEC24",
    # "20DEC31"
    # ]

    ####################################### Filter Past Dates #####################################################
    expiry_date_list = []
    for expiry_date_str in expiry_date_list_str:
        expiry_date = datetime.strptime(expiry_date_str, '%y%b%d')
        expiry_date = date(expiry_date.year, expiry_date.month, expiry_date.day)
        if expiry_date >= current_date:
            expiry_date_list.append(expiry_date)

    ################################### Covert date to string using datetime #######################################
    next_month_date = date.today() + relativedelta.relativedelta(months=1)
    next_month = datetime.combine(next_month_date, datetime.min.time()).strftime('%b').upper()
    next_next_month_date = date.today() + relativedelta.relativedelta(months=2)
    next_next_month = datetime.combine(next_next_month_date, datetime.min.time()).strftime('%b').upper()
    current_month = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%b').upper()
    #current_month = 'MAR'
    expiry_date_list_str = []
    expiry_dates_current_month_list = []
    expiry_dates_next_month_list = []
    expiry_dates_next_next_month_list = []
    for expiry_date in expiry_date_list:

        # ALL EXPIRY DATES
        expiry_date_str = datetime.combine(expiry_date, datetime.min.time()).strftime('%y%b%d').upper()
        expiry_date_list_str.append(expiry_date_str)

        # CURRENT MONTH EXPIRY DATES
        if current_month in expiry_date_str:
            expiry_dates_current_month_list.append(expiry_date_str)

        # NEXT MONTH EXPIRY DATES
        if next_month in expiry_date_str:
            expiry_dates_next_month_list.append(expiry_date_str)

        # NEXT NEXT MONTH EXPIRY DATES
        if next_next_month in expiry_date_str:
            expiry_dates_next_next_month_list.append(expiry_date_str)


    #################################### GET EXPIRY DATE FOR WEEKLY AND MONTHLY EXPIRY ###############################
    my_date = date.today()
    weekday = calendar.day_name[my_date.weekday()]
    if contract_type == 'WEEKLY':
        expiry_date_options = expiry_date_list_str[0]

        # EXPIRY_DATE_FUTURES - MONTHLY
        if len(expiry_dates_current_month_list) < 1: # if last week use next months date
            expiry_date_futures = expiry_dates_next_month_list[-1]
        else:
            expiry_date_futures = expiry_dates_current_month_list[-1] # Use Current Month Expiry Date

    elif contract_type == 'NEXTMONTHLY':
        expiry_date_options = expiry_date_list_str[0]
        expiry_date_futures = expiry_dates_next_month_list[-1]

    elif contract_type == 'NEXTWEEKLY':
        expiry_date_options = expiry_date_list_str[1]

        # EXPIRY_DATE_FUTURES - NEXTMONTHLY
        if len(expiry_dates_current_month_list) < 1: # if last week use next months date
            expiry_date_futures = expiry_dates_next_month_list[-1]
        else:
            expiry_date_futures = expiry_dates_current_month_list[-1] # Use Current Month Expiry Date
    elif contract_type == 'MONTHLY':

        if len(expiry_dates_current_month_list) < 1 or utils.checkIfLastThursday(): # if last week use next months date
            expiry_date_options = expiry_dates_next_month_list[-1]
            expiry_date_futures = expiry_dates_next_next_month_list[-1]
        else:
            expiry_date_options = expiry_dates_current_month_list[-1]  # Use Current Month Expiry Date
            expiry_date_futures = expiry_dates_next_month_list[-1]

    else:
        #expiry_date = expiry_dates_current_month_list[-1]  # Use Current Month Expiry Date
        if len(expiry_dates_current_month_list) < 1: # if last week use next months date
            expiry_date_options = expiry_dates_next_month_list[-1]
            expiry_date_futures = expiry_dates_next_month_list[-1]
        else:
            expiry_date_options = expiry_dates_current_month_list[-1] # Use Current Month Expiry Date
            expiry_date_futures = expiry_dates_current_month_list[-1]


    print('Expiry Date Selected Options, Futures:: '+str(expiry_date_options)+', '+str(expiry_date_futures))

    return expiry_date_options, expiry_date_futures


def addLevelPutCall(df_Call, df_Put, spot_value, Configuration):
    #df_Call = df_Call[((df_Call['STRIKE_PRICE'] / 50) % 2 == 0)] # GET STRIKES ONLY WITH MULTIPLE OF 100
    #df_Put = df_Put[((df_Put['STRIKE_PRICE'] / 50) % 2 == 0)]
    FilterLevel = int(Configuration['FILTER_LEVEL_OPTION_CHAIN'])
    SP_List = df_Call['STRIKE_PRICE'].tolist()
    SP_ATM = utils.takeClosest(spot_value, SP_List)

    # Split DF into ITM and OTM
    df_Call_ITM = df_Call[df_Call['STRIKE_PRICE'] < spot_value]
    df_Call_OTM = df_Call[df_Call['STRIKE_PRICE'] > spot_value]

    df_Call_OTM = df_Call_OTM.head(FilterLevel)
    df_Call_ITM = df_Call_ITM.tail(FilterLevel)
    # Add Option Type
    df_Call_OTM = df_Call_OTM.reset_index(drop=True)
    df_Call_ITM = df_Call_ITM.reset_index(drop=True)
    # Add Label Columns
    if SP_ATM in df_Call_ITM['STRIKE_PRICE'].values:
        print('ATM lies in ITM')

        # populate ITM Levels
        # Iterate ITM in reverse
        row_count_ITM = df_Call_ITM.shape[0]
        Level_List_ITM_Call = []
        Level_List_OTM_Put = []
        for i in range(row_count_ITM - 1, 0, -1):
            Level_List_ITM_Call.append('CE_ITM_' + str(i))
            Level_List_OTM_Put.append('PE_OTM_' + str(i))
        Level_List_ITM_Call.append('CE_ATM')
        Level_List_OTM_Put.append('PE_ATM')

        df_Call_ITM['LEVEL_CALL'] = pd.Series(Level_List_ITM_Call)

        # populate OTM Levels
        row_count_OTM = df_Call_OTM.shape[0]
        Level_List_OTM_Call = []
        Level_List_ITM_Put = []
        for i in range(0, row_count_OTM):
            Level_List_OTM_Call.append('CE_OTM_' + str(i + 1))
            Level_List_ITM_Put.append('PE_ITM_' + str(i + 1))

        df_Call_OTM['LEVEL_CALL'] = pd.Series(Level_List_OTM_Call)

        # Concat ITM and OTM data frames
        df_Call = pd.concat([df_Call_ITM, df_Call_OTM], ignore_index=True)

        Level_List_Put = Level_List_OTM_Put + Level_List_ITM_Put

    else:
        print('ATM lies in OTM')
        # populate ITM Levels
        # Iterate ITM in reverse
        row_count_ITM = df_Call_ITM.shape[0]
        Level_List_ITM_Call = []
        Level_List_OTM_Put = []
        for i in range(row_count_ITM, 0, -1):
            Level_List_ITM_Call.append('CE_ITM_' + str(i))
            Level_List_OTM_Put.append('PE_OTM_' + str(i))

        df_Call_ITM['LEVEL_CALL'] = pd.Series(Level_List_ITM_Call)

        # populate OTM Levels
        row_count_OTM = df_Call_OTM.shape[0]
        Level_List_ITM_Put = []
        Level_List_OTM_Call = []
        Level_List_OTM_Call.append('CE_ATM')
        Level_List_ITM_Put.append('PE_ATM')
        for i in range(0, row_count_OTM - 1):
            Level_List_OTM_Call.append('CE_OTM_' + str(i + 1))
            Level_List_ITM_Put.append('PE_ITM_' + str(i + 1))

        df_Call_OTM['LEVEL_CALL'] = pd.Series(Level_List_OTM_Call)
        # print(df_Call_OTM)

        # Concat ITM and OTM data frames
        df_Call = pd.concat([df_Call_ITM, df_Call_OTM], ignore_index=True)
        # print(df_Call.Level.values)
        Level_List_Put = Level_List_OTM_Put + Level_List_ITM_Put
    # print(df_Call[['STRIKE_PRICE', 'Level']])
    df_Put_ITM = df_Put[df_Put['STRIKE_PRICE'] > spot_value]
    df_Put_OTM = df_Put[df_Put['STRIKE_PRICE'] < spot_value]
    df_Put_OTM = df_Put_OTM.reset_index(drop=True)
    df_Put_ITM = df_Put_ITM.reset_index(drop=True)
    df_Put = pd.concat([df_Put_OTM, df_Put_ITM], ignore_index=True)
    df_Put_Call = pd.merge(df_Put, df_Call, how='inner', on='STRIKE_PRICE')
    df_Put_Call['LEVEL_PUT'] = pd.Series(Level_List_Put)

    return df_Put_Call, SP_ATM

def addBidAskToDF(df_Put_Call):
    try:
        for i in range(0, 5):
            bid_price, bid_orders, bid_qty = 'BID_PRICE_' + str(i + 1), 'BID_ORDERS_' + str(i + 1), 'BID_QTY_' + str(i + 1)
            ask_price, ask_orders, ask_qty = 'ASK_PRICE_' + str(i + 1), 'ASK_ORDERS_' + str(i + 1), 'ASK_QTY_' + str(i + 1)
            df_Put_Call[bid_price] = df_Put_Call['depth'].apply(lambda x: x['buy'][i]['price'])
            df_Put_Call[bid_orders] = df_Put_Call['depth'].apply(lambda x: x['buy'][i]['orders'])
            df_Put_Call[bid_qty] = df_Put_Call['depth'].apply(lambda x: x['buy'][i]['quantity'])

            df_Put_Call[ask_price] = df_Put_Call['depth'].apply(lambda x: x['sell'][i]['price'])
            df_Put_Call[ask_orders] = df_Put_Call['depth'].apply(lambda x: x['sell'][i]['orders'])
            df_Put_Call[ask_qty] = df_Put_Call['depth'].apply(lambda x: x['sell'][i]['quantity'])
    except Exception as ex:
        template = "DEPTH NOT COMING IN OPTION CHAIN, WILL ADD IT LATER."
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(message)

    return df_Put_Call

def populateTimeToExpiry(expiry_date, Configuration):


    ############################### CALCULATE TIME TO EXPIRY for 252 DAYS  #############################################
    #DAYS_IN_YEAR = float(Configuration['DAYS_IN_YEAR'])
    DAYS_IN_YEAR = 252
    time_to_expiry = get_time_delta(expiry_date)
    time_to_expiry_252 = time_to_expiry
    # MINUS HOLIDAYS
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%y%b%d').upper()

    ################################################# BACKTESTING MOCK #################################################
    if utils.isBackTestingEnv(Configuration):
        current_time = BasicCache().get('CURRENT_TIME')
        current_date = BasicCache().get('CURRENT_DATE')
        current_date = current_date.strftime('%y%b%d')
        current_date_time = BasicCache().get('CURRENT_DATE_TIME')
        date_format = "%y%b%d %H:%M:%S"
        date_format_days = "%y%b%d"
        now_close = datetime.strptime(current_date_time.strftime('%y%b%d') + " 15:30:00",date_format)
        time_to_expiry = get_time_delta_v1(expiry_date, current_date_time, now_close)
        time_to_expiry_252 = time_to_expiry
    #####################################################################################################################

    holidays = parent_utils.getHolidaysBetweenDates(current_date, expiry_date, '%y%b%d')
    #print(time_to_expiry, holidays)
    if time_to_expiry > holidays:
        time_to_expiry_252 = time_to_expiry - holidays
    time_to_expiry_annualized_252 = time_to_expiry_252 / DAYS_IN_YEAR

    ################################ CALCULATE TIME TO EXPIRY for 365 DAYS #############################################
    DAYS_IN_YEAR = 365
    time_to_expiry_365 = time_to_expiry
    # TIME TO EXPIRY ANNUALIZED
    time_to_expiry_annualized_365 = time_to_expiry_365 / DAYS_IN_YEAR

    return time_to_expiry_252, time_to_expiry_annualized_252, time_to_expiry_365, time_to_expiry_annualized_365

def get_time_delta(expiryDate):
    expiryDate_begin = expiryDate + " 09:15:00"
    expiryDate_end = expiryDate + " 15:30:00"
    date_format = "%y%b%d %H:%M:%S"
    exp_date_begin = datetime.strptime(expiryDate_begin, date_format)
    exp_date_end = datetime.strptime(expiryDate_end, date_format)
    now = datetime.strptime(datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%y%b%d %H:%M:%S'),date_format)
    date_format_days = "%y%b%d"
    now_close = datetime.strptime(datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%y%b%d') + " 15:30:00",date_format)


    delta_days = exp_date_end - now
    delta_seconds = exp_date_end - exp_date_begin
    delta_seconds_current_day = now_close - now
    if(now > exp_date_end):
        return 0
    elif(now < exp_date_begin):
        if(delta_seconds_current_day.days<0):
            return delta_days.days + delta_seconds.seconds / 22500
        else:
            return delta_days.days + delta_seconds_current_day.seconds/22500
    elif(now >= exp_date_begin):
        return  delta_days.seconds/22500

def get_time_delta_v1(expiryDate, now, now_close):
    expiryDate_begin = expiryDate + " 09:15:00"
    expiryDate_end = expiryDate + " 15:30:00"
    date_format = "%y%b%d %H:%M:%S"
    exp_date_begin = datetime.strptime(expiryDate_begin, date_format)
    exp_date_end = datetime.strptime(expiryDate_end, date_format)
    #now = datetime.strptime(datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%y%b%d %H:%M:%S'),date_format)

    delta_days = exp_date_end - now
    delta_seconds = exp_date_end - exp_date_begin
    delta_seconds_current_day = now_close - now
    if(now > exp_date_end):
        return 0
    elif(now < exp_date_begin):
        if(delta_seconds_current_day.days<0):
            return delta_days.days + delta_seconds.seconds / 22500
        else:
            return delta_days.days + delta_seconds_current_day.seconds/22500
    elif(now >= exp_date_begin):
        return  delta_days.seconds/22500

def addSignToDelta(row):
    if row['ORDER_TYPE_PUT'] == 'BUY':
        row['NET_DELTA'] = -1*row['NET_DELTA']
    else:
        row['NET_DELTA'] = row['NET_DELTA']
    return row

def populateLotsPerUnit(x):
    # ROUNDING TO 1
    round_up_factor = round((np.sign(x))*1/x)
    round_up_value = x*round_up_factor
    return round_up_value, round_up_factor


def addMarginInfo(MF_STEP_SIZE, orderGen_dict, Configuration, df_Put_Call, symbol):
    LOT_SIZE = Configuration['LOT_SIZE_' + symbol]
    MF_STEP_SIZE = orderGen_dict['MF_STEP_SIZE']


    # POPULATE PREMIUM
    df_Put_Call['PREMIUM_PUT'] = df_Put_Call.apply(lambda row: populatePremium('PUT', row, LOT_SIZE, MF_STEP_SIZE), axis=1)
    df_Put_Call['PREMIUM_CALL'] = df_Put_Call.apply(lambda row: populatePremium('CALL', row, LOT_SIZE, MF_STEP_SIZE),axis=1)

    # POPULATE MARGIN
    df_Put_Call['MARGIN_FUTURES'] = 0.0
    df_Put_Call['MARGIN_PUT'] = 0.0
    df_Put_Call['MARGIN_CALL'] = 0.0
    df_Put_Call['MARGIN_OVERALL'] = float(Configuration['TOTAL_INITIAL_MARGIN'])

    return df_Put_Call

def populateMargin(symbol, optionType, row, marginService):
    if row['ORDER_TYPE_'+optionType] == 'SELL':
        expiryMonth = parent_utils.getExpiryDate().strftime('%y%b').upper()
        symbol, expiryMonth, optionType, strikePrice, qty, tradeType = symbol, expiryMonth, optionType, row['STRIKE_PRICE'], row['QUANTITY_'+optionType], 'sell'
        marginOptions = marginService.calculateOptionsMarginIntraday(symbol, expiryMonth, optionType, strikePrice, qty, tradeType)
    else:
        marginOptions = 0.0  # No margin blocked in case of BUYING Options
    return marginOptions

def populatePremium(optionType, row, LOT_SIZE, MF_STEP_SIZE):

    if row['ORDER_TYPE_'+optionType] == 'BUY':
        PREMIUM = float(row['ENTRY_PRICE_'+optionType]) * float(LOT_SIZE) * float(MF_STEP_SIZE)*-1
    else:
        PREMIUM = float(row['ENTRY_PRICE_'+optionType]) * float(LOT_SIZE) * float(MF_STEP_SIZE)

    return PREMIUM


def placeDealsAndPopulateDBOptions(position_group_id, MF_STEP_SIZE, orderGen_dict, Configuration, df_Put_Call,
                                                         symbol, expiry_date, time_to_expiry_options_252, time_to_expiry_options_365,
                                                         spot_value, expiry_date_futures, schema_name):
    order_group_id = str(int(UniqueKeyGenerator().getUniqueKeyDateTime()))
    Configuration['order_group_id'] = order_group_id
    orderGen_dict['order_group_id'] = order_group_id

    ############################### PLACE ACTUAL TRADE USING OM ####################################################
    df_Put_Call = OrderPlacementHelper.orderPlacement(Configuration, df_Put_Call, expiry_date, expiry_date_futures,
                                                      position_group_id, symbol, orderGen_dict)

    ######################################## ADD IVs FOR ANALYSIS : POST ORDER PLACEMENT ###########################
    # Convert all columns to lowercase
    # df_Put_Call.columns = [x.upper() for x in df_Put_Call.columns]
    # df_Put_Call = populateVIXGreeksIV(symbol, Configuration, df_Put_Call, orderGen_dict)
    # # Convert all columns to lowercase
    # df_Put_Call.columns = [x.lower() for x in df_Put_Call.columns]

    # Transformation
    df_positions = transformationPositionsDataframe(df_Put_Call, position_group_id, MF_STEP_SIZE, symbol, expiry_date,
                                                    time_to_expiry_options_252, time_to_expiry_options_365,
                                                    spot_value, orderGen_dict)
    insertFreshPositions(df_positions, schema_name)
    insertFreshOrders(df_positions, schema_name)

    #################################### LOGGING ORDER GENERATION #######################################################
    logging.info( "######################################################################################################")
    print("######################################################################################################")
    logging.info("{}: ORDER GEN INITIATED for {}, STRIKE PRICE : {}".format(Configuration['SCHEMA_NAME'], orderGen_dict['symbol'], str(df_positions['strike_price'].tolist())))
    print("{}: ORDER GEN INITIATED for {}, STRIKE PRICE : {}".format(Configuration['SCHEMA_NAME'], orderGen_dict['symbol'], str(df_positions['strike_price'].tolist())))
    print("######################################################################################################")
    logging.info("######################################################################################################")

    ######################################### UPDATE AVAILABLE MARGIN INFO IN CONFIGURATION TABLE #######################
    #updateMarginInfo(df_positions, orderGen_dict)
    logging.info('{}: Fresh Position Taken: Inserted in OOPS_POSITIONS AND TXN Table for Symbol {} and PositionGroupId {}'.
                 format(Configuration['SCHEMA_NAME'], symbol, position_group_id))

    orderGen_dict['df_Put_Call'] = df_Put_Call
    orderGen_dict['df_positions_existing'] = df_positions  # RESET df_positions_existing

    return orderGen_dict

def updatePositionMergeValues(df_positions):
    for index, row_position in df_positions.iterrows():
        # UPDATE POSITIONS TABLE
        PositionsDAO.updatePositionsMergeValues(row_position)


def calculateWAPExecutionPrice(execution_price, values, quantity, num_units):
    return (execution_price* quantity + values* num_units)/(quantity+num_units)

def populateExecutionParams(row):
    execution_params = json.loads(row['EXECUTION_PARAMS_POSITION'])
    row['DAYS_IN_YEAR_POSITION'] = execution_params['execution_days_in_year']
    row['EXECUTION_PARITY_FUTURES_POSITION'] = execution_params['execution_parity_futures']
    row['ORDER_MANIFEST_POSITION'] = execution_params['order_manifest']
    row['BOUND_TYPE_POSITION'] = execution_params['bound_type']
    return row

def insertFreshPositions(df_positions, schema_name):

    for index, row in df_positions.iterrows():
        PositionsDAO.insert(row=row, schema_name= schema_name)
        PositionsBackUpDAO.insert(row=row, schema_name=schema_name)
    print('Inserted in POSITIONS Table')


def insertFreshOrders(df_positions, schema_name):
    for index, row in df_positions.iterrows():
        OrdersDAO.insert(row=row, schema_name = schema_name)
    print('Inserted in ORDERS Table')

def updatePositionsExistingOrderGenJob(df_positions,symbol):
    for index, row_position in df_positions.iterrows():
        PositionsDAO.updatePositionsExistingOrderGenJob(row_position=row_position, symbol=symbol)
    print('Inserted in OOPS_POSITIONS_OPTIONS Table')


def transformationPositionsDataframe(df_Put_Call, position_group_id, MF_STEP_SIZE, symbol, expiry_date,
                                                        time_to_expiry_options_252, time_to_expiry_options_365,
                                                        spot_value, orderGen_dict):
    df_positions = pd.DataFrame()

    # Convert all columns to lowercase
    df_Put_Call.columns = [x.lower() for x in df_Put_Call.columns]

    for index, row in df_Put_Call.iterrows():
        # GET NEW ORDER ID
        position_id = UniqueKeyGenerator().getUniqueKeyDateTime()

        entry_params_dict = {'entry_delta_call': row['delta_call'],
                                 'entry_delta_put': row['delta_put'],
                                 'entry_net_delta': row['net_delta'],
                                 'entry_net_delta_overall': row['net_delta'],
                                 'entry_delta_call_actual': row['delta_call'],
                                 'entry_delta_put_actual': row['delta_put'],
                                 'entry_delta_diff_actual': row['net_delta'],
                                 'entry_iv_put': row['iv_put'],
                                 'entry_iv_call': row['iv_call'],
                                 'entry_iv_diff_pct': row['iv_diff_pct'],
                                 'entry_iv_put_opt': row['iv_put_opt'],
                                 'entry_iv_call_opt': row['iv_call_opt'],
                                 'entry_iv_diff_pct_opt': row['iv_diff_pct_opt'],
                                 'entry_iv_put_actual': row['iv_put'],
                                 'entry_iv_call_actual': row['iv_call'],
                                 'entry_iv_diff_pct_actual': row['iv_diff_pct'],
                                 'entry_time_to_expiry_options_252': time_to_expiry_options_252,
                                 'entry_time_to_expiry_options_365': time_to_expiry_options_365,
                                 'entry_underlying' : float(spot_value),
                                 'entry_gamma_call': row['gamma_call'],
                                 'entry_gamma_put': row['gamma_put'],
                                 'entry_net_gamma': row['net_gamma'],
                                 'entry_theta_call': row['theta_call'],
                                 'entry_theta_put': row['theta_put'],
                                 'entry_net_theta': row['net_theta'],
                                 'entry_vega_call': row['vega_call'],
                                 'entry_vega_put': row['vega_put'],
                                 'entry_net_vega': row['net_vega'],
                                 'entry_days_in_year' : row['days_in_year'],
                                 'order_manifest': row['order_manifest'],
                                 'execution_price':row['entry_price_'+row['instrument_type'].lower()],
                                 'margin_one_mf': orderGen_dict['MARGIN_ONE_MF'],
                                 'entry_net_pnl_pending': row['net_pnl_pending'],
                                 'entry_atm_put_price': orderGen_dict['ENTRY_ATM_PUT_PRICE'],
                                 'entry_atm_call_price': orderGen_dict['ENTRY_ATM_CALL_PRICE'],
                                 'entry_atm_avg_price': orderGen_dict['ENTRY_ATM_AVG_PRICE'],
                                 'entry_atm_price_diff': orderGen_dict['ENTRY_ATM_PRICE_DIFF'],
                                 'entry_vix': orderGen_dict['VIX'],
                                 'future_price_initial': orderGen_dict['future_value']
                                 }
        if row['days_in_year'] == '252':
            entry_params_dict['entry_time_to_expiry_options'] = time_to_expiry_options_252
        else:
            entry_params_dict['entry_time_to_expiry_options'] = time_to_expiry_options_365

        entry_params = json.dumps(entry_params_dict)

        # CREATE POSITION DICTIONARY
        position_dict = populatePositionsDict(row, symbol, entry_params,
                                             position_group_id, position_id, expiry_date, orderGen_dict)

        # ADD IT TO DATAFRAME
        df_positions = df_positions.append(position_dict, ignore_index = True)

    return df_positions

def populateExecutionPrices(orderType, df_Put_Call):
    if orderType == 'BUY':
        df_Put_Call['EXECUTION_PRICE_CALL'] = df_Put_Call['ASK_PRICE_1_CALL']
        df_Put_Call['EXECUTION_PRICE_CALL_ACTUAL'] = df_Put_Call['ASK_PRICE_1_CALL']
        df_Put_Call['EXECUTION_PRICE_PUT'] = df_Put_Call['ASK_PRICE_1_PUT']
        df_Put_Call['EXECUTION_PRICE_PUT_ACTUAL'] = df_Put_Call['ASK_PRICE_1_PUT']
    elif orderType == 'SELL':
        df_Put_Call['EXECUTION_PRICE_CALL'] = df_Put_Call['BID_PRICE_1_CALL']
        df_Put_Call['EXECUTION_PRICE_CALL_ACTUAL'] = df_Put_Call['BID_PRICE_1_CALL']
        df_Put_Call['EXECUTION_PRICE_PUT'] = df_Put_Call['BID_PRICE_1_PUT']
        df_Put_Call['EXECUTION_PRICE_PUT_ACTUAL'] = df_Put_Call['BID_PRICE_1_PUT']
    else:
        df_Put_Call['EXECUTION_PRICE_CALL'] = df_Put_Call['LAST_PRICE_CALL']
        df_Put_Call['EXECUTION_PRICE_CALL_ACTUAL'] = df_Put_Call['LAST_PRICE_CALL']
        df_Put_Call['EXECUTION_PRICE_PUT'] = df_Put_Call['LAST_PRICE_PUT']
        df_Put_Call['EXECUTION_PRICE_PUT_ACTUAL'] = df_Put_Call['LAST_PRICE_PUT']

    return df_Put_Call


def populateWAPBidAskPrices(df_Put_Call, MF_STEP_SIZE, symbol, Configuration):
    LOT_SIZE = Configuration['LOT_SIZE_' + symbol]
    df_Put_Call['LOT_SIZE'] = LOT_SIZE
    df_Put_Call['MULTI_FACTOR'] = MF_STEP_SIZE
    df_Put_Call['QUANTITY_PUT'] = df_Put_Call.apply(lambda row: calculateQuantity(LOT_SIZE, MF_STEP_SIZE), axis=1).astype(float)
    df_Put_Call['QUANTITY_CALL'] = df_Put_Call.apply(lambda row: calculateQuantity(LOT_SIZE, MF_STEP_SIZE), axis=1).astype(float)
    df_Put_Call['BID_PRICE_PUT'] = df_Put_Call.apply(lambda row: calculateBidAskPriceByMF(row, 'PUT')[0],axis=1).astype(float)
    df_Put_Call['ASK_PRICE_PUT'] = df_Put_Call.apply(lambda row: calculateBidAskPriceByMF(row, 'PUT')[1],axis=1).astype(float)
    df_Put_Call['BID_PRICE_CALL'] = df_Put_Call.apply(lambda row: calculateBidAskPriceByMF(row, 'CALL')[0],axis=1).astype(float)
    df_Put_Call['ASK_PRICE_CALL'] = df_Put_Call.apply(lambda row: calculateBidAskPriceByMF(row, 'CALL')[1],axis=1).astype(float)

    return df_Put_Call

def populateExecutionPricesWAP(df_Put_Call):
    for index, row in df_Put_Call.iterrows():

        # SET CALL PRICES
        if row['ORDER_TYPE_CALL'] == 'SELL':
            df_Put_Call.at[index, 'ENTRY_PRICE_CALL'] = row['BID_PRICE_CALL']
        elif row['ORDER_TYPE_CALL'] == 'BUY':
            df_Put_Call.at[index, 'ENTRY_PRICE_CALL'] = row['ASK_PRICE_CALL']
        else:
            df_Put_Call.at[index, 'ENTRY_PRICE_CALL'] = row['LAST_PRICE_CALL']

        # SET PUT PRICES
        if row['ORDER_TYPE_PUT'] == 'SELL':
            df_Put_Call.at[index, 'ENTRY_PRICE_PUT'] = row['BID_PRICE_PUT']
        elif row['ORDER_TYPE_PUT'] == 'BUY':
            df_Put_Call.at[index, 'ENTRY_PRICE_PUT'] = row['ASK_PRICE_PUT']
        else:
            df_Put_Call.at[index, 'ENTRY_PRICE_PUT'] = row['LAST_PRICE_PUT']

    return df_Put_Call

def calculateQuantity(LOT_SIZE, MF_IA):
    return float(LOT_SIZE) * float(MF_IA)

def calculateBidAskPriceByMF(row, option_type):
    bid_ask_dict = {}

    quantity = row['QUANTITY_' + option_type]

    bid_ask_dict['BID_PRICE_1'] = row['BID_PRICE_1_' + option_type]
    bid_ask_dict['BID_PRICE_2'] = row['BID_PRICE_2_' + option_type]
    bid_ask_dict['BID_PRICE_3'] = row['BID_PRICE_3_' + option_type]
    bid_ask_dict['BID_PRICE_4'] = row['BID_PRICE_4_' + option_type]
    bid_ask_dict['BID_PRICE_5'] = row['BID_PRICE_5_' + option_type]
    bid_ask_dict['BID_QTY_1'] = row['BID_QTY_1_' + option_type]
    bid_ask_dict['BID_QTY_2'] = row['BID_QTY_2_' + option_type]
    bid_ask_dict['BID_QTY_3'] = row['BID_QTY_3_' + option_type]
    bid_ask_dict['BID_QTY_4'] = row['BID_QTY_4_' + option_type]
    bid_ask_dict['BID_QTY_5'] = row['BID_QTY_5_' + option_type]

    bid_ask_dict['ASK_PRICE_1'] = row['ASK_PRICE_1_' + option_type]
    bid_ask_dict['ASK_PRICE_2'] = row['ASK_PRICE_2_' + option_type]
    bid_ask_dict['ASK_PRICE_3'] = row['ASK_PRICE_3_' + option_type]
    bid_ask_dict['ASK_PRICE_4'] = row['ASK_PRICE_4_' + option_type]
    bid_ask_dict['ASK_PRICE_5'] = row['ASK_PRICE_5_' + option_type]
    bid_ask_dict['ASK_QTY_1'] = row['ASK_QTY_1_' + option_type]
    bid_ask_dict['ASK_QTY_2'] = row['ASK_QTY_2_' + option_type]
    bid_ask_dict['ASK_QTY_3'] = row['ASK_QTY_3_' + option_type]
    bid_ask_dict['ASK_QTY_4'] = row['ASK_QTY_4_' + option_type]
    bid_ask_dict['ASK_QTY_5'] = row['ASK_QTY_5_' + option_type]

    BID_PRICE, ASK_PRICE = parent_utils.getWAPBidAskPrice(bid_ask_dict, quantity)

    return BID_PRICE, ASK_PRICE

def addBidAskPricesMDS(df_Put_Call, expiry_date, symbol, Configuration, orderGen_dict, isValidationRequired = True, isSquareOffJob = False):
    SP_LIST = df_Put_Call['STRIKE_PRICE'].tolist()
    SP_LIST = [int(i) for i in SP_LIST]

    ############################ CALL OPTION CHAIN LITE MARKET DEPTH PUT ###############################################
    mdsAdaptor = utils.getMDSAdaptor(Configuration)

    df_Market_Depth_Put = mdsAdaptor.getOptionChainLiteMarketDepth(Configuration, SP_LIST, 'PE', expiry_date, symbol)

    remove_column_list = ['buy', 'sell']
    df_Market_Depth_Put.drop(remove_column_list, axis=1, inplace=True)
    df_Market_Depth_Put.columns = [str(col).upper() + '_PUT' for col in df_Market_Depth_Put.columns]
    df_Market_Depth_Put['STRIKE_PRICE'] = df_Market_Depth_Put.index
    df_Market_Depth_Put.reset_index(inplace=True)
    df_Market_Depth_Put.drop(['index'], axis=1, inplace=True)
    df_Market_Depth_Put['STRIKE_PRICE'] = df_Market_Depth_Put['STRIKE_PRICE'].astype(float)
    df_Put_Call = pd.merge(df_Put_Call, df_Market_Depth_Put, how='inner', on='STRIKE_PRICE')

    ############################## CALL OPTION CHAIN LITE MARKET DEPTH CALL ###########################################
    mdsAdaptor = utils.getMDSAdaptor(Configuration)
    df_Market_Depth_Call = mdsAdaptor.getOptionChainLiteMarketDepth(Configuration, SP_LIST, 'CE', expiry_date, symbol)

    remove_column_list = ['buy', 'sell']
    df_Market_Depth_Call.drop(remove_column_list, axis=1, inplace=True)
    df_Market_Depth_Call.columns = [str(col).upper() + '_CALL' for col in df_Market_Depth_Call.columns]
    df_Market_Depth_Call['STRIKE_PRICE'] = df_Market_Depth_Call.index
    df_Market_Depth_Call.reset_index(inplace=True)
    df_Market_Depth_Call.drop(['index'], axis=1, inplace=True)
    df_Market_Depth_Call['STRIKE_PRICE'] = df_Market_Depth_Call['STRIKE_PRICE'].astype(float)
    df_Put_Call = pd.merge(df_Put_Call, df_Market_Depth_Call, how='inner', on='STRIKE_PRICE')

    ################################ VALIDATION ON BID ASK #############################################################
    if not isSquareOffJob and isValidationRequired and 'BID_PRICE_1_PUT' in df_Put_Call.columns:  # IN CASE DEPTH IS NOT THERE IN OPTION CHAIN
        df_positions_existing = orderGen_dict['df_positions_existing']

        # FILTERATION RULE 2: REMOVE EXISTING STRIKE PRICES OR ACTIVE ORDERS
        if not df_positions_existing.empty:
            #SP_ATM = df_positions_existing['strike_price'].iloc[0] # TAKE POSITION IN SAME STRIKE PRICE
            SP_List = df_positions_existing['strike_price_put'].tolist()
            SP_List = [float(SP) for SP in SP_List]
            df_Put_Call = df_Put_Call[~df_Put_Call['STRIKE_PRICE'].isin(SP_List)]
            df_Put_Call.reset_index(drop=True, inplace=True)

        ##################################### SELECT 1ST AND LAST ROW ##################################################
        #df_Put_Call = df_Put_Call.iloc[[0, len(df_Put_Call) - 1]]
        if df_Put_Call is None or len(df_Put_Call) == 0: return

    return df_Put_Call

def initializeGreeksIV(df_Put_Call):
    df_Put_Call['IV_PUT'], df_Put_Call['IV_CALL'], df_Put_Call['IV_PUT_OPT'], df_Put_Call['IV_CALL_OPT'], df_Put_Call[
        'DELTA_PUT'], df_Put_Call['DELTA_CALL'], df_Put_Call['NET_DELTA'], df_Put_Call['GAMMA_PUT'], df_Put_Call[
        'GAMMA_CALL'], df_Put_Call['NET_GAMMA'], df_Put_Call['IV_DIFF_PCT'], df_Put_Call[
        'IV_DIFF_PCT_OPT'] = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0

    return df_Put_Call


def populatePositionsDict(row_position, symbol, entry_params, position_group_id, position_id, expiry_date, orderGen_dict):

    # COMMON FIELDS
    position_dict = {
        'signal_id': str(int(position_id)),
        'signal_group_id': str(int(position_group_id)),
        'position_id': str(int(position_id)),
        'position_group_id': str(int(position_group_id)),
        'order_id': str(int(UniqueKeyGenerator().getUniqueKeyDateTime())),
        'order_group_id': orderGen_dict['order_group_id'],
        'moneyness': row_position['level_'+row_position['instrument_type'].lower()],
        'symbol': symbol,
        'expiry_date': expiry_date,
        'strike_price': int(row_position['strike_price']),
        'instrument_type': row_position['instrument_type'],
        'transaction_type': row_position['order_type_'+row_position['instrument_type'].lower()],
        'order_type': 'MARKET',
        'contract_type': orderGen_dict['contract_type'],
        'num_lots': row_position['multi_factor'],
        'lot_size': row_position['lot_size'],
        'quantity': row_position['quantity_'+row_position['instrument_type'].lower()],
        'is_active': True,
        'is_success': True,
        'margin_overall': row_position["margin_overall"],
        'entry_price': row_position['entry_price_'+row_position['instrument_type'].lower()],
        'exit_price': None,
        'execution_price': row_position['entry_price_' + row_position['instrument_type'].lower()],
        'params': entry_params,
        'realized_pnl': None,
        'realized_pnl_group': None,
        'realized_pnl_overall': None,
        'broker_order_id': "NA",
        'broker_order_status': "NA"
        }
    return position_dict

def fetchHistoricalPrices(symbol, Configuration):
    mdsAdaptor = utils.getMDSAdaptor(Configuration)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata'))
    current_date_str = current_date.strftime('%Y-%m-%d')
    current_time_str = current_date.strftime('%H:%M:%S')
    toDateTime = current_date_str+'+'+current_time_str

    yesterday_date = utils.getPastDateMinus(current_date, 1)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    yesterday_time_str = '12:00:00' # Taking historical data from yesterday 12 PM
    fromDateTime = yesterday_date_str + '+' + yesterday_time_str

    #2017-12-15+09:15:00
    ts_start = time.time()
    df_historical_data = mdsAdaptor.callHistoricalPricesAPI(fromDateTime,toDateTime,Configuration, symbol)
    ts_end = time.time()
    time_taken = ts_end - ts_start
    logging.info('{}: Time Taken by Historical prices API is : {}'.format(Configuration['SCHEMA_NAME'], str(time_taken)))

    return df_historical_data

def fetchTechnicalIndicators(orderGen_dict, Configuration):

    orderGen_dict = BTAdaptor.fetchTechnicalIndicators(orderGen_dict, Configuration)

    return orderGen_dict

def populateVIXGreeksIV(symbol, Configuration, df_Put_Call, orderGen_dict):

    # POPULATE VIX
    mdsAdaptor = utils.getMDSAdaptor(Configuration)
    current_date = datetime.now(pytz.timezone('Asia/Kolkata'))
    current_date_str = current_date.strftime('%Y-%m-%d')
    current_time_str = current_date.strftime('%H:%M')
    toDateTime = current_date_str + '+' + current_time_str+':00'
    fromDateTime = toDateTime

    # 2017-12-15+09:15:00
    VIX = mdsAdaptor.callHistoricalPricesAPIForVIX(fromDateTime, toDateTime, Configuration, symbol="VIX")
    df_Put_Call['VIX'] = VIX
    orderGen_dict['VIX'] = VIX

    # POPULATE GREEKS
    expiry_date_greeks = orderGen_dict['expiry_date']
    expiry_date_greeks = datetime.strptime(expiry_date_greeks, '%y%b%d').strftime('%Y%b%d')
    current_date_greeks = current_date.strftime('%Y%b%d')
    current_time_greeks = current_date.strftime('%H:%M')
    net_delta = 0.0
    for index, row in df_Put_Call.iterrows():
        instrument_type = getInstrumentTypeGreeks(row['INSTRUMENT_TYPE'])
        option_price = row['ENTRY_PRICE_'+row['INSTRUMENT_TYPE']]
        underlying_spot = orderGen_dict['spot_value']
        strike_price = row['STRIKE_PRICE']
        order_type = row['ORDER_TYPE_' + row['INSTRUMENT_TYPE']]
        num_lots = float(row['MULTI_FACTOR'])
        greeksSign = 1

        # ADD SIGN TO DELTA
        # FOR LONG DELTA POSITIVE, FOR SHORT DELTA NEGATIVE
        if order_type == 'SELL':
            greeksSign = -1

        # CALL GREEKS API
        iv, delta, gamma, theta, vega = GreeksAdaptor.fetchGreeks(expiry_date_greeks, current_date_greeks, current_time_greeks, instrument_type,
                                  option_price,underlying_spot,strike_price,order_type, Configuration, raiseException=False)

        # ADD SIGN TO DELTA
        # FOR LONG DELTA POSITIVE, FOR SHORT DELTA NEGATIVE
        # if (instrument_type == 'CE' and order_type == 'SELL') or (instrument_type == 'PE' and order_type == 'BUY'):
        #     delta = -delta

        # UPDATE GREEKS WITH LOTS AND SIGN
        delta = greeksSign * delta * num_lots
        gamma = greeksSign * gamma * num_lots
        theta = greeksSign * theta * num_lots
        vega = greeksSign * vega * num_lots

        # ADD GREEKS TO DATAFRAME
        df_Put_Call.at[index, 'IV_' + row['INSTRUMENT_TYPE']] = iv
        df_Put_Call.at[index, 'DELTA_' + row['INSTRUMENT_TYPE']] = delta
        df_Put_Call.at[index, 'GAMMA_' + row['INSTRUMENT_TYPE']] = gamma
        df_Put_Call.at[index, 'THETA_' + row['INSTRUMENT_TYPE']] = theta
        df_Put_Call.at[index, 'VEGA_' + row['INSTRUMENT_TYPE']] = vega

        # NET_DELTA
        net_delta = net_delta + delta

    # ADD NET GREEKS VALUES
    df_Put_Call['NET_DELTA'] = net_delta
    df_Put_Call = df_Put_Call.apply(lambda row: populateIndividualTheta(row), axis=1)
    df_Put_Call['NET_THETA'] = df_Put_Call['THETA_INDIVIDUAL'].sum()
    df_Put_Call['NET_THETA_ONE_MF'] = df_Put_Call['NET_THETA'] / df_Put_Call['MULTI_FACTOR']
    df_Put_Call = df_Put_Call.apply(lambda row: populateExecutionThetaPnlPending(row, Configuration, orderGen_dict),
                                    axis=1)

    df_Put_Call = df_Put_Call.apply(lambda row: populateIndividualVega(row), axis=1)
    df_Put_Call['NET_VEGA'] = df_Put_Call['VEGA_INDIVIDUAL'].sum()

    return df_Put_Call

def getInstrumentTypeGreeks(instrument_type):
    if instrument_type == 'PUT':
        return 'PE'
    else:
        return 'CE'

def populateExecutionThetaPnlPending(row, Configuration, orderGen_dict):
    #EXISTING_EXECUTION_PNL_PENDING = orderGen_dict['EXISTING_EXECUTION_PNL_PENDING']
    symbol = orderGen_dict['symbol']
    NET_PNL_PENDING = utils.calculateThetaPnlPending(row['NET_THETA'], Configuration, symbol)

    row['NET_PNL_PENDING'] = NET_PNL_PENDING
    return row

def populateIndividualTheta(row):
    row['THETA_INDIVIDUAL'] = 0.0
    if 'INSTRUMENT_TYPE' in row.keys():
        if row['INSTRUMENT_TYPE'] == 'PUT':
            row['THETA_INDIVIDUAL'] = row['THETA_PUT']
        else:
            row['THETA_INDIVIDUAL'] = row['THETA_CALL']

    return row

def populateIndividualVega(row):
    row['VEGA_INDIVIDUAL'] = 0.0
    if 'INSTRUMENT_TYPE' in row.keys():
        if row['INSTRUMENT_TYPE'] == 'PUT':
            row['VEGA_INDIVIDUAL'] = row['VEGA_PUT']
        else:
            row['VEGA_INDIVIDUAL'] = row['VEGA_CALL']

    return row

if __name__ == "__main__":
   print(populateLotsPerUnit(-0.57))