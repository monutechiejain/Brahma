
from testing.backtesting.BackTestingConstants import EXPIRY_DATES
from testing.backtesting import BackTestingHelper
from config.cache.BasicCache import BasicCache
from datetime import date, datetime, timedelta
import logging
from controller import orderGen
from common.constants import PROFILE_PROPERTIES
from dao import GlobalConfigurationDAO,LiveAccountsDAO,ClientConfigurationDAO

def initiateBackTesting(backtesting_dict):
    year = backtesting_dict['year']
    month = backtesting_dict['month']
    start_time = backtesting_dict['start_time']
    end_time = backtesting_dict['end_time']
    symbol = backtesting_dict['symbol']
    current_date = backtesting_dict['current_date']
    today = backtesting_dict['day']
    current_expiry = backtesting_dict['current_expiry']
    next_expiry = backtesting_dict['next_expiry']

    df_configurations_result = GlobalConfigurationDAO.fetchAllConfiguartions(PROFILE_PROPERTIES['SCHEMA_NAME'])
    global_configuration_dict = GlobalConfigurationDAO.getConfigurations(PROFILE_PROPERTIES['SCHEMA_NAME'])
    df_live_strategy = LiveAccountsDAO.fetchCustomers()
    customer = df_live_strategy['CLUSTER_ID'].tolist()[0]
    schema_name = customer
    client_configuration_dict = ClientConfigurationDAO.getConfigurations(schema_name)
    logging.info("\n{}: Order Gen Starts for customer :: {}".format(schema_name, schema_name))
    # MERGE 2 DICTIONARIES
    Configuration = {**global_configuration_dict, **client_configuration_dict}
    symbols_list_str = Configuration['SYMBOLS_LIST_' + backtesting_dict['day'].upper()]
    symbol, contract_type = symbols_list_str.split('_')
    if contract_type == 'WEEKLY':
        expiry = current_expiry
        expiry = expiry.strftime('%Y%b%d').upper()
    else:
        expiry = next_expiry
        expiry = expiry.strftime('%Y%b%d').upper()

    # EXPIRY DATES LIST FROM CONSTANTS FILE
    # EXPIRY_DATES_LIST = EXPIRY_DATES[year][month][week]
    #
    # EXPIRY_DATE_ENTRY_DATE_STR = EXPIRY_DATES_LIST[0]
    # EXPIRY_DATE_EXIT_DATE_STR = EXPIRY_DATES_LIST[1]
    # EXPIRY_DATE_MONTHLY_DATE_STR = EXPIRY_DATES_LIST[2]
    #
    # # CONVERT DATE STRING TO DATE AND FETCH POSITION START AND END DATE
    # POSITIONS_START_DATE, POSITIONS_END_DATE, \
    # EXPIRY_DATE_ENTRY_DATE, EXPIRY_DATE_EXIT_DATE = BackTestingHelper.getPositionDates(EXPIRY_DATE_ENTRY_DATE_STR,
    #                                                                                    EXPIRY_DATE_EXIT_DATE_STR,
    #                                                                                    backtesting_dict)
    #
    # # POSITIONS START AND END TIME FROM CONFIG
    # POSITIONS_START_TIME =  backtesting_dict['start_time']
    # POSITIONS_START_TIME_V1 = backtesting_dict['start_time_v1']
    # POSITIONS_END_TIME = backtesting_dict['end_time']
    #
    # POSITIONS_START_DATE_STR = POSITIONS_START_DATE.strftime('%Y-%m-%d').upper()
    # EXPIRY = EXPIRY_DATE_EXIT_DATE.strftime('%y%b%d').upper()
    #
    # # SET HEADERS AND EXPIRY DATES IN BASIC CACHE
    # EXPIRY_DATE_MONTHLY_DATE = datetime.strptime(EXPIRY_DATE_MONTHLY_DATE_STR, '%y%b%d')
    # EXPIRY_DATE_MONTHLY_DATE = date(EXPIRY_DATE_MONTHLY_DATE.year, EXPIRY_DATE_MONTHLY_DATE.month, EXPIRY_DATE_MONTHLY_DATE.day)
    #



    POSITIONS_START_TIME =  backtesting_dict['start_time']
    POSITIONS_START_DATE_STR = current_date.strftime('%Y-%m-%d').upper()
    DATE_TIME = POSITIONS_START_DATE_STR + ' ' + POSITIONS_START_TIME

    yesterday_date = BackTestingHelper.getPastDateMinus(current_date, 1, backtesting_dict)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    yesterday_time_str = '12:00:00'  # Taking historical data from yesterday 12 PM
    fromDateTime = yesterday_date_str + '+' + yesterday_time_str

    # MOCK DATA
    #DATE_TIME = '2021-8-10 14:00:00'

    # SET CACHE DEFAULT VALUES, WORKS LIKE DICTIONARY
    BasicCache().set('CURRENT_DATE', current_date)
    BasicCache().set('CURRENT_DATE_TIME', datetime(current_date.year, current_date.month, current_date.day))
    BasicCache().set('CURRENT_TIME', start_time)
    BasicCache().set('isSquareOff', False)
    BasicCache().set('DATE_TIME', DATE_TIME)
    BasicCache().set('FROM_DATE_TIME', fromDateTime)
    #BasicCache().set('CURRENT_DATE_TIME', DATE_TIME)
    BasicCache().set('CURRENT_EXPIRY', current_expiry)
    BasicCache().set('NEXT_EXPIRY', next_expiry)
    BasicCache().set('EXPIRY', expiry)
    BasicCache().set('SYMBOL', symbol)
    #BasicCache().set('EXPIRY_DATE_OPTIONS', EXPIRY_DATE_EXIT_DATE.strftime('%y%b%d').upper())
    #BasicCache().set('EXPIRY_DATE_FUTURES', EXPIRY_DATE_MONTHLY_DATE.strftime('%y%b%d').upper())
    #BasicCache().set('ATM_PRICE_LOWER_RANGE', 300)
    #BasicCache().set('ATM_PRICE_UPPER_RANGE', 800)

    ########################################### ORDER GEN/POSITIONS TRACKING LOOP ######################################
    # IT WILL BREAK ONCE CURRENT WEEK POSITIONS ARE SQUARED OFF
    while(True):

        logging.info('##################################################################################################')
        logging.info('CURRENT_DATE_TIME : {}, CURRENT_EXPIRY : {}, NEXT_EXPIRY : {}'.format(
            BasicCache().get('DATE_TIME'), BasicCache().get('CURRENT_EXPIRY'), BasicCache().get('NEXT_EXPIRY')))
        logging.info('##################################################################################################')

        ################################################## CALL ORDER GEN ##############################################
        orderGen.placeOrders()

        # FETCH NEXT DATE TIME, Add 5 minutes to CURRENT TIME, IN CASE ITS MARKET CLOSE TIME, ADD DAY SKIPPING WEEKENDS/HOLIDAYS
        # MOCK DATA
        #DATE_TIME = '2021-8-10 14:00:00'

        backtesting_dict['DATE_TIME'] = DATE_TIME
        DATE_TIME, DATE_TIME_FINAL, TIME_FINAL_STR = BackTestingHelper.getNextDateTime(backtesting_dict, year)
        BasicCache().set('DATE_TIME', DATE_TIME)
        BasicCache().set('CURRENT_DATE_TIME', DATE_TIME_FINAL)
        BasicCache().set('CURRENT_DATE', DATE_TIME_FINAL.date())
        BasicCache().set('CURRENT_TIME', TIME_FINAL_STR)

        # EXIT THIS WEEK ONCE SQUAREDOFF AND CLEAN UP JOB HAS BEEN RUN
        if BasicCache().get('isSquareOff'):
            break

    pass
