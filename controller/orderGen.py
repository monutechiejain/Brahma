from controller.OrderGenOrchestrator import OrderGenOrchestrator
from common.constants import PROFILE_PROPERTIES
from dao import GlobalConfigurationDAO, LiveAccountsDAO, ClientConfigurationDAO, PositionsBackUpDAO, \
    PositionsTrackingDAO, PositionsTrackingBackUpDAO, PositionsDAO
import logging
import pytz
import time
from datetime import datetime, date
import calendar
import traceback
from common import utils
from setup import setup
from concurrent.futures import ThreadPoolExecutor
from config.cache.BasicCache import BasicCache

def placeOrders():

    # Commenting below line as setup will be run only once when onboarding the client ,
    # After that to change any property during runtime we have ti use AOI to update directly in database
    # FETCH GLOBAL CONFIGS AND SET UP CONFIG
    # if not utils.isBackTestingEnvProfile(PROFILE_PROPERTIES['SCHEMA_NAME']):
    #     df_configurations_result = GlobalConfigurationDAO.fetchAllConfiguartions(PROFILE_PROPERTIES['SCHEMA_NAME'])
    #     if len(df_configurations_result) > 0:
    #         conf_created_date_time = datetime.strptime(str(df_configurations_result['CREATED_DATE_TIME'].iloc[0]), '%Y-%m-%d %H:%M:%S')
    #         conf_created_date = date(conf_created_date_time.year, conf_created_date_time.month, conf_created_date_time.day)

    #         isTodayDate = utils.isTodayDate(conf_created_date)

    #         # Reset Configurations
    #         if not isTodayDate:
    #             setup.configSetUp()
    #     else:
    #         setup.configSetUp()

    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
    my_date = date.today()
    weekday = calendar.day_name[my_date.weekday()]
    weekday_uppercase = weekday.upper()

    # FETCH GLOBAL CONFIGS
    global_configuration_dict = GlobalConfigurationDAO.getConfigurations(PROFILE_PROPERTIES['SCHEMA_NAME'])

    # FETCH CUSTOMERS
    df_live_strategy = LiveAccountsDAO.fetchCustomers()
    customers_list = df_live_strategy['CLUSTER_ID'].tolist()

    ####################################################################################################################
    # MOCK: LOCAL PRD TESTING
    #customers_list = ['BRAHMA_V1_PRE_PRD_KG']
    ####################################################################################################################

    logging.info("\n Customers List :: " + str(customers_list))

    # MULTITHREADING FOR ALL CUSTOMERS  SINGLE THREAD PER CUSTOMER
    pool = ThreadPoolExecutor(max_workers=len(customers_list))

    # POPULATE CLIENT CONFIGS AND PERFORM OPERATIONS ON EACH CUSTOMER
    for customer in customers_list:
        # MULTITHREADING IN PRD
        pool.submit(orderOrchestration, customer, global_configuration_dict, weekday_uppercase)

        # LOCAL -> SEQUENTIAL
        #orderOrchestration(customer, global_configuration_dict, weekday_uppercase)

    pool.shutdown(wait=True)
    ####################################################################################################################


def orderOrchestration(customer, global_configuration_dict, weekday_uppercase):
    schema_name = customer
    client_configuration_dict = ClientConfigurationDAO.getConfigurations(schema_name)
    logging.info("\n{}: Order Gen Starts for customer :: {}".format(schema_name, schema_name))
    # MERGE 2 DICTIONARIES
    Configuration = {**global_configuration_dict, **client_configuration_dict}
    Configuration['SCHEMA_NAME'] = schema_name

    ############################################### FORM OM URL ########################################################
    Configuration['OM_URL'] = Configuration['OM_URL']+":"+Configuration['OM_PORT_'+Configuration['BROKER'].upper()]
    ####################################################################################################################

    if Configuration['ORDER_GENERATION_ACTIVE'] == 'N':
        ts_start = time.time()
        # TODO : UMCOMMENT AFTER DEV COMPLETION
        # SET ORDER_GENERATION_ACTIVE AS Y
        #ClientConfigurationDAO.updateConfiguration(schema_name, 'ORDER_GENERATION_ACTIVE', 'Y') #LOCK REMOVED
        orderGenOrchestrator = OrderGenOrchestrator()

        # ITERATING THROUGH ALL SYMBOLS FOR PLACING ORDER
        symbols_list_str = Configuration['SYMBOLS_LIST_' + weekday_uppercase]
        symbols_list = symbols_list_str.split(',')
        for symbol in symbols_list:
            symbol, contract_type = symbol.split('_')
            if Configuration['SYMBOL_ACTIVE_' + symbol] == 'Y':

                ##################################### PLACE ORDERS ######################################################
                squareOff_dict = orderGenOrchestrator.placeOrders(symbol=symbol, contract_type=contract_type,
                                                                  Configuration=Configuration, schema_name=customer)

                # RUN JOB WITHOUT DELAY, RUNNING AGAIN, NOT GOING BACK TO SCHEDULER
                if squareOff_dict is not None and squareOff_dict['isRunJobWithoutDelay']:
                    squareOff_dict = orderGenOrchestrator.placeOrders(symbol=symbol, contract_type=contract_type,
                                                                      Configuration=Configuration)

                # CLEAN UP POST SQUAREOFF_FINAL
                # 1. MOVE POSITIONS FROM POSITIONS TO POSITIONS_BACK_UP TABLE
                # 2. DELETE POSITIONS TABLE
                # 2. MOVE POSITIONS_TRACKING DATA TO POSITIONS_TRACKING_BACK_UP TABLE
                EXIT_TIME = Configuration['EXIT_TIME'].split(",")
                EXIT_TIME = EXIT_TIME[-2:]
                current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
                if (squareOff_dict is not None and squareOff_dict['isSquareOff'] == True) or current_time in EXIT_TIME:
                    # CLEAN UP
                    # cleanUpActivity(Configuration, weekday, current_time, squareOff_dict, schema_name)
                    cleanUpActivity(Configuration, schema_name)

                    # SET isSquareOff True for Backtesting once squaredoff
                    if utils.isBackTestingEnv(Configuration):
                        BasicCache().set('isSquareOff', True)

                # break   # TODO Comment post testing - ONLY FOR TESTING NIFTY ONLY

        # SET ORDER_GENERATION_ACTIVE AS N
        ClientConfigurationDAO.updateConfiguration(customer, 'ORDER_GENERATION_ACTIVE', 'N')

        ts_end = time.time()
        time_taken = ts_end - ts_start
        print('{}: Time Taken to run ORDERGEN JOB OVERALL is : {}'.format(customer, str(time_taken)))
        logging.info('{}: Time Taken to run ORDERGEN JOB OVERALL is : {}'.format(customer, str(time_taken)))
    else:
        print('{}: ORDER GENERATION IS ALREADY ACTIVE. PLEASE TRY AFTER SOME TIME'.format(customer))
        logging.info('{}: ORDER GENERATION IS ALREADY ACTIVE. PLEASE TRY A'
                     'FTER SOME TIME'.format(customer))


def cleanUpActivity(Configuration, schema_name):
    '''
    delete Positions Table and copy data in Positions Audit
    :param df_positions:
    :return:
    '''
    try:
        ################################ POPULATE POSITIONS AUDIT/BAK ###################################################
        df_positions_all = PositionsDAO.getPositionsAll(schema_name)

        if len(df_positions_all) > 0 :

            # DELETE ALL FROM POSITIONS TABLE
            PositionsDAO.deletePositionsAll(schema_name)
            logging.info("Deleted Successfully from Positions Table")

            # FULL DELETE FROM POSITIONS TRACKING TABLE
            PositionsTrackingDAO.deletePositionsTrackingAll(schema_name)
            logging.info("Deleted Successfully from Positions Tracking Table By Clean Up Job.")
            ########################################################################################################

    except Exception as ex:
        template = "Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(traceback.format_exc())
        print(traceback.format_exc())
        logging.info(message)

        # SEND MAIL IN CASE OF FAILURE
        sendMailCleanUpFailure(Configuration, ex, message)

# def cleanUpActivity(Configuration, weekday, current_time, squareOff_dict, schema_name):
#     '''
#     delete Positions Table and copy data in Positions Audit
#     :param df_positions:
#     :return:
#     '''
#     try:
#         ################################ POPULATE POSITIONS AUDIT/BAK ###################################################
#         df_positions_all = PositionsDAO.getPositionsAll(schema_name)
#         df_positions_all = df_positions_all.replace({np.nan: None})
#         # Convert all columns to lowercase
#         df_positions_all.columns = [x.lower() for x in df_positions_all.columns]
#         position_group_id = squareOff_dict['position_group_id']
#
#         if len(df_positions_all) > 0 :
#             position_id_list = []
#             for index, row_position in df_positions_all.iterrows():
#                 PositionsBackUpDAO.insert(row=row_position, schema_name=schema_name)
#                 position_id_list.append(row_position['position_id'])
#
#             print('Inserted Successfully in PositionsBackUp Table By Clean Up Job.')
#             logging.info("Inserted Successfully in PositionsBackUp Table By Clean Up Job.")
#
#             # DELETE FROM POSITIONS TABLE
#             PositionsDAO.deletePositionsByPositionIds(schema_name, position_id_list)
#
#             # DELETE ALL FROM POSITIONS TABLE
#             PositionsDAO.deletePositionsAll(schema_name)
#             logging.info("Deleted Successfully from Positions Table")
#
#             ################################ BAKUP POSITIONS TRACKING TABLE ############################################
#             df_positions_tracking_all = PositionsTrackingDAO.getPositionsByPositionGroupId(schema_name, position_group_id)
#             df_positions_tracking_all = df_positions_tracking_all.replace({np.nan: None})
#             # Convert all columns to lowercase
#             df_positions_tracking_all.columns = [x.lower() for x in df_positions_tracking_all.columns]
#             if len(df_positions_tracking_all) > 0:
#                 position_tracking_list = []
#                 for index, row in df_positions_tracking_all.iterrows():
#                     PositionsTrackingBackUpDAO.insert(row = row, schema_name=schema_name)
#                     position_tracking_list.append(row['position_tracking_id'])
#
#                 print('Inserted Successfully in Position Tracking Back Up Table By Clean Up Job.')
#                 logging.info("Inserted Successfully in Position Tracking Back Up Table By Clean Up Job.")
#
#                 # DELETE FROM POSITIONS TABLE
#                 PositionsTrackingDAO.deletePositionsByPositionGroupId(schema_name, position_group_id)
#
#                 # FULL DELETE FROM POSITIONS TRACKING TABLE
#                 PositionsTrackingDAO.deletePositionsTrackingAll(schema_name)
#                 logging.info("Deleted Successfully from Positions Tracking Table By Clean Up Job.")
#             ########################################################################################################
#
#     except Exception as ex:
#         template = "Exception {} occurred with message : {}"
#         message = template.format(type(ex).__name__, ex.args)
#         print(message)
#         logging.info(traceback.format_exc())
#         print(traceback.format_exc())
#         logging.info(message)
#
#         # SEND MAIL IN CASE OF FAILURE
#         sendMailCleanUpFailure(Configuration, ex, message)

def sendMailCleanUpFailure(Configuration, ex, message):
    try:
        if 'PRD' in Configuration['ENVIRONMENT'] and Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
            if len(ex.args) > 0:
                message = '<b>' + str(ex.args[0]) + '<b>'
            subject = "FAILURE | CLEAN_UP_ACTIVITY | STRATEGY : " + Configuration['SCHEMA_NAME']
            utils.send_email_dqns(Configuration, subject, message, "HTML")
    except Exception as ex:
        template = "Exception {} occurred with message : {}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        logging.info(traceback.format_exc())
        print(traceback.format_exc())
        logging.info(message)


if __name__ == "__main__":
    from testing import TestGenericOrderGen
    from config.logging.LogConfig import LogConfig
    LogConfig()
    TestGenericOrderGen.setDefaultMockProperties()
    placeOrders()