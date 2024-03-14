from controller import orderGen
import time
from datetime import datetime
import pytz
import logging
import traceback
from common import utils
from common.constants import PROFILE_PROPERTIES
from dao import GlobalConfigurationDAO

############################################### SCHEDULER CONSTANTS  ##################################################
MARKET_CLOSE_TIME = ['15:30','15:31','15:32',
                     '16:00', '17:00', '18:00', '19:00',
                     '20:00']  # IST Times

# GET CONFIGURATIONS
global_configuration_dict = GlobalConfigurationDAO.getConfigurations(PROFILE_PROPERTIES['SCHEMA_NAME'])
Configuration = global_configuration_dict

# TADING START TIME
tradingStartTime = Configuration['TRADING_START_TIME']
tradingStartHour, tradingStartMinute = tradingStartTime.split(':')

# JOB SCHEDULE INTERVAL
JOB_SCHEDULE_INTERVAL = int(Configuration['JOB_SCHEDULE_INTERVAL'])

# TRADING STOP DAY
IS_TRADING_STOP_DAY = Configuration['IS_TRADING_STOP_DAY']

####################################### TRADING STOP DAYS  #############################################################
if 'PRD' in PROFILE_PROPERTIES['ACTIVE_ENV']:
    TRADING_STOP_DAYS_STR = Configuration['TRADING_STOP_DAYS']
    if TRADING_STOP_DAYS_STR is not None and TRADING_STOP_DAYS_STR and TRADING_STOP_DAYS_STR != '':
        TRADING_STOP_DAYS = TRADING_STOP_DAYS_STR.split(',')
    else:
        TRADING_STOP_DAYS = []
else:
    TRADING_STOP_DAYS = []
########################################################################################################################


####################################### TRADING STOP DAYS LOCAL #########################################################
if 'PRD' in PROFILE_PROPERTIES['ACTIVE_ENV']:
    TRADING_STOP_DAYS_LOCAL_STR = Configuration['TRADING_STOP_DAYS_LOCAL']
    if TRADING_STOP_DAYS_LOCAL_STR is not None and TRADING_STOP_DAYS_LOCAL_STR and TRADING_STOP_DAYS_LOCAL_STR != '':
        TRADING_STOP_DAYS_LOCAL = TRADING_STOP_DAYS_LOCAL_STR.split(',')
    else:
        TRADING_STOP_DAYS_LOCAL = []
else:
    TRADING_STOP_DAYS_LOCAL = []
########################################################################################################################

def startUp():
    #while (True):
        try:
            from config.logging.LogConfig import LogConfig
            LogConfig()

            ######################################## POPULATE IS_TRADING_HALT_TODAY ########################################
            if len(TRADING_STOP_DAYS) > 0:
                isTradingHaltToday = utils.checkIfTodayisParticularDayFromList(TRADING_STOP_DAYS)
            else:
                isTradingHaltToday = False
            ################################################################################################################

            ######################################## POPULATE IS_TRADING_HALT_TODAY_LOCAL ##################################
            if not isTradingHaltToday:
                if len(TRADING_STOP_DAYS_LOCAL) > 0:
                    isTradingHaltToday = utils.checkIfTodayisParticularDayFromList(TRADING_STOP_DAYS_LOCAL)
                else:
                    isTradingHaltToday = False
            ################################################################################################################

            ################################# EXIT IF TRADING HALT DAY OR HOLIDAY ##########################################
            if utils.isHolidayAndWeekendToday() or isTradingHaltToday or IS_TRADING_STOP_DAY == 'Y':
                logging.info("Its Holiday or TradingHaltDay : Exiting from Scheduler.......................")
                #break
            ################################################################################################################

            ###################################################### CALL ORDER GEN JOB ###################################
            current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
            hour, minute = current_time.split(':')

            # MARKET OPEN TIME - RUN SCHEDULER
            #############################################################################################################
            if (float(hour) == float(tradingStartHour) and float(minute) >= float(tradingStartMinute)) or (
                    float(hour) >= 10 and float(hour) <= 16):
                # Starting Order Gen Scheduler
                max_retries = 4
                retry = 0
                while (retry < max_retries):
                    try:
                        orderGen.placeOrders()
                    except Exception as ex:
                        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                        message = template.format(type(ex).__name__, ex.args)
                        print(message)
                        logging.info(message)
                        logging.info(traceback.format_exc())

                    # RETRY MECHANISM
                    time.sleep(2)
                    retry = retry + 1
                    if retry == max_retries:
                        break

            ############################################################################################################

            # MARKET CLOSE TIME - STOP SCHEDULER
            if float(hour) >= 15 and float(minute) >= 28:
                # if True:
                logging.info("MARKET_CLOSE_TIME : Exiting from Scheduler.......................")
                #break
            ################################################################################################################

            ######################################## SCHEDULING ORDER GEN JOB AFTER SOME INTERVAL ######################
            #time.sleep(JOB_SCHEDULE_INTERVAL)

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            logging.info(message)
            logging.info(traceback.format_exc())

def lambda_handler(event, context):
    startUp()

if __name__ == "__main__":
    from testing import TestGenericOrderGen
    from config.logging.LogConfig import LogConfig
    LogConfig()
    TestGenericOrderGen.setDefaultMockProperties()
    startUp()