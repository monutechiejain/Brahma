import logging
import traceback
import time
from service import OrderGenOptionsService, SquareOffOptionsService, MarginFreeUpService, OptionsAdjustmentService
import pytz
from dao import PositionsDAO, PositionsTrackingBackUpDAO
from datetime import datetime, date
from common import utils, TrackerUtil
from common.UniqueKeyGenerator import UniqueKeyGenerator


class OrderGenOrchestrator:

    def __init__(self):
        print('Initializing inside OrderGenOrchestrator')

    def placeOrders(self, **kwargs):
        symbol = kwargs['symbol']
        contract_type = kwargs['contract_type']
        Configuration = kwargs['Configuration']
        schema_name = kwargs['schema_name']
        ############################# 1. ORDER GEN OPTIONS SERVICE  ##########################################
        df_active_positions_options = PositionsDAO.getActivePositionsBySymbolAndContractType(schema_name, symbol, contract_type)

        squareOff_dict = {}
        squareOff_dict['isRunJobWithoutDelay'] = False
        squareOff_dict['isSquareOff'] = False

        ############################################# GET UNIQUE ID #####################################################
        if len(df_active_positions_options) == 0:
            position_group_id = UniqueKeyGenerator().getUniqueKeyDateTime()

        else:
            df_active_positions_options.columns = [x.lower() for x in df_active_positions_options.columns]
            position_group_id = df_active_positions_options['position_group_id'].iloc[0]

            # CHECK IF POSITION TAKEN FROM PREVIOUS DAY
            positions_created_date = position_group_id[0:6]

            positions_created_date = datetime.strptime(positions_created_date, '%y%m%d')
            positions_created_date = date(positions_created_date.year, positions_created_date.month, positions_created_date.day)

            isTodayDate = utils.isTodayDate(positions_created_date)

            # Reset Configurations
            if not isTodayDate and not utils.isBackTestingEnv(Configuration):
                logging.info('Previous Day Positions Marking Inactive {} for symbol {}'.format(schema_name, str(symbol)))
                df_active_positions_options['is_active'] = False
                for index, row_position in df_active_positions_options.iterrows():
                    # UPDATE POSITIONS TABLE
                    PositionsDAO.updatePositionsMarkInactive(schema_name, row_position)
                return squareOff_dict

        ################################################################################################################

        kwargs['position_group_id'] = position_group_id
        try:

            ts_start = time.time()
            stopOrderGenTime = Configuration['STOP_ORDER_GEN_TIME_1']
            stopHour, stopMinute = stopOrderGenTime.split(':')
            current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
            #current_time = '11:25'
            hour, minute = current_time.split(':')

            ############################## STEPS FOR ORDERGEN OPTIONS/ SQUAREOFF/ ORDERGEN FUTURES #####################
            # 1. CHECK EXISTING POSITIONS, IF NONE START ORDERGEN OPTIONS
            # 2. IF EXISTING POSITIONS, START SQUAREOFF
            # 3. BASED ON INPUTS FROM SQUAREOFF DICT, PLACE ORDERGEN FUTURES

            # MOCK DATA
            #df_positions_options = pd.DataFrame()
            if utils.isMockAndLocalEnv(Configuration) or utils.isBackTestingEnv(Configuration):
                stopHour, stopMinute = '24', '59'
                Configuration['STOP_ORDER_GEN_TIME_1'] = '24:59'
                Configuration['STOP_ORDER_GEN_TIME_2'] = '24:59'

            if (Configuration['STRATEGY_TYPE'] == 'INTRADAY'
                                        and not ((int(hour) == int(stopHour) and int(minute) >= int(stopMinute))
                                        or int(hour) > int(stopHour))
                                        and not len(df_active_positions_options) > 0):

                # CHECK BACKUP
                df_positions_tracking_backup_last_itr = PositionsTrackingBackUpDAO.getPositionsByLatestPositionTrackingGroupIdAndSymbol(schema_name, symbol)
                df_positions_tracking_backup_last_itr.sort_values(by=['ID'], ascending=False, inplace=True)

                # CHECK IF POSITIONS TAKEN ALREADY FOR TODAY
                if len(df_positions_tracking_backup_last_itr)>0:
                    latest_position_group_id = df_positions_tracking_backup_last_itr['POSITION_GROUP_ID'].iloc[0]
                    positions_created_date = latest_position_group_id[0:6]

                    positions_created_date = datetime.strptime(positions_created_date, '%y%m%d')
                    positions_created_date = date(positions_created_date.year, positions_created_date.month, positions_created_date.day)

                    isTodayDate = utils.isTodayDate(positions_created_date)

                    # Reset Configurations
                    if not isTodayDate or utils.isBackTestingEnv(Configuration):
                            orderGen_dict = OrderGenOptionsService.orderGenOptions(**kwargs)
                    else:
                        logging.info('Intraday Positions Already Closed for the {} for symbol {}'.format(schema_name,str(symbol)))
                        return squareOff_dict
                else:
                    orderGen_dict = OrderGenOptionsService.orderGenOptions(**kwargs)

                # CHECK EXISTING POSITIONS
                df_positions_existing = orderGen_dict['df_positions_existing']
                if len(df_positions_existing) == 0:
                    return squareOff_dict

            ############################# 2. SQUARE OFF JOB ############################################################
            squareOff_dict = SquareOffOptionsService.squareOffOptions(**kwargs)
            if squareOff_dict is not None and squareOff_dict['isSquareOff'] == False and squareOff_dict['isRunJobWithoutDelay']:
                return squareOff_dict

            ############################# 3. MARGIN FREE UP SERVICE ####################################################
            isMarginFreeUpAllowed = False
            if squareOff_dict is not None and squareOff_dict['isSquareOff'] == False:
                squareOff_dict = MarginFreeUpService.marginFreeUp(**kwargs, squareOff_dict=squareOff_dict)
                isMarginFreeUpAllowed = squareOff_dict['isMarginFreeUpAllowed']

            ############################# 4. OPTIONS HEDGING SERVICE ###################################################
            if squareOff_dict is not None and squareOff_dict['isSquareOff'] == False and not isMarginFreeUpAllowed:
                optionsAdjustment_dict, squareOff_dict = OptionsAdjustmentService.orderGenOptionsAdjustment(**kwargs, squareOff_dict=squareOff_dict)
                squareOff_dict['isRunJobWithoutDelay'] = optionsAdjustment_dict['isRunJobWithoutDelay']

            ############################# 5. POPULATE TRACKER TABLE ###################################################
            try:
                if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT']:
                    if squareOff_dict is not None:
                        TrackerUtil.insertTrackerPositions(Configuration, squareOff_dict)
            except Exception as ex:
                template = "Exception {} occurred while Populating Tracker Table with  message : {}"
                message = template.format(type(ex).__name__, ex.args)
                print(message)
                logging.info(traceback.format_exc())
                print(traceback.format_exc())
                logging.info(message)
            ##################################################################################################################

            ts_end = time.time()
            time_taken = ts_end - ts_start
            print('Time Taken to run OrderGen job {} FRESH for symbol {} is {}'.format(schema_name,str(symbol), str(time_taken)))
            logging.info('Time Taken to run OrderGen job FRESH {} for symbol {} is {}'.format(schema_name,str(symbol), str(time_taken)))

        except Exception as ex:
            template = "Exception {} occurred with message : {}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            logging.info(traceback.format_exc())
            print(traceback.format_exc())
            logging.info(message)

            # SEND MAIL IN CASE OF FAILURE
            self.sendMailOrderGenFailure(Configuration, ex, message, symbol)

        return squareOff_dict


    # SEND MAIL IN CASE OF FAILURES - PARENT METHOD
    def sendMailOrderGenFailure(self, Configuration, ex, message, symbol):
        try:
            if 'PRD' in Configuration['ENVIRONMENT'] and Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                if len(ex.args) > 0:
                    message = '<b>' + str(ex.args[0]) + '<b>'
                subject = "FAILURE | SYMBOL : " + symbol + " | STRATEGY : " + Configuration['SCHEMA_NAME']
                utils.send_email_dqns(Configuration, subject, message, "HTML")
                utils.send_sns(Configuration)
        except Exception as ex:
            template = "Exception {} occurred with message : {}"
            message = template.format(type(ex).__name__, ex.args)
            print(message)
            logging.info(traceback.format_exc())
            print(traceback.format_exc())
            logging.info(message)
