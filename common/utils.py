import calendar
import logging
import smtplib
import traceback
from datetime import date, timedelta
from datetime import datetime
import requests
import time

import numpy as np
import pytz
from pytz import timezone


from bisect import bisect_left
from adaptor.MDSAdaptor import MDSAdaptor
from adaptor.MDSAdaptorTest import MDSAdaptor as MDSAdaptorTest
from adaptor.MDSAdaptorBackTesting import MDSAdaptor as MDSAdaptorBackTesting
from adaptor import OMServiceCallHelper, OMServiceCallHelperTest
from common import HolidayUtil

# FOR LOCAL RUNNING
# from common.constants import holiday_list
# from adaptor.MDSAdaptor import MDSAdaptor
# from adaptor.MDSAdaptorTest import MDSAdaptor as MDSAdaptorTest
# from adaptor import OMServiceCallHelper, OMServiceCallHelperTest

holiday_list = HolidayUtil.populateHolidays()

# Checking for Last Friday
def checkIfLastFriday():
    todayte = date.today()
    month = todayte.month
    year = todayte.year
    daysInMonth = calendar.monthrange(year, month)[1]  # Returns (month, numberOfDaysInMonth)
    dt = date(year, month, daysInMonth)

    # Back up to the most recent Friday
    offset = 5 - dt.isoweekday()
    if offset > 0: offset -= 7  # Back up one week if necessary
    dt += timedelta(offset)  # dt is now date of last Fr in month
    print(dt)

    if dt == todayte:
        return True
    else:
        return False


#get last thursday of month
def getLastThursday(year, month):
   # Create a datetime.date for the last day of the given month
    daysInMonth = calendar.monthrange(year, month)[1]  # Returns (month, numberOfDaysInMonth)
    dt = date(year, month, daysInMonth)

    # Back up to the most recent Thursday
    offset = 4 - dt.isoweekday()
    if offset > 0: offset -= 7  # Back up one week if necessary
    dt += timedelta(offset)  # dt is now date of last Th in month
    return dt


# Checking for Last Friday
def checkIfLastThursday():
    isLastThursday = False

    todayte = date.today()
    month = todayte.month
    year = todayte.year
    daysInMonth = calendar.monthrange(year, month)[1]  # Returns (month, numberOfDaysInMonth)
    dt = date(year, month, daysInMonth)

    # MOCK TEST DATA
    #todayte = date(2020,3,25)

    # Back up to the most recent Friday
    offset = 4 - dt.isoweekday()
    if offset > 0: offset -= 7  # Back up one week if necessary
    dt += timedelta(offset)  # dt is now date of last Fr in month
    print(dt)

    # SET LAST WEDNESDAY AS EXPIRY, IF LAST THURSDAY IS HOLIDAY
    dt_last_wednesday = dt - timedelta(1)
    if dt_last_wednesday == todayte:
        if dt in holiday_list:
            isLastThursday = True
            return isLastThursday

    if dt == todayte:
        isLastThursday = True
        return isLastThursday

    return isLastThursday

    # Checking for Last Friday
def checkIfDayBeforeLastThursday():
    isLastThursday = False
    isDayBeforeLastThursday = False

    todayte = date.today()
    # MOCK TEST DATA
    #todayte = date(2020,5,27)

    nextDate = todayte + timedelta(1)

    month = nextDate.month
    year = nextDate.year
    daysInMonth = calendar.monthrange(year, month)[1]  # Returns (month, numberOfDaysInMonth)
    dt = date(year, month, daysInMonth)



    # Back up to the most recent Friday
    offset = 4 - dt.isoweekday()
    if offset > 0: offset -= 7  # Back up one week if necessary
    dt += timedelta(offset)  # dt is now date of last Fr in month
    print(dt)

    # SET LAST WEDNESDAY AS EXPIRY, IF LAST THURSDAY IS HOLIDAY
    dt_last_wednesday = dt - timedelta(1)
    if dt_last_wednesday == nextDate:
        if dt in holiday_list:
            isLastThursday = True

    if dt == nextDate:
        isLastThursday = True

    if isLastThursday:
        isDayBeforeLastThursday = True

    return isDayBeforeLastThursday


    #Get Expiry Date i.e. Last Thursday of Near of Mid Month
def getExpiryDate():
    todayte = date.today()
    #todayte = date(2018,12,1)
    cmon = todayte.month
    cyear = todayte.year
    nthu = getLastThursday(cyear, cmon)
    pastDateWithMinusThershold = getPastDateMinus(nthu,int(0))
    # calculate next month expiry if thursday has been passed or delta is very less
    if nthu.year == todayte.year and nthu.month == todayte.month and \
            (todayte > nthu or todayte > pastDateWithMinusThershold):
        print('Calculate next month expiry')
        if cmon != 12:
            nthu = getLastThursday(cyear, cmon + 1)
        else:
            nthu = getLastThursday(cyear + 1, 1)
    return nthu

def getWeeklyExpiryDate():
    today = date.today()

    #MOCK
    #today = date(2021, 7, 22)
    thursday = today + timedelta((3-today.weekday())%7)

    # If you want next Thursday, in case today is Thursday
    #thursday = today + timedelta((2-today.weekday())%7+1)
    return thursday

def getWeeklyExpiryDate_v2():
    today = date.today()

    #MOCK
    #today = date(2021, 7, 22)
    thursday = today + timedelta((3-today.weekday())%7)

    weekday = calendar.day_name[today.weekday()]

    # If you want next Thursday, in case today is Thursday
    if weekday == 'Thursday':
        thursday = today + timedelta((2-today.weekday())%7+1)
    return thursday

#Check for Trading Holiday by date
def checkForHolidays(current_date):
    #check for nse holidays
    current_date = datetime.strptime(current_date, '%Y-%m-%d')
    current_date = date(current_date.year, current_date.month, current_date.day)

    for holiday in holiday_list:
        if current_date == holiday:
            return True
    return None

#Check for Trading Holiday for today
def isHolidayToday():
    #check for nse holidays
    start_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    current_date = date(current_date.year, current_date.month, current_date.day)

    for holiday in holiday_list:
        if current_date == holiday:
            return True
    return False

#Check for Trading Holiday for today
def isHolidayAndWeekendToday():
    #check for nse holidays
    start_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    current_date = date(current_date.year, current_date.month, current_date.day)
    #current_date = date(2019,4,6)

    # Check Holiday Today
    for holiday in holiday_list:
        if current_date == holiday:
            return True

    # Check Weekend Today
    weekno = current_date.weekday()
    #print(weekno)
    if weekno >= 5:
        return True

    return False

def checkIfThursday():
    isTodayThursday = False
    current_date = date.today()
    #current_date = date(2020, 4, 8)
    weekday = calendar.day_name[current_date.weekday()]

    # IN CASE THURSDAY IS HOLIDAY, WEDNESDAY WOULD BE EXPIRY DAY
    if weekday == 'Wednesday':
        current_date = current_date + timedelta(1)
        if current_date in holiday_list:
            isTodayThursday = True
            return isTodayThursday

    if weekday == "Thursday":
        isTodayThursday = True
        return isTodayThursday

    return isTodayThursday


def checkIfDayBeforeThursday():
    isTodayDayBeforeThursday = False
    isTodayThursday = False
    current_date = date.today()
    #current_date = date(2020, 4, 27)
    next_date = current_date + timedelta(1)

    weekday = calendar.day_name[next_date.weekday()]

    # IN CASE THURSDAY IS HOLIDAY, WEDNESDAY WOULD BE EXPIRY DAY
    if weekday == 'Wednesday':
        next_date = next_date + timedelta(1)
        if next_date in holiday_list:
            isTodayThursday = True

    if weekday == "Thursday":
        isTodayThursday = True

    if isTodayThursday:
        isTodayDayBeforeThursday = True

    return isTodayDayBeforeThursday


#calculate past date - business days plus minus
def getPastDateMinus(current_date, noOfDays):
    past_date = current_date

    while noOfDays > 0:
        past_date =  past_date-timedelta(days=1)
        past_date_converted = date(past_date.year, past_date.month, past_date.day)
        weekday = past_date.weekday()
        if weekday >= 5:  # sunday = 6
            continue
        if past_date_converted in holiday_list:
            continue
        noOfDays -= 1

    return past_date

# Get Active Days skipping weekends and Holidays
def getActiveDaysBetweenDates( start_date, end_date):
    ActiveDays = float(np.busday_count(start_date.date(), end_date.date(), holidays=holiday_list))
    return ActiveDays

# Get Active Days skipping weekends and Holidays
def getActiveDaysBetweenDates_v2( start_date, end_date):
    ActiveDays = float(np.busday_count(start_date, end_date, holidays=holiday_list))
    return ActiveDays

def getDaysBetweenDates(start_date, end_date):
    delta = end_date - start_date
    return delta.days

def getHolidaysBetweenDates(start_date, end_date, date_format):
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)

    TotalDays = getDaysBetweenDates(start_date, end_date)
    ActiveDays = getActiveDaysBetweenDates(start_date, end_date)
    return TotalDays - ActiveDays

def date_by_adding_business_days(from_date, add_days,holidays):
    business_days_to_add = add_days
    current_date = from_date
    while business_days_to_add > 0:
        current_date += timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # sunday = 6
            continue
        if current_date in holidays:
            continue
        business_days_to_add -= 1
    return current_date

#what is 5% of 20
def xPercentageOfY(percent, whole):
  return (percent * whole) / 100.0

#what is percetage is 5 of 20
def percentageIsXofY(part, whole):
  return 100 * float(part)/float(whole)

def xPercentageOfYIncrementedValue(percent, whole):
  return (whole + (percent * whole) / 100.0)

def xPercentageOfYDecrementedValue(percent, whole):
  return (whole - (percent * whole) / 100.0)

#intercept to price ratio
def calculate_intercept_to_price_ratio(Y, intercept):
    intercept_price_ratio= float(intercept)/float(Y)
    return intercept_price_ratio


def send_email(user, pwd, recipient, subject, body):

    FROM = user
    TO = recipient if isinstance(recipient, list) else [recipient]
    SUBJECT = subject
    TEXT = body

    # Prepare actual message
    message = """From: %s\nTo: %s\nSubject: %s\n\n%s
    """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
    try:
        server = smtplib.SMTP_SSL('smtp.mail.yahoo.com', 465)
        server.ehlo()
        #server.starttls()
        server.login(user, pwd)
        server.sendmail(FROM, TO, message)
        server.close()
        print('successfully sent the mail')
    except Exception as e:
        print("failed to send mail")
        logging.info(traceback.format_exc())
        print(e)


def send_email_dqns(Configuration, subject, message, contentType):

    try:
        if Configuration['ENVIRONMENT'] == 'PRD':
            emailRequest = {
                "subject": subject,
                "message": message,
                "env": getEnv(Configuration['ENVIRONMENT']),
                "contentType": contentType,
                "to": Configuration['EMAIL_TO']
            }
        else:
            emailRequest = {
                "subject": subject,
                "message": message,
                "env": getEnv(Configuration['ENVIRONMENT']),
                "contentType": contentType
            }
        #print(emailRequest)

        if Configuration['ENVIRONMENT'] != 'LOCAL':
            requests.post(Configuration['DQNS_URL'], json=emailRequest)
            print('successfully sent the mail')
            logging.info('successfully sent the mail')

    except Exception as e:
        print("failed to send mail")
        logging.info(traceback.format_exc())
        print(e)

def send_sns(Configuration):

    try:
        if Configuration['ENVIRONMENT'] != 'LOCAL':
            responseDict = requests.post(Configuration['SNS_URL'],timeout= 60).json()
            print('successfully sent the sns message!!:: '+str(responseDict))
            logging.info('successfully sent the sns message!!:: '+str(responseDict))

    except Exception as e:
        print("failed to send sns message!!!!!")
        logging.info(traceback.format_exc())
        print(e)

def isFloat(float_value):
    try:
        float(float_value)
        return True
    except ValueError:
        return False


def get_percentage_change(current, previous):
    try:
        return ((current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return 0


def getEpochTimeStampFromDate(date_str):
    str_to_dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    jst = timezone('Asia/Kolkata')
    jst_dt = str_to_dt.astimezone(jst)
    timestamp = jst_dt.timestamp()
    return timestamp


def getNextHourMinuteCombination(hour, minute, symbol, frequency):
    '''

    :param hour:
    :param minute:
    :return:
    '''
    time_fe = symbol+'_'+frequency
    if 15 <= float(minute) <= 59:
        time_fe_current = time_fe+'_'+str(int(hour)+1)+'_'+str(15)
        time_fe_next = time_fe + '_' + str(int(hour) + 2) + '_' + str(15)
    else:
        time_fe_current = time_fe + '_' + str(int(hour)) + '_' + str(15)
        time_fe_next = time_fe + '_' + str(int(hour) + 1) + '_' + str(15)

    print('Single Time FE {} '.format(time_fe_current))
    #time_fe = 'BANKNIFTY_60M_16_15'
    return time_fe_current, time_fe_next

def getAllHourMinuteCombination(hour, minute, symbol, frequency):
    '''

    :param hour:
    :param minute:
    :return:
    '''
    time_fe = symbol+'_'+frequency
    if 15 <= float(minute) <= 59:
        time_fe_previous = time_fe+'_'+str(int(hour))+'_'+str(15)
        time_fe_current = time_fe+'_'+str(int(hour)+1)+'_'+str(15)
        time_fe_next = time_fe + '_' + str(int(hour) + 2) + '_' + str(15)
    else:
        time_fe_previous = time_fe + '_' + str(int(hour)-1) + '_' + str(15)
        time_fe_current = time_fe + '_' + str(int(hour)) + '_' + str(15)
        time_fe_next = time_fe + '_' + str(int(hour) + 1) + '_' + str(15)

    print('Single Time FE {} '.format(time_fe_current))
    #time_fe = 'BANKNIFTY_60M_16_15'
    return time_fe_previous, time_fe_current, time_fe_next

def is_time_between(begin_time, end_time, check_time=None):
    begin_time, end_time, check_time = begin_time, end_time, check_time
    # If check time is not given, default to current UTC time
    check_time = check_time or datetime.utcnow().time()
    if begin_time < end_time:
        return check_time >= begin_time and check_time <= end_time
    else: # crosses midnight
        return check_time >= begin_time or check_time <= end_time

def getWAPBidAskPrice(bid_ask_dict, quantity):

    NUM_UNITS = float(quantity)

    buy_sell_price = [bid_ask_dict['BID_PRICE_1'], bid_ask_dict['BID_PRICE_2'],
                      bid_ask_dict['BID_PRICE_3'], bid_ask_dict['BID_PRICE_4'],
                      bid_ask_dict['BID_PRICE_5']]

    buy_sell_qty = [bid_ask_dict['BID_QTY_1'], bid_ask_dict['BID_QTY_2'],
                    bid_ask_dict['BID_QTY_3'], bid_ask_dict['BID_QTY_4'],
                    bid_ask_dict['BID_QTY_5']]
    buy_sell_price_sliced, buy_sell_qty_sliced = prepareListForWAP(NUM_UNITS, buy_sell_price, buy_sell_qty)

    BID_PRICE = round(np.average(buy_sell_price_sliced, weights=buy_sell_qty_sliced), 2)

    buy_sell_price = [bid_ask_dict['ASK_PRICE_1'], bid_ask_dict['ASK_PRICE_2'],
                      bid_ask_dict['ASK_PRICE_3'], bid_ask_dict['ASK_PRICE_4'],
                      bid_ask_dict['ASK_PRICE_5']]

    buy_sell_qty = [bid_ask_dict['ASK_QTY_1'], bid_ask_dict['ASK_QTY_2'],
                    bid_ask_dict['ASK_QTY_3'], bid_ask_dict['ASK_QTY_4'],
                    bid_ask_dict['ASK_QTY_5']]

    buy_sell_price_sliced, buy_sell_qty_sliced = prepareListForWAP(NUM_UNITS, buy_sell_price, buy_sell_qty)

    ASK_PRICE = round(np.average(buy_sell_price_sliced, weights=buy_sell_qty_sliced), 2)

    return BID_PRICE, ASK_PRICE


def prepareListForWAP(NUM_UNITS, buy_sell_price, buy_sell_qty):
    # create a cumulative sum list in sorted order
    buy_sell_qty_cumsum = np.cumsum(buy_sell_qty)
    # find index for slicing list and use it for calculating weighted average price
    index_sliced = bisect_left(buy_sell_qty_cumsum, NUM_UNITS)
    # Check if MF does not cover all 5 bids, then add pending units to last element in sliced list
    if index_sliced < len(buy_sell_price) - 1:
        buy_sell_price_sliced = buy_sell_price[:index_sliced + 1]
        buy_sell_qty_sliced = buy_sell_qty[:index_sliced]
        pending_num_units = NUM_UNITS - sum(buy_sell_qty_sliced)
        buy_sell_qty_sliced.append(pending_num_units)
    else:
        buy_sell_price_sliced = buy_sell_price
        buy_sell_qty_sliced = buy_sell_qty[:index_sliced]
        pending_num_units = NUM_UNITS - sum(buy_sell_qty_sliced)

        # If number of units exceeds bid quantity, then add pending units plus last price(BID_PRICE_5)
        # Right now only in scenario when MF is 1 and no stocks quantity matching numunits 1
        if len(buy_sell_qty_sliced) < len(buy_sell_price_sliced):
            buy_sell_qty_sliced.append(pending_num_units)
        else:
            buy_sell_price_sliced.append(buy_sell_price_sliced[-1])
            buy_sell_qty_sliced.append(pending_num_units)
    return buy_sell_price_sliced, buy_sell_qty_sliced

def calculateWAPPrices(price_1, price_2, quantity_1, quantity_2):
    return (float(price_1)* float(quantity_1) + float(price_2)* float(quantity_2))/(float(quantity_1)+float(quantity_2))

def sum_two(value_1, value_2):
    return float(value_1)+float(value_2)

#get number closest to a given value
def takeClosest(num,collection):
   return min(collection,key=lambda x:abs(x-num))

# DIfference Between two times on same day
def getTimeDiffInSeconds(time1, time2, format):
    return (datetime.strptime(time2, format) - datetime.strptime(time1, format)).total_seconds()

def checkIfTodayisParticularDayFromList(weekday_list):
    isTodayWeekdayFromList = False
    current_date = date.today()
    #current_date = date(2020, 4, 8)
    weekday = calendar.day_name[current_date.weekday()]

    # MOCK DATA
    #weekday = 'Friday'

    if weekday in weekday_list:
        isTodayWeekdayFromList = True
        return isTodayWeekdayFromList

    return isTodayWeekdayFromList

################################################## GET ENV #############################################################
def getEnv(active_env):

    if 'PRD' in active_env:
        return 'PRD'
    elif 'DEV' in active_env:
        return 'DEV'
    elif 'SIT' in active_env:
        return 'SIT'
    elif 'QA' in active_env:
        return 'QA'
    elif 'UAT' in active_env:
        return 'UAT'
    else:
        return active_env
########################################################################################################################

def isMockPrdEnv(Configuration):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT'] and Configuration['ENVIRONMENT'].startswith('MOCK_PRD'):
        return True

    return False

def isLocalPrdEnv(Configuration):
    if Configuration['BROKER_API_ACTIVE'] == 'Y' and 'PRD' in Configuration['ENVIRONMENT'] and Configuration['ENVIRONMENT'].startswith('LOCAL_PRD'):
        return True

    return False

def isLocalDevEnv(Configuration):
    if Configuration['ENVIRONMENT'].startswith('LOCAL_DEV'):
        Configuration['BROKER_API_ACTIVE'] = 'N'
        return True

    return False

def isMockDevEnv(Configuration):
    if Configuration['ENVIRONMENT'].startswith('MOCK_DEV'):
        Configuration['BROKER_API_ACTIVE'] = 'N'
        return True

    return False

def isBackTestingEnv(Configuration):
    if Configuration['ENVIRONMENT'].startswith('BACKTESTING'):
        Configuration['BROKER_API_ACTIVE'] = 'N'
        return True

    return False

def isBackTestingEnvProfile(schema_name):
    if 'BACKTESTING' in schema_name:
        return True

    return False

def isMockAndLocalEnv(Configuration):
    return isMockPrdEnv(Configuration) or \
           isLocalPrdEnv(Configuration) or \
           isLocalDevEnv(Configuration) or \
           isMockDevEnv(Configuration)

def isMockEnv(Configuration):
    return isMockPrdEnv(Configuration) or \
           isMockDevEnv(Configuration)

def getMDSAdaptor(Configuration):
    mdsAdaptor = MDSAdaptor()

    if isMockPrdEnv(Configuration) or isMockDevEnv(Configuration):
        mdsAdaptor = MDSAdaptorTest()

    if isBackTestingEnv(Configuration):
        mdsAdaptor = MDSAdaptorBackTesting()

    return mdsAdaptor

def getOMServiceCallHelper(Configuration):
    if isMockPrdEnv(Configuration) or isMockDevEnv(Configuration):
        return OMServiceCallHelperTest
    return OMServiceCallHelper

# def stopAutomation(symbol):
#     from dao import GlobalConfigurationDAO
#     configurationDAO = ConfigurationDAO
#     # configurationDAO.updateConfiguration('BROKER_API_ACTIVE', 'N')
#     configurationDAO.updateConfiguration('SYMBOL_ACTIVE_' + symbol, 'N')

def isExpiryToday(expiry_date):
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')
    current_date = datetime.strptime(current_date, '%Y-%m-%d')
    #current_date = date(2021, 9, 2)
    current_date = date(current_date.year, current_date.month, current_date.day)
    current_date = current_date.strftime('%y%b%d').upper()

    if current_date == expiry_date:
            return True
    return False


def isTodayDate(today_date):
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')
    current_date = datetime.strptime(current_date, '%Y-%m-%d')
    #current_date = date(2021, 9, 2)
    current_date = date(current_date.year, current_date.month, current_date.day)

    if current_date == today_date:
            return True
    return False

def isOOPSOrNOSRunningToday(Configuration, expiry_date):
    isOOPSRunningToday = False
    isNOSRunningToday = True

    RUN_OOPS_ON_DAYS_LIST = Configuration['RUN_OOPS_ON_DAYS'].split(',')

    # OOPS WILL BE RUNNING ON DAYS MENTIONED IN CONFIG FILE AND EXPIRY DAY
    if checkIfTodayisParticularDayFromList(RUN_OOPS_ON_DAYS_LIST) or isExpiryToday(expiry_date):
        isOOPSRunningToday = True
        isNOSRunningToday = False

    return isOOPSRunningToday, isNOSRunningToday

def calculateThetaPnlPending(net_theta, Configuration, symbol):
    LOT_SIZE = float(Configuration['LOT_SIZE_' + symbol])
    MARKET_CLOSE_TIME = Configuration['MARKET_CLOSE_TIME']
    current_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M')
    TIME_FORMAT = '%H:%M'

    # MOCK DATA
    # MARKET_CLOSE_TIME = '16:23'
    # current_time = '16:21'
    # NET_PNL_OVERALL = 70000

    # CALCULATE TIME DELTA IN MINUTES
    TIME_DELTA = getTimeDiffInSeconds(current_time, MARKET_CLOSE_TIME, TIME_FORMAT) / 60
    TRADING_MNS_PER_DAY = 375

    THETA_PNL_PENDING = ((net_theta * LOT_SIZE) / TRADING_MNS_PER_DAY) * TIME_DELTA

    return THETA_PNL_PENDING

def getUniqueKeyDateTime():
    time.sleep(.05)
    tz = pytz.timezone('Asia/Calcutta')
    today_date = pytz.utc.localize(datetime.utcnow()).astimezone(tz)
    today_time = str(today_date.time())
    split = today_time.split('.')
    unique_time_milli = split[1][3:]
    unique_time_final = split[0] + unique_time_milli
    unique_date_final = str(today_date.date())[2:].replace('-', '')
    return unique_date_final + unique_time_final.replace(':', '')

def getUniqueKeyTime():
    time.sleep(.05)
    tz = pytz.timezone('Asia/Calcutta')
    today_date = pytz.utc.localize(datetime.utcnow()).astimezone(tz)
    today_time = str(today_date.time())
    split = today_time.split('.')
    unique_time_milli = split[1][3:]
    unique_time_final = split[0] + unique_time_milli
    return unique_time_final.replace(':', '')

def getCurrentDay():
    current_date = date.today()
    # current_date = date(2020, 4, 8)
    return calendar.day_name[current_date.weekday()]


'''#Mail Content for Exception Scenarios
def send_mail_dq(subject, body):
    recipient_list = configurationDAO.getValue('DERIVEQ_DL_CORE_TEAM').split(",")
    #print(recipient_list)

    subject = PROFILE_PROPERTIES['ENVIRONMENT']+' | '+subject
    body =body
    # send email
    send_email(PROFILE_PROPERTIES['SENDER_EMAIL'], PROFILE_PROPERTIES['SENDER_EMAIL_PWD'], recipient_list, subject, body)
'''

if __name__ == "__main__":
    # print(getExpiryDate().strftime('%b'))
    # print(getExpiryDate())

    # current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d%b%Y').upper()
    # expiryDate = '14NOV2019'
    # print(getHolidaysBetweenDates(current_date, expiryDate, '%d%b%Y'))

    #print(checkIfLastThursday())
    #print(checkIfThursday())
    #print(checkIfDayBeforeThursday())
    #print(checkIfDayBeforeLastThursday())
    print(getWeeklyExpiryDate().strftime('%y%b%d').upper())
    print(getExpiryDate().strftime('%y%b%d').upper())
    print(getUniqueKeyDateTime())
    print(getUniqueKeyTime())

