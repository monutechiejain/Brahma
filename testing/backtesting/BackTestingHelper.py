from common import utils
from datetime import datetime, date, timedelta
import numpy as np
from testing.backtesting.BackTestingConstants import HOLIDAY_LIST

def getPositionDates(EXPIRY_DATE_ENTRY_DATE, EXPIRY_DATE_EXIT_DATE, backtesting_dict):
    POSITIONS_START_DATE = None
    POSITIONS_END_DATE = None
    year = backtesting_dict['year']

    EXPIRY_DATE_ENTRY_DATE = datetime.strptime(EXPIRY_DATE_ENTRY_DATE, '%y%b%d')
    EXPIRY_DATE_ENTRY_DATE = date(EXPIRY_DATE_ENTRY_DATE.year, EXPIRY_DATE_ENTRY_DATE.month, EXPIRY_DATE_ENTRY_DATE.day)

    EXPIRY_DATE_EXIT_DATE = datetime.strptime(EXPIRY_DATE_EXIT_DATE, '%y%b%d')
    EXPIRY_DATE_EXIT_DATE = date(EXPIRY_DATE_EXIT_DATE.year, EXPIRY_DATE_EXIT_DATE.month, EXPIRY_DATE_EXIT_DATE.day)

    # GET NO OF DAYS BETWEEN 2 EXPIRIES
    No_of_days = float(np.busday_count(EXPIRY_DATE_ENTRY_DATE, EXPIRY_DATE_EXIT_DATE, holidays=HOLIDAY_LIST[year]))
    #datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d%b%Y').upper()

    # POSITIONS_START_DATE = EXPIRY_DATE_ENTRY_DATE
    # POSITIONS_END_DATE = EXPIRY_DATE_EXIT_DATE - timedelta(1)
    #
    # if No_of_days < 4:
    #     raise ValueError("Number of Active Days are less than 4 !!")

    if No_of_days == 6:
        POSITIONS_START_DATE = EXPIRY_DATE_ENTRY_DATE
        POSITIONS_END_DATE = EXPIRY_DATE_EXIT_DATE - timedelta(1)
    elif No_of_days == 5:
        POSITIONS_START_DATE = EXPIRY_DATE_ENTRY_DATE - timedelta(1)
        POSITIONS_END_DATE = EXPIRY_DATE_EXIT_DATE - timedelta(1)
    else:
        print("Number of Active Days are less than 5 !!")
        raise ValueError("Number of Active Days are less than 5 !!")

    return POSITIONS_START_DATE, POSITIONS_END_DATE, EXPIRY_DATE_ENTRY_DATE, EXPIRY_DATE_EXIT_DATE

def getNextDateTime(backtesting_dict, year):
    DATE_TIME_INITIAL_STR = backtesting_dict['DATE_TIME']
    DATE_TIME_INITIAL = datetime.strptime(DATE_TIME_INITIAL_STR, '%Y-%m-%d %H:%M:%S')

    DATE_INITIAL = date(DATE_TIME_INITIAL.year, DATE_TIME_INITIAL.month, DATE_TIME_INITIAL.day)
    DATE_TIME_FINAL = DATE_TIME_INITIAL + timedelta(minutes=1)
    DATE_TIME = DATE_TIME_FINAL.strftime('%Y-%m-%d %H:%M:%S')
    TIME_FINAL_STR = DATE_TIME_FINAL.strftime('%H:%M')

    # # IF MARKET CLOSE TIME, ADD 1 BUSINESS DAY
    # if '15:25' in DATE_TIME_INITIAL_STR or '15:30' in DATE_TIME_INITIAL_STR:
    #     DATE_FINAL = utils.date_by_adding_business_days(DATE_INITIAL, 1, HOLIDAY_LIST[year])
    #     DATE_FINAL_STR = DATE_FINAL.strftime('%Y-%m-%d')
    #     TIME_FINAL_STR = '09:20:00'
    #     DATE_TIME = DATE_FINAL_STR+' '+TIME_FINAL_STR
    #     TIME_FINAL_STR = '09:20'

    return DATE_TIME, DATE_TIME_FINAL, TIME_FINAL_STR


def getPastDateMinus(current_date, noOfDays, backtesting_dict):
    past_date = current_date
    year = backtesting_dict['year']

    while noOfDays > 0:
        past_date =  past_date-timedelta(days=1)
        past_date_converted = date(past_date.year, past_date.month, past_date.day)
        weekday = past_date.weekday()
        if weekday >= 5:  # sunday = 6
            continue
        if past_date_converted in HOLIDAY_LIST[year]:
            continue
        noOfDays -= 1

    return past_date


