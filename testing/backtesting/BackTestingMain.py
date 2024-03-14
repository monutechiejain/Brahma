
from testing.backtesting.BackTestingConfig import BACK_TESTING_PROPERTIES
from testing.backtesting import BackTestingService
from config.logging.FileLogConfig import FileLogConfig
import traceback
import pandas as pd
from datetime import datetime, date
from testing.backtesting.BackTestingConstants import EXPIRY_DATES, HOLIDAY_LIST

# SET ENV VARIABLES
# os.environ["config_host"] = "http://agent.finnovesh.com:6011"
# os.environ["active_env"] = "backtesting"
# os.environ["config_file_name"] = "elo1_local"
# os.environ["SCHEMA_NAME"] = "ELO1_V1_BACKTESTING"
# os.environ["DB_HOST"] = "db.dev.finnovesh.com"
# os.environ["DB_USER"] = "devFinnovesh"
# os.environ["DB_PASSWORD"] = "6aGkGtfNnGz294m"
# os.environ["mds_host"] = "http://agent.finnovesh.com:6011/config/backdata"
# os.environ["om_host"] = "http://api.oops1_dq.deriveq.in1:6015"
# os.environ["util_host"] = "http://api.sit.deriveq.com"

def backTestingMain():

    # SET ACTIVE ENV, MDS BACKTESTING URL

    # 1. FETCH EXPIRY DATES, YEAR, MONTHS, START_TIME, END_TIME
    # 2. CALCULATE GRANULAR LEVEL EXPIRY_DATE, START_DATE, START_TIME, END_DATE, END_TIME, it will be Strategy Specific

    backtesting_dict = {}
    backtesting_dict['years_list'] = BACK_TESTING_PROPERTIES['YEARS']
    backtesting_dict['months_list'] = BACK_TESTING_PROPERTIES['MONTHS']
    backtesting_dict['start_time'] = BACK_TESTING_PROPERTIES['START_TIME']
    backtesting_dict['end_time'] = BACK_TESTING_PROPERTIES['END_TIME']
    backtesting_dict['start_time_v1'] = BACK_TESTING_PROPERTIES['START_TIME_V1']
    backtesting_dict['symbol'] = BACK_TESTING_PROPERTIES['SYMBOL']

    years_list = BACK_TESTING_PROPERTIES['YEARS']
    months_list = BACK_TESTING_PROPERTIES['MONTHS']
    run_days_list =  BACK_TESTING_PROPERTIES['RUN_ONLY_FOR_DAYS']

    # CHECK DICTIONARY STRUCTURE IN CONSTANTS FILE WHICH HAS YEAR AS PARENT ATTRIBUTE FOLLOWED BY MONTH AND WEEKS
    for year in years_list:
        df_backtest_dates = pd.read_excel('BackTestingDays.xlsx', sheet_name=year)
        for month in months_list:
                backtesting_dict['year'] = year
                backtesting_dict['month'] = month

                # EXPIRY DATES LIST FROM XLS FILE
                try:

                    df_backtest_dates = filterDays(df_backtest_dates, month, year, run_days_list)

                    for index, row in df_backtest_dates.iterrows():
                        date_dict = row.to_dict()
                        backtesting_dict['current_date'] = date_dict['Date'].date()
                        backtesting_dict['day'] = date_dict['DayName']
                        backtesting_dict['current_expiry'] = date_dict['CurrentExpiry'].date()
                        backtesting_dict['next_expiry'] = date_dict['NextExpiry'].date()
                        try:
                            BackTestingService.initiateBackTesting(backtesting_dict)
                        except Exception as ex:
                            template = "Exception {} occurred while Runnning for this day: {} with message : {}"
                            message = template.format(type(ex).__name__,year, ex.args)
                            print(traceback.format_exc())
                            print(message)
                except Exception as ex:
                    template = "Exception {} occurred while Runnning for this year: {} and month : {} with message : {}"
                    message = template.format(type(ex).__name__, year, month, ex.args)
                    print(traceback.format_exc())
                    print(message)

    pass


def filterDays(df_backtest_dates, month, year, run_days_list):
    # FILTER BASED on MONTH
    df_backtest_dates = df_backtest_dates[(df_backtest_dates['Month'] == month)]

    # FILTER BASED ON DATES
    dates_format_list= [date(year=int(year),month=month_string_to_number(month),day=d) for d in run_days_list]
    df_backtest_dates = df_backtest_dates[df_backtest_dates['Date'].isin(dates_format_list)]

    # FILTER WEEKENDS AND HOLIDAYS AND WEEKENDS
    holidays_list = HOLIDAY_LIST[year]
    df_backtest_dates = df_backtest_dates[~df_backtest_dates['Date'].isin(holidays_list)]
    df_backtest_dates = df_backtest_dates[df_backtest_dates['Date'].dt.weekday<5]


    return df_backtest_dates

def month_string_to_number(string):
    m = {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr':4,
         'may':5,
         'jun':6,
         'jul':7,
         'aug':8,
         'sep':9,
         'oct':10,
         'nov':11,
         'dec':12
        }
    s = string.strip()[:3].lower()

    try:
        out = m[s]
        return out
    except:
        raise ValueError('Not a month')

if __name__ == "__main__":
    FileLogConfig()
    backTestingMain()