
import requests
import logging
import time
from datetime import datetime, date
import traceback

HOLIDAYS_URL = 'https://finnovesh.com/static/api/holidays/holidays.json'

def populateHolidays():
    holiday_list_date=[]
    max_retries = 2
    retry = 0
    while (retry < max_retries):

        try:

            holidays_dict = requests.get(HOLIDAYS_URL, timeout=60).json()
            holidays_list_str = holidays_dict[str(date.today().year)]
            for holiday in holidays_list_str:
                holiday = datetime.strptime(holiday, '%Y%b%d')
                holiday = date(holiday.year, holiday.month, holiday.day)
                holiday_list_date.append(holiday)

            break
        except Exception as exception:
            print('Exception occurred::', exception)
            logging.info('Exception occurred::' + str(exception))
            logging.info(traceback.format_exc())
            # retrying in case of failures
            time.sleep(2)
            retry = retry + 1
            if retry == max_retries:
                raise Exception('HOLIDAYS API Not Responding')


    return holiday_list_date

if __name__ == "__main__":
    populateHolidays()