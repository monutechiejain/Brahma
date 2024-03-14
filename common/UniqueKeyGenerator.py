import time
from datetime import datetime

import pytz


class UniqueKeyGenerator:

    def getUniqueKeyDateTime(self):
        time.sleep(.05)
        tz = pytz.timezone('Asia/Calcutta')
        today_date = pytz.utc.localize(datetime.utcnow()).astimezone(tz)
        today_time = str(today_date.time())
        split = today_time.split('.')
        unique_time_milli = split[1][3:]
        unique_time_final = split[0] + unique_time_milli
        unique_date_final = str(today_date.date())[2:].replace('-', '')
        return unique_date_final + unique_time_final.replace(':', '')

    def getUniqueKeyTime(self):
        time.sleep(.05)
        tz = pytz.timezone('Asia/Calcutta')
        today_date = pytz.utc.localize(datetime.utcnow()).astimezone(tz)
        today_time = str(today_date.time())
        split = today_time.split('.')
        unique_time_milli = split[1][3:]
        unique_time_final = split[0] + unique_time_milli
        return unique_time_final.replace(':', '')
