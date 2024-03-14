import requests
import logging
import time
import json
import traceback
from common import utils

def fetchGreeks(expiry_date_greeks, current_date_greeks, current_time_greeks, instrument_type,
                                  option_price,underlying_spot,strike_price,order_type, Configuration, raiseException = True):
    ts_start = time.time()
    iv, delta, gamma, theta, vega=0.0,0.0,0.0,0.0,0.0

    ################################# BACKTESTING , WE ARE NOT CALLING GREEKS API ######################################
    if utils.isBackTestingEnv(Configuration):
        return iv, delta, gamma, theta, vega
    ####################################################################################################################
    max_retries = 1
    retry = 0
    while (retry < max_retries):

        try:

            greeks_url = Configuration["GREEKS_URL"]
            payload = {
                "expiry_date": expiry_date_greeks,
                "current_date": current_date_greeks,
                "current_time": current_time_greeks,
                "instrument_type": instrument_type,
                "option_price": float(option_price),
                "underlying_spot": int(underlying_spot),
                "strike_price": int(strike_price),
                "order_type": order_type

            }
            logging.info('{}: GREEKS API REQUEST :: {}'.format(Configuration['SCHEMA_NAME'], str(payload)))

            #greeks_dict = requests.post(greeks_url, json=payload, timeout=60).json()

            if utils.isMockPrdEnv(Configuration) or utils.isMockDevEnv(Configuration):
                file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\greeks.txt"
                with open(file_path) as json_file:
                    greeks_dict = json.load(json_file)
            else:
                greeks_reponse = requests.post(greeks_url, json=payload, timeout= 10)
                logging.info('{}: GREEKS API RESPONSE ::{}'.format(Configuration['SCHEMA_NAME'], str(greeks_reponse.text)))
                greeks_dict = greeks_reponse.json()

            iv, delta, gamma, theta, vega = abs(greeks_dict['iv']), abs(greeks_dict['delta']), abs(greeks_dict['gamma']), abs(greeks_dict['theta']), abs(greeks_dict['vega'])
            break
        except Exception as exception:
            print('Exception occurred::', exception)
            logging.info('Exception occurred::' + str(exception))
            logging.info(traceback.format_exc())
            # retrying in case of failures
            retry = retry + 1
            if raiseException:
                time.sleep(2)
                if retry == max_retries:
                    raise Exception(Configuration['SCHEMA_NAME'] + ': GREEKS API Not Responding')

    ts_end = time.time()
    time_taken = ts_end - ts_start
    logging.info('{}: Time Taken by Greeks API is : {}'.format(Configuration['SCHEMA_NAME'], str(time_taken)))


    return iv, delta, gamma, theta, vega
