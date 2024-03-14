import logging
import requests
from datetime import datetime
import pandas as pd
import time
import json
import traceback

desired_width=320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns',15)
pd.set_option('display.max_rows',200)

class MDSAdaptor:


    def getLTP(self, Configuration, symbol, expiry, constituents, constituentType):

        time_start = time.time()
        ltp_dict = self.callMDSLtpAPI(Configuration, constituentType, constituents, expiry, symbol)
        time_end = time.time()
        logging.info('Time taken by MDS LTP Call: '+str(time_end-time_start))

        df_ltp = pd.DataFrame.from_dict(ltp_dict, orient='columns')
        indexList = ['timestamp', 'last_price', 'depth', 'spotPrice']

        # TODO - Read Depth and Index Spot Price
        # Filter dataframe by rows or index
        df_ltp = df_ltp.loc[indexList]

        # Transformation -- Add BID ASK PRICE/QTY/ORDERS to LTP Data Frame as ROWS
        self.addBidAskToLTP(df_ltp)
        return df_ltp

    def callMDSLtpAPI(self, Configuration, constituentType, constituents, expiry, symbol):
        '''
        MDS LTP API call for Order Gen and Square Off Job to calculate current estimates
        :param constituentType:
        :param constituents:
        :param expiry:
        :param symbol:
        :return:
        '''
        ltp_dict = None
        max_retries = 2
        retry = 0
        while (retry < max_retries):

            try:
                mds_ltp_url = Configuration["MDS_URL"] + '/nse/ltp'
                payload = {
                    "symbol": symbol,
                    "derivativeDetails": {
                        "expiry": expiry,
                        "indexDetails": {
                            "constituents": constituents,
                            "constituentType": constituentType
                        }
                    }
                }
                ltp_dict = requests.post(mds_ltp_url, headers={'X_AUTH': Configuration["MDS_AUTH_KEY"]}, json=payload, timeout= 60).json()
                # TODO: Mock LTP Response
                #file_path = "/home/ec2-user/venv/python36/lib/python3.6/site-packages/oops1/src/data/mock/ltp_json_" + symbol + ".txt"
                # file_path = "E:\\Workspace\\Workspace_Python_2019\\NeutralOptionsSelling\\oops1\\src\\data\\mock\\ltp_json_" + symbol + ".txt"
                # with open(file_path) as json_file:
                #       ltp_dict = json.load(json_file)
                break
            except Exception as exception:
                print('Exception occurred::', exception)
                logging.info('Exception occurred::' + str(exception))
                logging.info(traceback.format_exc())
                # retrying in case of failures
                time.sleep(2)
                retry = retry + 1
                if retry == max_retries:
                    raise Exception('MDS LTP API Not Responding')
        return ltp_dict

    def addBidAskToLTP(self, df_ltp):
        for i in range(0, 5):
            bid_price, bid_orders, bid_qty = 'BID_PRICE_' + str(i + 1), 'BID_ORDERS_' + str(i + 1), 'BID_QTY_' + str(i + 1)
            ask_price, ask_orders, ask_qty = 'ASK_PRICE_' + str(i + 1), 'ASK_ORDERS_' + str(i + 1), 'ASK_QTY_' + str(i + 1)
            df_ltp.loc[bid_price] = df_ltp.loc['depth'].apply(lambda x: x['buy'][i]['price'])
            df_ltp.loc[bid_orders] = df_ltp.loc['depth'].apply(lambda x: x['buy'][i]['orders'])
            df_ltp.loc[bid_qty] = df_ltp.loc['depth'].apply(lambda x: x['buy'][i]['quantity'])

            df_ltp.loc[ask_price] = df_ltp.loc['depth'].apply(lambda x: x['sell'][i]['price'])
            df_ltp.loc[ask_orders] = df_ltp.loc['depth'].apply(lambda x: x['sell'][i]['orders'])
            df_ltp.loc[ask_qty] = df_ltp.loc['depth'].apply(lambda x: x['sell'][i]['quantity'])

    def addBidAskToMarketDepth(self, df_Market_Depth):
        for i in range(0, 5):
            bid_price, bid_orders, bid_qty = 'BID_PRICE_' + str(i + 1), 'BID_ORDERS_' + str(i + 1), 'BID_QTY_' + str(i + 1)
            ask_price, ask_orders, ask_qty = 'ASK_PRICE_' + str(i + 1), 'ASK_ORDERS_' + str(i + 1), 'ASK_QTY_' + str(i + 1)
            df_Market_Depth[bid_price] = df_Market_Depth['buy'].apply(lambda x: x[i]['price'])
            df_Market_Depth[bid_orders] = df_Market_Depth['buy'].apply(lambda x: x[i]['orders'])
            df_Market_Depth[bid_qty] = df_Market_Depth['buy'].apply(lambda x: x[i]['quantity'])

            df_Market_Depth[ask_price] = df_Market_Depth['sell'].apply(lambda x: x[i]['price'])
            df_Market_Depth[ask_orders] = df_Market_Depth['sell'].apply(lambda x: x[i]['orders'])
            df_Market_Depth[ask_qty] = df_Market_Depth['sell'].apply(lambda x: x[i]['quantity'])

    def  getOptionChainLiteMarketDepth(self, Configuration, SP_LIST, option_type, expiry, symbol):

        time_start = time.time()
        df_Market_Depth = self.callMDSOptionChainLiteMarketDepth(Configuration, SP_LIST, option_type, expiry, symbol)
        time_end = time.time()
        logging.info('Time taken by MDS OptionChain Lite Market Depth Call: '+str(time_end-time_start))

        # TODO - Read Depth and Index Spot Price
        # Filter dataframe by rows or index
        df_Market_Depth = df_Market_Depth.T

        df_Market_Depth.index = df_Market_Depth.index.astype('int64')
        df_Market_Depth = df_Market_Depth.loc[SP_LIST]
        df_Market_Depth = df_Market_Depth.dropna()

        # Transformation -- Add BID ASK PRICE/QTY/ORDERS to LTP Data Frame as ROWS
        self.addBidAskToMarketDepth(df_Market_Depth)
        return df_Market_Depth

    def callMDSOptionChainLiteMarketDepth(self, Configuration, SP_LIST, option_type, expiry, symbol):
        '''
        MDS LTP API call for Order Gen and Square Off Job to calculate current estimates
        :param constituentType:
        :param constituents:
        :param expiry:
        :param symbol:
        :return:
        '''
        df_Market_Depth = pd.DataFrame()
        max_retries = 2
        retry = 0
        while (retry < max_retries):

            try:
                mds_option_chain_lite_market_depth_url = Configuration["MDS_URL"] + '/nse/marketDepthController'
                #logging.info("MDS OPTION CHAIN URL: {}".format(mds_option_chain_lite_market_depth_url))
                payload = {
                      "symbol": symbol,
                      "derivativeDetails": {
                        "strikePrice":SP_LIST,
                        "expiryDate": expiry,
                        "type": option_type
                      }
                    }
                option_chain_lite_dict = requests.post(mds_option_chain_lite_market_depth_url, headers={'X_AUTH': Configuration["MDS_AUTH_KEY"]}, json=payload, timeout= 60).json()
                # TODO: Mock LTP Response
                #file_path = "/home/ec2-user/venv/python36/lib/python3.6/site-packages/oops1/src/data/mock/ltp_json_" + symbol + ".txt"
                # file_path = "E:\\Workspace\\Workspace_Python_2019\\NeutralOptionsSelling\\oops1\\src\\data\\mock\\options_chain_lite_" + symbol + ".txt"
                # with open(file_path) as json_file:
                #     option_chain_lite_dict = json.load(json_file)

                df_Market_Depth = pd.DataFrame.from_dict(option_chain_lite_dict, orient='columns')
                break
            except Exception as exception:
                print('Exception occurred::', exception)
                logging.info('Exception occurred::' + str(exception))
                logging.info(traceback.format_exc())
                # retrying in case of failures
                time.sleep(2)
                retry = retry + 1
                if retry == max_retries:
                    raise Exception(Configuration['SCHEMA_NAME']+': MDS OPTION CHAIN LITE API Not Responding')
        return df_Market_Depth

    def getStrikePricesAPI(self, Configuration, symbol, expiry_date):

        df_Call = pd.DataFrame()
        df_Put = pd.DataFrame()
        time_start = time.time()
        df_Strike_Price = self.callStrikePricesAPI(Configuration, symbol, expiry_date)
        time_end = time.time()
        logging.info('Time taken by MDS STRIKE PRICES Call: ' + str(time_end - time_start))
        df_Strike_Price.columns = [str(col).upper() for col in df_Strike_Price.columns]
        df_Strike_Price["STRIKE_PRICE"] = pd.to_numeric(df_Strike_Price["STRIKE_PRICE"])
        df_Call["STRIKE_PRICE"] = df_Strike_Price["STRIKE_PRICE"]
        df_Put["STRIKE_PRICE"] = df_Strike_Price["STRIKE_PRICE"]
        spot_value, future_value = df_Strike_Price['LTP'].iloc[0], df_Strike_Price['LTP_FUTURE'].iloc[0]
        #print(spot_value, future_value)

        return df_Call, df_Put, spot_value, future_value

    def callStrikePricesAPI(self, Configuration, symbol, expiry_date):
        '''
        MDS LTP API call for Order Gen and Square Off Job to calculate current estimates
        :param constituentType:
        :param constituents:
        :param expiry:
        :param symbol:
        :return:
        '''
        df_Strike_Price = pd.DataFrame()
        max_retries = 2
        retry = 0
        while (retry < max_retries):

            try:
                mds_strike_price_url= Configuration["MDS_URL"] + '/nse/strikeprice/?symbol='+symbol+'&expiryDate='+expiry_date
                logging.info("mds_strike_price_url: {}".format(mds_strike_price_url))
                strike_price_dict = requests.get(mds_strike_price_url,timeout= 60).json()
                # TODO: Mock LTP Response
                # file_path = "/home/ec2-user/venv/python36/lib/python3.6/site-packages/oops1/src/data/mock/strike_price_" + symbol + ".txt"
                # file_path = "E:\\Workspace\\Workspace_Python_2019\\NeutralOptionsSelling\\oops1\\src\\data\\mock\\strike_price_" + symbol + ".txt"
                # with open(file_path) as json_file:
                #     strike_price_dict = json.load(json_file)

                df_Strike_Price = pd.DataFrame.from_dict(strike_price_dict, orient='columns')
                break
            except Exception as exception:
                print('Exception occurred::', exception)
                logging.info('Exception occurred::' + str(exception))
                logging.info(traceback.format_exc())
                # retrying in case of failures
                time.sleep(2)
                retry = retry + 1
                if retry == max_retries:
                    raise Exception(Configuration['SCHEMA_NAME']+': MDS STRIKE PRICE API Not Responding')
        return df_Strike_Price

    def callHistoricalPricesAPI(self, fromDateTime, toDateTime, Configuration, symbol):
        # MOCK DATA
        # fromDateTime='2022-07-25+13:50:00'
        # toDateTime='2022-07-25+15:30:00'

        CANDLES_TIME_FRAME = Configuration['CANDLES_TIME_FRAME']
        df_Historical_Prices = pd.DataFrame()
        max_retries = 2
        retry = 0
        while (retry < max_retries):

            try:
                mds_historical_price_url = Configuration["MDS_URL"] + '/nse/historical/candles?' \
                                                                      'symbol=' + symbol + '&timeframe=' + CANDLES_TIME_FRAME + \
                                           '&startDate=' + fromDateTime + '&endDate=' + toDateTime + '&oi=1'

                historical_price_dict = requests.get(mds_historical_price_url, timeout=60).json()

                # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\historical_price_" + symbol + ".txt"
                # with open(file_path) as json_file:
                #     historical_price_dict = json.load(json_file)

                df_Historical_Prices = pd.DataFrame(historical_price_dict['data']['candles'])
                df_Historical_Prices.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'oi']
                break
            except Exception as exception:
                print('Exception occurred::', exception)
                logging.info('Exception occurred::' + str(exception))
                logging.info(traceback.format_exc())
                # retrying in case of failures
                time.sleep(2)
                retry = retry + 1
                if retry == max_retries:
                    raise Exception(Configuration['SCHEMA_NAME']+': MDS HISTORICAL PRICE API Not Responding')
        return df_Historical_Prices

    def callHistoricalPricesAPIForVIX(self, fromDateTime,toDateTime,Configuration, symbol):
        # MOCK DATA
        #fromDateTime='2022-07-25+13:50:00'
        #toDateTime='2022-07-25+15:30:00'

        CANDLES_TIME_FRAME = Configuration['CANDLES_TIME_FRAME_VIX']
        df_Historical_Prices = pd.DataFrame()
        max_retries = 2
        retry = 0
        vix = 0.0
        while (retry < max_retries):

            try:
                mds_historical_price_url = Configuration["MDS_URL"] + '/nse/historical/candles?' \
                                                                      'symbol='+symbol+'&timeframe='+CANDLES_TIME_FRAME+\
                                           '&startDate='+fromDateTime+'&endDate='+toDateTime+'&oi=1'

                historical_price_dict = requests.get(mds_historical_price_url,timeout= 60).json()

                # file_path = "E:\\Workspace\\Workspace_Python_2019\\brahma\\data\\mock\\vix.txt"
                # with open(file_path) as json_file:
                #     historical_price_dict = json.load(json_file)

                df_Historical_Prices = pd.DataFrame(historical_price_dict['data']['candles'])
                df_Historical_Prices.columns = ['date', 'open', 'high', 'low', 'close', 'volume','oi']
                vix = df_Historical_Prices['close'].iloc[0]
                logging.info(Configuration['SCHEMA_NAME'] + ': VIX::' + str(vix))
                break
            except Exception as exception:
                print('Exception occurred::', exception)
                logging.info('Exception occurred::' + str(exception))
                logging.info(traceback.format_exc())
                # retrying in case of failures
                time.sleep(2)
                retry = retry + 1
                if retry == max_retries:
                    raise Exception(Configuration['SCHEMA_NAME']+': MDS HISTORICAL PRICE API Not Responding')
        return vix


if __name__ == "__main__":
    symbol = 'BANKNIFTY'
    expiry_date = '20FEB06'
    mdsAdaptor = MDSAdaptor()
    mdsAdaptor.getStrikePricesAPI(symbol, expiry_date)