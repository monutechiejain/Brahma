import pandas as pd
import numpy as np
from adaptor import OMAdaptor
from dao import PositionsDAO
import logging
from common import utils
import time
import traceback

def reconciliationSquareoff(df_Positions_db, df_ActivePositions, Configuration, OM_URL,
                                                   symbol, position_group_id):

    ############################################ INITIALIZE RECONCILE DF ###############################################
    LOT_SIZE = float(Configuration['LOT_SIZE_'+symbol])
    df_reconcile_squareoff = pd.DataFrame()
    df_reconcile_db = pd.DataFrame()
    df_ActivePositions_Parent = df_ActivePositions.copy()
    df_Positions_db_Parent = df_Positions_db.copy()
    is_reconcile_broker = False

    ################################ SQUAREOFF IF ACTIVE AND DB POSITIONS MISMATCH #####################################
    ################################ CASE 1 : POSITIONS MISMATCH AND QTY MATCH FOR REMAINING POSITIONS #################
    if len(df_Positions_db) != len(df_ActivePositions) and len(df_ActivePositions) > len(df_Positions_db):
        df_reconcile_squareoff = df_reconcile_squareoff.append(df_ActivePositions[~df_ActivePositions['tradingsymbol'].
                                                               isin(df_Positions_db['tradingsymbol'])], ignore_index=True)
        df_reconcile_squareoff = df_reconcile_squareoff[['quantity','tradingsymbol']]
        df_ActivePositions_merged = pd.merge(df_Positions_db, df_ActivePositions, how='inner', on=['tradingsymbol'])
        df_ActivePositions_merged = df_ActivePositions_merged[['tradingsymbol']]
        df_ActivePositions = pd.merge(df_ActivePositions, df_ActivePositions_merged, how='inner', on=['tradingsymbol'])

    ################################ CASE 2 : POSITIONS MATCH AND QTY MISMATCH FOR REMAINING POSITIONS #################
    if len(df_Positions_db) == len(df_ActivePositions) and len(df_Positions_db) != 0 and len(df_ActivePositions) != 0:
        df_ActivePositions = pd.merge(df_Positions_db, df_ActivePositions, on='tradingsymbol',how='inner')

        # ONLY CONSIDER POSITIONS WHERE BROKER HAS MORE QUANTITY THAN DB
        df_ActivePositions['isMatched'] = np.where(df_ActivePositions['quantity_active'] <= df_ActivePositions['quantity_db'], True, False)
        df_Not_Matched = df_ActivePositions[df_ActivePositions['isMatched'] == False]
        df_Not_Matched.reset_index(drop=True, inplace=True)

        if len(df_Not_Matched) > 0:
            df_Not_Matched['quantity_squareoff'] = np.sign(df_Not_Matched['quantity']).mul(df_Not_Matched['quantity_active'].sub(df_Not_Matched['quantity_db']))
            df_Not_Matched['quantity'] = df_Not_Matched['quantity_squareoff']
            df_Not_Matched = df_Not_Matched[['quantity','tradingsymbol']]
            df_reconcile_squareoff = pd.concat([df_reconcile_squareoff, df_Not_Matched], ignore_index=True)

    ################################ ADD ORDER TYPE TO RECONCILLIATION SQUAREOFF DF ####################################
    if len(df_reconcile_squareoff) > 0:

        ################################################ ORDER TYPE AND QUANTITY #######################################
        df_reconcile_squareoff['order_type'] =  np.where(df_reconcile_squareoff['quantity']>0, 'SELL', 'BUY')  # Reverse as squaring off
        df_reconcile_squareoff['quantity'] = df_reconcile_squareoff['quantity'].abs()

        ################################################# SQUAREOFF CALL ################################################
        OMAdaptor.squareOffPositionsByDataframe(df_reconcile_squareoff, Configuration, OM_URL, symbol, position_group_id)
        is_reconcile_broker = True
        ################################################################################################################

    # RESET FOR DB RECONCILIATION
    df_ActivePositions = df_ActivePositions_Parent
    df_Positions_db = df_Positions_db_Parent
    ################################ CASE 3 : POSITIONS MISMATCH AND QTY MATCH FOR REMAINING POSITIONS #################
    if len(df_Positions_db) != len(df_ActivePositions) and len(df_ActivePositions) < len(df_Positions_db):
        df_reconcile_db = df_reconcile_db.append(df_Positions_db[~df_Positions_db['tradingsymbol'].isin(df_ActivePositions['tradingsymbol'])], ignore_index=True)
        df_reconcile_db = df_reconcile_db[['oops_positions_id', 'quantity_db', 'tradingsymbol']]
        df_reconcile_db['is_active'] = 'N'

        logging.info('RECONCILE DB AND MARKED INACTIVE FLAG - {}'.format(df_reconcile_db.head()))
        # UPDATE DB WITH INACTIVE FLAG
        for index, row_position in df_reconcile_db.iterrows():
            # UPDATE POSITIONS TABLE
            PositionsDAO.updatePositionsMarkInactive(row_position)

        df_Positions_merged = pd.merge(df_Positions_db, df_ActivePositions, how='inner', on=['tradingsymbol'])
        df_Positions_merged = df_Positions_merged[['tradingsymbol']]
        df_Positions_db = pd.merge(df_Positions_db, df_Positions_merged, how='inner',
                                      on=['tradingsymbol'])

    ################################ CASE 4 : POSITIONS MATCH AND QTY MISMATCH FOR REMAINING POSITIONS #################
    if len(df_Positions_db) == len(df_ActivePositions) and len(df_Positions_db) != 0 and len(df_ActivePositions) != 0:
        df_Positions_db = pd.merge(df_ActivePositions, df_Positions_db ,on='tradingsymbol',how='inner')

        # ONLY CONSIDER POSITIONS WHERE DB HAS MORE QTY THAN ZERODHA
        df_Positions_db['isMatched'] = np.where(df_Positions_db['quantity_db'] <= df_Positions_db['quantity_active'], True, False)
        df_Not_Matched_db = df_Positions_db[df_Positions_db['isMatched'] == False]
        df_Not_Matched_db.reset_index(drop=True, inplace=True)

        if len(df_Not_Matched_db) > 0:
            df_Not_Matched_db['quantity'] = df_Not_Matched_db['quantity_active']
            df_Not_Matched_db['quantity'] = df_Not_Matched_db['quantity'].astype(float)
            df_Not_Matched_db['multi_factor'] = df_Not_Matched_db['quantity']/LOT_SIZE
            df_Not_Matched_db = df_Not_Matched_db[['oops_positions_id','quantity','multi_factor']]
            df_Not_Matched_db[['oops_positions_id','quantity','multi_factor']] = df_Not_Matched_db[['oops_positions_id','quantity','multi_factor']].astype(str)

            logging.info('RECONCILE DB AND UPDATE QTY AND MF - {}'.format(df_Not_Matched_db.head()))
            # UPDATE DB WITH QUANTITY AND MF
            for index, row_position in df_Not_Matched_db.iterrows():
                # UPDATE POSITIONS TABLE
                PositionsDAO.updatePositionsQtyMF(row_position)

def reconcileAveragePrice(df_Order, Configuration, OM_URL, orderType, symbol):
    max_retries = 3
    retry = 0
    tradeType = Configuration['TRADE_TYPE']
    time.sleep(3)
    while (retry < max_retries):

        try:

            # Fetch Active Positions
            activePositionsResponse = utils.getOMServiceCallHelper(Configuration).fetchActivePositions(Configuration, OM_URL, orderType)

            df_ActivePositions = pd.DataFrame(activePositionsResponse)

            if len(df_ActivePositions) == 0:
                raise ValueError("No Active Positions while reconciling for Average Price!!")

            # GET ONLY SELECTED TRADE TYPE e.g. MIS ONLY
            df_ActivePositions = df_ActivePositions[(df_ActivePositions['product'] == tradeType) &
                                                    (df_ActivePositions["tradingsymbol"].str.startswith(symbol, na=False)) ]
            df_ActivePositions['quantity_active'] = df_ActivePositions['quantity'].abs()
            df_ActivePositions['quantity_active'] = df_ActivePositions['quantity_active'].astype(float)
            df_ActivePositions = df_ActivePositions[['quantity', 'quantity_active', 'tradingsymbol', 'buy_quantity',
                                                     'buy_price', 'sell_quantity', 'sell_price']]
            df_ActivePositions.reset_index(drop=True, inplace=True)

            # Iterating over df_order
            for index, row in df_Order.iterrows():

                row_active_position = df_ActivePositions[(df_ActivePositions["tradingsymbol"] == row['tradingsymbol'])].iloc[0]

                # SET CALL PRICES
                if row['ORDER_TYPE'] == 'BUY':
                    df_Order.at[index, 'average_price'] = row_active_position['buy_price']
                else:
                    df_Order.at[index, 'average_price'] = row_active_position['sell_price']

            break
        except Exception as exception:
            message = 'Reconciliation Average Price :: Exception occurred while Fetching Active Positions for OrderType {} , Exception: {}'.format(
                str(orderType), str(exception))
            logging.info(message)
            print(message)
            logging.info(traceback.format_exc())
            # retrying in case of failures
            time.sleep(3)
            retry = retry + 1
            if retry == max_retries:
                if Configuration['NOTIFICATIONS_ACTIVE'] == 'Y':
                    subject = "FAILURE | " + Configuration['SCHEMA_NAME'] + " | " + symbol + " | " + orderType
                    utils.send_email_dqns(Configuration, subject, message, "HTML")
            #     raise Exception(message)


    return df_Order




