
from common import utils
from common import MarginUtil
from common.constants import index_list
import pandas as pd
import numpy as np
from dao import PositionsDAO,PositionsTrackingDAO
from common import utils
import json
import copy


############################################## FETCH ACTIVE POSITIONS REALTIME #########################################
def fetchPositions(Configuration):
    positionsResponseDict = {}
    netPositionsResponseDict = {}
    activePositionsResponseDict = {}

    ############################################### INITIALIZE ACTIVE POSITIONS DATA ###################################
    activePositionsResponseDict['activePositionsCount'] = 0
    activePositionsResponseDict['activePositionPnl'] = 0.0
    activePositionsResponseDict['activePositionsCount_BANKNIFTY'] = 0
    activePositionsResponseDict['activePositionsPnl_BANKNIFTY'] = 0.0
    activePositionsResponseDict['activePositionsCount_NIFTY'] = 0
    activePositionsResponseDict['activePositionsPnl_NIFTY'] = 0.0

    ############################################### INITIALIZE NET POSITIONS DATA ######################################
    netPositionsResponseDict['netPnl'] = 0.0
    netPositionsResponseDict['netPositionsCount'] = 0
    netPositionsResponseDict['netPnl_BANKNIFTY'] = 0.0
    netPositionsResponseDict['netPositionsCount_BANKNIFTY'] = 0
    netPositionsResponseDict['netPnl_NIFTY'] = 0.0
    netPositionsResponseDict['netPositionsCount_NIFTY'] = 0

    ################################################## ADD 2 IMP PARAMETERS ###########################################
    positionsResponseDict["activePositionsCount"] = 0
    positionsResponseDict["activePositionsCount_BANKNIFTY"] = 0
    positionsResponseDict["activePositionsCount_NIFTY"] = 0
    positionsResponseDict["netPnl"] = 0.0

    orderType = "FETCH_ACTIVE_POSITIONS"
    OM_URL = Configuration['OM_URL']
    tradeType = Configuration['TRADE_TYPE']

    ########################################## FETCH SYMBOLS LIST ######################################################
    symbol1, symbol2 = index_list[0], index_list[1]

    ###################################### MARGIN AND ACTIVE POSITIONS CALL ############################################
    positionsResponse = utils.getOMServiceCallHelper(Configuration).fetchActivePositions(Configuration, OM_URL,
                                                                       orderType)

    ########################################## POPULATE DATAFRAMES ######################################################
    df_Positions_BANKNIFTY = pd.DataFrame()
    df_Positions_NIFTY = pd.DataFrame()
    df_ActivePositions_All = pd.DataFrame()
    df_ActivePositions_BANKNIFTY = pd.DataFrame()
    df_ActivePositions_NIFTY = pd.DataFrame()

    ######################################### FETCH ACTIVE POSITIONS ###################################################
    df_Positions_All = pd.DataFrame(positionsResponse)
    if df_Positions_All is not None and len(df_Positions_All) > 0:
        df_Positions_All = df_Positions_All[(df_Positions_All['product'] == tradeType) &
                            (df_Positions_All["tradingsymbol"].str.startswith(symbol1,na=False) |
                             (df_Positions_All["tradingsymbol"].str.startswith(symbol2,na=False)))]
        df_Positions_BANKNIFTY = df_Positions_All[(df_Positions_All["tradingsymbol"].str.startswith(symbol1, na=False))]
        df_Positions_NIFTY = df_Positions_All[(df_Positions_All["tradingsymbol"].str.startswith(symbol2, na=False))]
        df_ActivePositions_All = df_Positions_All[(df_Positions_All['quantity'] != 0)]
        df_ActivePositions_BANKNIFTY = df_ActivePositions_All[(df_Positions_All["tradingsymbol"].str.startswith(symbol1, na=False))]
        df_ActivePositions_NIFTY = df_ActivePositions_All[(df_Positions_All["tradingsymbol"].str.startswith(symbol2, na=False))]

    ###################################### ACTIVE POSITIONS/PNL OVERALL ###################################################
    if df_ActivePositions_All is not None and len(df_ActivePositions_All) > 0:
        activePositionsResponseDict['activePositionsCount'] = len(df_ActivePositions_All)
        positionsResponseDict["activePositionsCount"] = len(df_ActivePositions_All)   # THIS IS DUPLICATE, ADDING IN PARENT DICT
        activePositionsResponseDict['activePositionPnl'] = df_ActivePositions_All['pnl'].sum()

    ####################################### BANKNIFTY ACTIVE ##########################################################
    if df_ActivePositions_BANKNIFTY is not None and len(df_ActivePositions_BANKNIFTY) > 0:
        activePositionsResponseDict['activePositionsCount_BANKNIFTY'] = len(df_ActivePositions_BANKNIFTY)
        positionsResponseDict["activePositionsCount_BANKNIFTY"] = len(df_ActivePositions_BANKNIFTY)   # THIS IS DUPLICATE, ADDING IN PARENT DICT
        activePositionsResponseDict['activePositionsPnl_BANKNIFTY'] = df_ActivePositions_BANKNIFTY['pnl'].sum()

    ######################################## NIFTY ACTIVE #############################################################
    if df_ActivePositions_NIFTY is not None and len(df_ActivePositions_NIFTY) > 0:
        activePositionsResponseDict['activePositionsCount_NIFTY'] = len(df_ActivePositions_NIFTY)
        positionsResponseDict['activePositionsCount_NIFTY'] = len(df_ActivePositions_NIFTY) # THIS IS DUPLICATE, ADDING IN PARENT DICT
        activePositionsResponseDict['activePositionsPnl_NIFTY'] = df_ActivePositions_NIFTY['pnl'].sum()

    ###################################### NET POSITIONS/PNL OVERALL ###################################################
    if df_Positions_All is not None and len(df_Positions_All) > 0:
        netPositionsResponseDict['netPnl'] = df_Positions_All['pnl'].sum()
        positionsResponseDict["netPnl"] = df_Positions_All['pnl'].sum() # THIS IS DUPLICATE, ADDING IN PARENT DICT
        netPositionsResponseDict['netPositionsCount'] = len(df_Positions_All)

    ####################################### BANKNIFTY OVERALL ##########################################################
    if df_Positions_BANKNIFTY is not None and len(df_Positions_BANKNIFTY) > 0:
        netPositionsResponseDict['netPnl_BANKNIFTY'] = df_Positions_BANKNIFTY['pnl'].sum()
        netPositionsResponseDict['netPositionsCount_BANKNIFTY'] = len(df_Positions_BANKNIFTY)

    ######################################## NIFTY OVERALL ##########################################################
    if df_Positions_NIFTY is not None and len(df_Positions_NIFTY) > 0:
        netPositionsResponseDict['netPnl_NIFTY'] = df_Positions_NIFTY['pnl'].sum()
        netPositionsResponseDict['netPositionsCount_NIFTY'] = len(df_Positions_NIFTY)


    ###################################### POPULATE POSITIONS RESPONSE ################################################
    positionsResponseDict['marginProfileResponse'] = MarginUtil.getMarginProfileDetails(Configuration)
    positionsResponseDict['activePositionsDetails'] = activePositionsResponseDict
    positionsResponseDict['netPositionsDetails'] = netPositionsResponseDict
    positionsDetails = {}
    positionsDetails['activePositions_BANKNIFTY'] = df_ActivePositions_BANKNIFTY.to_dict(orient='records')
    positionsDetails['activePositions_NIFTY'] = df_ActivePositions_NIFTY.to_dict(orient='records')
    positionsDetails['netPositions_BANKNIFTY'] = df_Positions_BANKNIFTY.to_dict(orient='records')
    positionsDetails['netPositions_NIFTY'] = df_Positions_NIFTY.to_dict(orient='records')

    positionsResponseDict['positionsDetailsList']=positionsDetails
    return positionsResponseDict