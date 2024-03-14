from dao import GlobalConfigurationDAO, PositionsDAO, PositionsTrackingDAO, PositionsBackUpDAO, PositionsTrackingBackUpDAO
from setup import setup

def cleanUp(schema_name):

    # RESET CONFIG
    setup.configSetUp()

    # DELETE ALL FROM POSITIONS TABLE
    PositionsDAO.deletePositionsAll(schema_name)
    print("Deleted All Positions from Positions Table!!")

    # FULL DELETE FROM POSITIONS TRACKING TABLE
    PositionsTrackingDAO.deletePositionsTrackingAll(schema_name)
    print("Deleted All from Positions Tracking Table!!")

    # DELETE ALL FROM POSITIONS BACK UP TABLE
    PositionsBackUpDAO.deletePositionsBackUpAll(schema_name)
    print("Deleted All Positions from Positions Table!!")

    # FULL DELETE FROM POSITIONS TRACKING Back UP TABLE
    PositionsTrackingBackUpDAO.deletePositionsTrackingBackUpAll(schema_name)
    print("Deleted All from Positions Tracking Back Up Table!!")

def fetchPositionsTrackingLatestData(schema_name):
    df_positions_tracking_last_itr_result = PositionsTrackingDAO.getLatestPositionsTrackingRows(schema_name)
    return df_positions_tracking_last_itr_result

def fetchPositionsTrackingBackUpLatestData(schema_name):
    df_positions_tracking_bak_last_itr_result = PositionsTrackingBackUpDAO.getLatestPositionsTrackingBackUpRows(schema_name)
    return df_positions_tracking_bak_last_itr_result

if __name__ == '__main__':
    fetchPositionsTrackingLatestData()