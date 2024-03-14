import logging
from common import PositionsUtil
import numpy as np
from dao import PositionsDAO, PositionsBackUpDAO, PositionsTrackingDAO, PositionsTrackingBackUpDAO

########################################################## CHECK CLEANUP ###############################################
def checkCleanUp():
    isCleanUpDone = True
    df_positions_all = PositionsDAO.getPositionsAll()

    if len(df_positions_all) > 0 :
        isCleanUpDone = False

    return isCleanUpDone


########################################################## PERFORM CLEANUP #############################################
def doCleanUp(Configuration):

    message = "CLEANUP DONE SUCCESSFULLY!!!"

    df_positions_all = PositionsDAO.getPositionsAll()
    df_positions_all = df_positions_all.replace({np.nan: None})
    # Convert all columns to lowercase
    df_positions_all.columns = [x.lower() for x in df_positions_all.columns]

    # POSITION GROUP ID LIST
    position_group_id_list = df_positions_all['position_group_id'].unique()

    positionsResponseDict = PositionsUtil.fetchPositions(Configuration)
    activePositionsCount = float(positionsResponseDict['activePositionsCount'])

    if activePositionsCount > 0:
        message = "CLEAN UP CANNOT BE PERFOMED AS THERE ARE ACTIVE POSITIONS IN BROKER TERMINAL!!"
        return message

    if len(df_positions_all) > 0 and activePositionsCount == 0:
        position_id_list = []
        for index, row_position in df_positions_all.iterrows():
            PositionsBackUpDAO.insert(row=row_position)
            position_id_list.append(row_position['position_id'])

        print('Inserted Successfully in PositionsBackUp Table By Clean Up Job.')
        logging.info("Inserted Successfully in PositionsBackUp Table By Clean Up Job.")

        # DELETE FROM POSITIONS TABLE
        PositionsDAO.deletePositionsByPositionIds(position_id_list)

        # DELETE ALL FROM POSITIONS TABLE
        PositionsDAO.deletePositionsAll()
        logging.info("Deleted Successfully from Positions Table")

        ################################ BAKUP POSITIONS TRACKING TABLE ############################################
        for position_group_id in position_group_id_list:
            df_positions_tracking_all = PositionsTrackingDAO.getPositionsByPositionGroupId(position_group_id)
            df_positions_tracking_all = df_positions_tracking_all.replace({np.nan: None})
            # Convert all columns to lowercase
            df_positions_tracking_all.columns = [x.lower() for x in df_positions_tracking_all.columns]
            if len(df_positions_tracking_all) > 0:
                position_tracking_list = []
                for index, row in df_positions_tracking_all.iterrows():
                    PositionsTrackingBackUpDAO.insert(row=row)
                    position_tracking_list.append(row['position_tracking_id'])

                print('Inserted Successfully in Position Tracking Back Up Table By Clean Up Job.')
                logging.info("Inserted Successfully in Position Tracking Back Up Table By Clean Up Job.")

                # DELETE FROM POSITIONS TABLE
                PositionsTrackingDAO.deletePositionsByPositionGroupId(position_group_id)

                # FULL DELETE FROM POSITIONS TRACKING TABLE
                PositionsTrackingDAO.deletePositionsTrackingAll()
                logging.info("Deleted Successfully from Positions Tracking Table By Clean Up Job.")
            ########################################################################################################

    return message



