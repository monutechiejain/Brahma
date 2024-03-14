
import math
import pandas as pd
import logging

def qtyFreezeSplit(df_positions_existing, Configuration, symbol):
    isQtyFreeze = False
    ############################################## COPY DF TO FOR QTY FREEZE SCENARIO ##################################
    df_positions_existing_qty_freeze_initial =  df_positions_existing.copy()

    QTY_FREEZE_LIMIT_MF = float(Configuration["QTY_FREEZE_LIMIT_MF_" + symbol])
    df_positions_existing_qty_freeze_initial['QUANTITY'] = df_positions_existing_qty_freeze_initial['QUANTITY'].astype(float)
    df_positions_existing_qty_freeze_initial['NUM_LOTS'] = df_positions_existing_qty_freeze_initial['NUM_LOTS'].astype(float)

    ############################# CHECK IF ANY QTY IS GREATER THAN QTY FREEZE LIMIT #####################################
    df_positions_with_qty_freeze = df_positions_existing_qty_freeze_initial[df_positions_existing_qty_freeze_initial['NUM_LOTS'] >= QTY_FREEZE_LIMIT_MF]
    df_positions_without_qty_freeze = df_positions_existing_qty_freeze_initial[df_positions_existing_qty_freeze_initial['NUM_LOTS'] < QTY_FREEZE_LIMIT_MF]

    ############################################## RETURN IF QTY FREEZE LIMIT NOT BREACHED #############################
    if df_positions_with_qty_freeze is not None and len(df_positions_with_qty_freeze) ==0:
        return df_positions_existing, isQtyFreeze

    ######################################## SPLIT AND APPEND DATAFRAME TO CATER QTY FREEZE #############################
    isQtyFreeze = True
    df_positions_with_qty_freeze_temp = pd.DataFrame()
    # FOR LOOP FOR QTY FREEZE
    for index, row in df_positions_with_qty_freeze.iterrows():
        NUM_LOTS = row['NUM_LOTS']
        LOT_SIZE = float(row['LOT_SIZE'])
        NUM_LOTS_REPS, NUM_LOTS_NTH_BELOW, NUM_LOTS_NTH = populateMultiFactorReps(NUM_LOTS, QTY_FREEZE_LIMIT_MF)
        logging.info('QtyFreezeHelper :: STRIKE PRICE: {}, NUM_LOTS_REPS: {}, NUM_LOTS_NTH_BELOW: {}, '
                     'NUM_LOTS_NTH: {}'.format(str(row['STRIKE_PRICE']),str(NUM_LOTS_REPS),str(NUM_LOTS_NTH_BELOW), str(NUM_LOTS_NTH)))

        df_row = row.to_frame().transpose()
        df_row.reset_index(drop=True, inplace=True)
        df_row_N = pd.DataFrame()

        # REPLICATE ROWS IN DATAFRAME
        df_row_N = df_row_N.append([df_row]*NUM_LOTS_REPS, ignore_index=True)

        # UPDATE QTY, MF
        df_row_N = updateQtyFreezeData(df_row_N, NUM_LOTS_REPS,NUM_LOTS_NTH_BELOW, NUM_LOTS_NTH, NUM_LOTS,
                                                                LOT_SIZE)
        df_positions_with_qty_freeze_temp = df_positions_with_qty_freeze_temp.append(df_row_N, ignore_index=True)

    ################################################# FINAL DF - ADD SPLIT DF AND WITHOUT SPLIT DF ####################
    df_positions_existing = pd.concat([df_positions_without_qty_freeze, df_positions_with_qty_freeze_temp], ignore_index=True)

    return df_positions_existing, isQtyFreeze


def populateMultiFactorReps(MULTI_FACTOR, QTY_FREEZE_LIMIT_MF):
    MULTI_FACTOR_REPS = math.ceil(MULTI_FACTOR / QTY_FREEZE_LIMIT_MF)
    # if (math.ceil(MULTI_FACTOR / MULTI_FACTOR_REPS)) >= QTY_FREEZE_LIMIT_MF:
    #     MULTI_FACTOR_REPS = MULTI_FACTOR_REPS + 1

    MULTI_FACTOR_NTH_BELOW = float(math.ceil(MULTI_FACTOR/MULTI_FACTOR_REPS))
    MULTI_FACTOR_NTH = float(MULTI_FACTOR - ((MULTI_FACTOR_REPS-1)*MULTI_FACTOR_NTH_BELOW))
    return MULTI_FACTOR_REPS, MULTI_FACTOR_NTH_BELOW, MULTI_FACTOR_NTH


def updateQtyFreezeData(df_row_N, MULTI_FACTOR_REPS,MULTI_FACTOR_NTH_BELOW, MULTI_FACTOR_NTH, MULTI_FACTOR,
                                                                LOT_SIZE):

    for index, row in df_row_N.iterrows():

        if index < MULTI_FACTOR_REPS-1:
            df_row_N.at[index, 'MULTI_FACTOR'] = MULTI_FACTOR_NTH_BELOW
            df_row_N.at[index, 'QUANTITY'] = LOT_SIZE * MULTI_FACTOR_NTH_BELOW
        else:
            df_row_N.at[index, 'MULTI_FACTOR'] = MULTI_FACTOR_NTH
            df_row_N.at[index, 'QUANTITY'] = LOT_SIZE * MULTI_FACTOR_NTH

    return df_row_N


def qtyFreezeSplitSquareOffException(df_ActivePositions, Configuration, symbol):
    isQtyFreeze = False
    ############################################## COPY DF TO FOR QTY FREEZE SCENARIO ##################################
    df_ActivePositions_qty_freeze_initial =  df_ActivePositions.copy()
    df_ActivePositions_qty_freeze_initial.rename(columns={'quantity': 'QUANTITY'}, inplace=True)
    QTY_FREEZE_LIMIT_MF = float(Configuration["QTY_FREEZE_LIMIT_MF_" + symbol])
    LOT_SIZE = float(Configuration["LOT_SIZE_" + symbol])

    df_ActivePositions_qty_freeze_initial = df_ActivePositions_qty_freeze_initial.apply(lambda row: populateMultiFactorException(row, LOT_SIZE), axis=1)

    ############################# CHECK IF ANY QTY IS GREATER THAN QTY FREEZE LIMIT #####################################
    df_ActivePositions_with_qty_freeze = df_ActivePositions_qty_freeze_initial[df_ActivePositions_qty_freeze_initial['MULTI_FACTOR'] >= QTY_FREEZE_LIMIT_MF]
    df_ActivePositions_without_qty_freeze = df_ActivePositions_qty_freeze_initial[df_ActivePositions_qty_freeze_initial['MULTI_FACTOR'] < QTY_FREEZE_LIMIT_MF]

    ############################################## RETURN IF QTY FREEZE LIMIT NOT BREACHED #############################
    if df_ActivePositions_with_qty_freeze is not None and len(df_ActivePositions_with_qty_freeze) ==0:
        return df_ActivePositions, isQtyFreeze

    ######################################## SPLIT AND APPEND DATAFRAME TO CATER QTY FREEZE #############################
    isQtyFreeze = True
    df_ActivePositions_with_qty_freeze_temp = pd.DataFrame()
    # FOR LOOP FOR QTY FREEZE
    for index, row in df_ActivePositions_with_qty_freeze.iterrows():
        MULTI_FACTOR = row['MULTI_FACTOR']
        MULTI_FACTOR_REPS, MULTI_FACTOR_NTH_BELOW, MULTI_FACTOR_NTH = populateMultiFactorReps(MULTI_FACTOR, QTY_FREEZE_LIMIT_MF)
        logging.info('QtyFreezeHelper :: tradingsymbol: {}, MULTI_FACTOR_REPS: {}, MULTI_FACTOR_NTH_BELOW: {}, '
                     'MULTI_FACTOR_NTH: {}'.format(str(row['tradingsymbol']),str(MULTI_FACTOR_REPS),str(MULTI_FACTOR_NTH_BELOW), str(MULTI_FACTOR_NTH)))

        df_row = row.to_frame().transpose()
        df_row.reset_index(drop=True, inplace=True)
        df_row_N = pd.DataFrame()

        # REPLICATE ROWS IN DATAFRAME
        df_row_N = df_row_N.append([df_row]*MULTI_FACTOR_REPS, ignore_index=True)

        # UPDATE QTY, MF
        df_row_N = updateQtyFreezeData(df_row_N, MULTI_FACTOR_REPS,MULTI_FACTOR_NTH_BELOW, MULTI_FACTOR_NTH, MULTI_FACTOR,
                                                                LOT_SIZE)
        df_ActivePositions_with_qty_freeze_temp = df_ActivePositions_with_qty_freeze_temp.append(df_row_N, ignore_index=True)

    ################################################# FINAL DF - ADD SPLIT DF AND WITHOUT SPLIT DF ####################
    df_ActivePositions = pd.concat([df_ActivePositions_without_qty_freeze, df_ActivePositions_with_qty_freeze_temp], ignore_index=True)
    df_ActivePositions.rename(columns={'QUANTITY': 'quantity'}, inplace=True)
    return df_ActivePositions, isQtyFreeze

def populateMultiFactorException(row, LOT_SIZE):
    row['MULTI_FACTOR'] = float(row['QUANTITY']/LOT_SIZE)
    return row