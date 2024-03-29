
-- ############################################ BACKTEST ANALYSIS ################################################
-- ############################################ BACKTEST WEDNESDAY 1500 ################################################

-- BRAHMA_V1_BACKTESTING_AG - BACKTESTING DATA
-- SOME RULES LIKE 
select POSITION_GROUP_ID, CONTRACT_EXPIRY_DATE, SYMBOL, REALIZED_PNL_OVERALL, SUBSTRING(CONTRACT_EXPIRY_DATE,3,3) AS MONTHNAME,
STRIKE_PRICE, INSTRUMENT_TYPE, TRANSACTION_TYPE, 
QUANTITY,  ENTRY_IV, CURRENT_IV, ENTRY_ATM_AVG_PRICE, ENTRY_UNDERLYING, CURRENT_UNDERLYING
FROM BRAHMA_V1_BACKTESTING_AG.POSITIONS_TRACKING_BACK_UP
JOIN ( SELECT max(POSITION_TRACKING_GROUP_ID) POSITION_TRACKING_GROUP_ID FROM BRAHMA_V1_BACKTESTING_AG.POSITIONS_TRACKING_BACK_UP
 where ORDER_MANIFEST like '%CALL_BUY_LEG_1_SQUAREOFF_FINAL'
GROUP BY CONTRACT_EXPIRY_DATE
) last ON POSITIONS_TRACKING_BACK_UP.POSITION_TRACKING_GROUP_ID = last.POSITION_TRACKING_GROUP_ID
and ORDER_MANIFEST like '%CALL_BUY_LEG_1_SQUAREOFF_FINAL';


-- ############################################ BACKTEST Reports ################################################

-- BRAHMA_V1_BACKTESTING_AG - BACKTESTING DATA IF WE START BRICS AT 15:00 WEDNESDAY
-- SOME RULES LIKE 
select POSITION_GROUP_ID, CONTRACT_EXPIRY_DATE, SYMBOL, REALIZED_PNL_OVERALL, SUBSTRING(CONTRACT_EXPIRY_DATE,3,3) AS MONTHNAME,
STRIKE_PRICE, INSTRUMENT_TYPE, TRANSACTION_TYPE, 
QUANTITY,  ENTRY_IV, CURRENT_IV, ENTRY_ATM_AVG_PRICE, ENTRY_UNDERLYING, CURRENT_UNDERLYING
FROM BRAHMA_V1_BACKTESTING_AG.POSITIONS_TRACKING_BACK_UP_WED_1500
JOIN ( SELECT max(POSITION_TRACKING_GROUP_ID) POSITION_TRACKING_GROUP_ID FROM BRAHMA_V1_BACKTESTING_AG.POSITIONS_TRACKING_BACK_UP_WED_1500
 where ORDER_MANIFEST like '%CALL_BUY_LEG_1_SQUAREOFF_FINAL'
GROUP BY CONTRACT_EXPIRY_DATE
) last ON POSITIONS_TRACKING_BACK_UP_WED_1500.POSITION_TRACKING_GROUP_ID = last.POSITION_TRACKING_GROUP_ID
and ORDER_MANIFEST like '%CALL_BUY_LEG_1_SQUAREOFF_FINAL';

-- BRAHMA_V1_BACKTESTING_AG - BACKTESTING DATA IF WE START BRICS AT