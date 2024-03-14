-- USE BRAHMA_V1_SIT;
-- USE BRAHMA_V1_SIT_NA;
-- USE BRAHMA_V1_SIT_PT;

-- SETUP
-- -----------------------------
-- 1. set innodb_lock_wait_timeout=111
SHOW VARIABLES LIKE 'wait_%';
SHOW VARIABLES LIKE 'wait_%';
SHOW VARIABLES LIKE 'max_connection%';
show processlist;

-- ####################################################################################################################################
-- ##################################  BRAHMA_V1_SIT_CLUSTER   ##################################################################################
-- ####################################################################################################################################
-- GLOBAL DB QUERIES
select * from BRAHMA_V1_SIT.CONFIG;
select * from BRAHMA_V1_SIT.LIVE_ACCOUNTS;

-- insert into BRAHMA_V1_SIT.LIVE_ACCOUNTS select * from BRAHMA_V1_SIT.LIVE_ACCOUNTS;
-- insert into BRAHMA_V1_SIT_PT.POSITIONS select * from BRAHMA_V1_SIT_PT.POSITIONS;
-- insert into BRAHMA_V1_SIT_PT.POSITIONS_TRACKING select * from BRAHMA_V1_SIT_PT.POSITIONS_TRACKING;

-- CLIENT/LOCAL DB QUERIES
select * from BRAHMA_V1_SIT_NA.CONFIG;
select * from BRAHMA_V1_SIT_PT.CONFIG;



update BRAHMA_V1_SIT_NA.CONFIG set CONFIG_VALUE = 'N' where CONFIG_KEY = 'ORDER_GENERATION_ACTIVE';
update BRAHMA_V1_SIT_PT.CONFIG set CONFIG_VALUE = 'N' where CONFIG_KEY = 'ORDER_GENERATION_ACTIVE';

update BRAHMA_V1_SIT.CONFIG set CONFIG_VALUE = 'Y' where CONFIG_KEY = 'IS_MANUAL_OVERRIDE_BANKNIFTY';

-- ---------------------------------- BRAHMA_V1_SIT_NA -------------------------------------------------------------------
select * from BRAHMA_V1_SIT_NA.POSITIONS where IS_ACTIVE = true;

select * from BRAHMA_V1_SIT_NA.ORDERS where IS_SUCCESS = true;



-- TESTING
select * from BRAHMA_V1_SIT_NA.POSITION where IS_ACTIVE = true;
update BRAHMA_V1_SIT_NA.POSITION set IS_ACTIVE = true;
select * from BRAHMA_V1_SIT_NA.ORDERS where symbol ='NIFTY' ;
select * from BRAHMA_V1_SIT_NA.ORDERS where symbol ='NIFTY' and OOPS_TRANS_ID = 6 ORDER BY CREATED_DATE DESC;
select * from BRAHMA_V1_SIT_NA.ORDERS where symbol ='BANKNIFTY' and OOPS_TRANS_ID = 14 ORDER BY CREATED_DATE DESC;
select count(*)  from BRAHMA_V1_SIT_NA.OOPS_POSITIONS_TXN where symbol ='NIFTY' and INSTRUMENT_TYPE = 'FUTURES' and  OOPS_TRANS_ID = 14 ORDER BY CREATED_DATE DESC;
select count(*)  from BRAHMA_V1_SIT_NA.OOPS_POSITIONS_TXN where symbol ='BANKNIFTY' and INSTRUMENT_TYPE = 'FUTURES' and  OOPS_TRANS_ID = 14 ORDER BY CREATED_DATE DESC;
select * from BRAHMA_V1_SIT_NA.OOPS_POSITIONS_AUDIT where INSTRUMENT_TYPE = 'PUT';
select * from BRAHMA_V1_SIT_NA.OOPS_SQUAREOFF_AUDIT_BAK  where symbol ='NIFTY';
select * from BRAHMA_V1_SIT.CONFIG;  # GLOBAL CONFIG
select * from BRAHMA_V1_SIT_NA.CONFIG;  # CLIENT CONFIG
select * from BRAHMA_V1_SIT_NA.TRACKER;
select * from  BRAHMA_V1_SIT_NA.POSITIONS_TRACKING ORDER BY CREATED_DATE_TIME DESC;
select * from  BRAHMA_V1_SIT_NA.POSITIONS_BACK_UP ORDER BY CREATED_DATE_TIME DESC;
select * from  BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP ORDER BY CREATED_DATE_TIME DESC;
select * from BRAHMA_V1_SIT_NA.POSITION where IS_ACTIVE = true;

update BRAHMA_V1_SIT_NA.CONFIG set CONFIG_VALUE = 'Y' where CONFIG_KEY = 'IS_PARTIAL_SQUAREOFF_NIFTY';
update BRAHMA_V1_SIT_NA.CONFIG set CONFIG_VALUE = 'Y' where CONFIG_KEY = 'IS_MANUAL_OVERRIDE_NIFTY';

-- --------------------------------------  POSITIONS_TRACKING QUERY NIFTY  --------------------------------------------------------------
SELECT POSITION_TRACKING_GROUP_ID, POSITION_GROUP_ID, CREATED_DATE_TIME, CURRENT_UNDERLYING, ORDER_MANIFEST,ENTRY_PRICE, CURRENT_PRICE,
CURRENT_PRICE_PNL_PCT,NET_PNL_OVERALL, UNREALIZED_PNL,
REALIZED_PNL,  ENTRY_IV, CURRENT_IV, ENTRY_VIX, UNREALIZED_PNL_GROUP, REALIZED_PNL_GROUP, REALIZED_PNL_OVERALL, MONEYNESS, 
SYMBOL, EXPIRY_DATE, STRIKE_PRICE, INSTRUMENT_TYPE, TRANSACTION_TYPE, ORDER_TYPE, CONTRACT_TYPE, NUM_LOTS, LOT_SIZE, QUANTITY, 
IS_ACTIVE, IS_SQUARE_OFF, MARGIN_OVERALL, ENTRY_PRICE, EXIT_PRICE, CURRENT_PRICE, EXECUTION_PRICE, PARAMS,  
ENTRY_ATM_PUT_PRICE, ENTRY_ATM_CALL_PRICE, ENTRY_ATM_AVG_PRICE,  ENTRY_ATM_PRICE_DIFF, ENTRY_VIX, ENTRY_DELTA, ENTRY_NET_DELTA, 
ENTRY_NET_DELTA_OVERALL, CURRENT_DELTA, CURRENT_NET_DELTA, CURRENT_NET_DELTA_OVERALL, ENTRY_GAMMA, ENTRY_NET_GAMMA, CURRENT_GAMMA, 
CURRENT_NET_GAMMA, ENTRY_IV, ENTRY_IV_DIFF_PCT, CURRENT_IV, CURRENT_IV_DIFF_PCT, NET_DELTA_THRESHOLD, ENTRY_THETA, CURRENT_THETA, 
ENTRY_NET_THETA, CURRENT_NET_THETA, ENTRY_VEGA, CURRENT_VEGA, ENTRY_NET_VEGA, CURRENT_NET_VEGA, CONTRACT_EXPIRY_DATE, ENTRY_TIME_TO_EXPIRY,
CURRENT_TIME_TO_EXPIRY, ORDER_MANIFEST, TIME_VALUE_OPTIONS, ENTRY_UNDERLYING, CURRENT_UNDERLYING, EXPECTED_THETA_PNL_PENDING, 
CURRENT_THETA_PNL_PENDING, NET_PNL_THRESHOLD, CREATED_DATE_TIME, UPDATED_DATE_TIME
FROM BRAHMA_V1_SIT_NA.POSITIONS_TRACKING where symbol ='NIFTY' ORDER BY CREATED_DATE_TIME DESC;

update BRAHMA_V1_SIT_NA.CONFIG set CONFIG_VALUE = 'Y' where CONFIG_KEY = 'SYMBOL_ACTIVE_NIFTY';
update BRAHMA_V1_SIT_NA.CONFIG set CONFIG_VALUE = 'N' where CONFIG_KEY = 'ORDER_GENERATION_ACTIVE';
delete from BRAHMA_V1_SIT_NA.POSITION;
delete from BRAHMA_V1_SIT_NA.ORDERS;
delete from BRAHMA_V1_SIT_NA.POSITIONS_TRACKING;
delete from BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP;


-- SELECT QUERY POSITIONS TRACKING - QUICK VIEW
SELECT POSITION_TRACKING_GROUP_ID, CREATED_DATE_TIME, CURRENT_UNDERLYING, ORDER_MANIFEST, NET_PNL_OVERALL, ENTRY_PRICE, CURRENT_PRICE,
CURRENT_PRICE_PNL_PCT, REALIZED_PNL_OVERALL, UNREALIZED_PNL,
REALIZED_PNL, ENTRY_VIX, UNREALIZED_PNL_GROUP, REALIZED_PNL_GROUP,  MONEYNESS, 
SYMBOL, EXPIRY_DATE, STRIKE_PRICE, INSTRUMENT_TYPE, TRANSACTION_TYPE, ORDER_TYPE, CONTRACT_TYPE, NUM_LOTS, LOT_SIZE, QUANTITY, 
IS_ACTIVE, IS_SQUARE_OFF, MARGIN_OVERALL, PARAMS, CREATED_DATE_TIME, UPDATED_DATE_TIME
FROM BRAHMA_V1_SIT_NA.POSITIONS_TRACKING where symbol ='NIFTY' ORDER BY CREATED_DATE_TIME DESC;

-- delete from BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP where POSITION_GROUP_ID = 220125015328125;
-- delete from BRAHMA_V1_SIT_NA.POSITIONS_BACK_UP;

-- delete from BRAHMA_V1_SIT.POSITION where id in (29,30);

-- delete from BRAHMA_V1_SIT.TRACKER;
-- delete from BRAHMA_V1_SIT.CONFIG where CONFIG_KEY = 'TRADING_STOP_DAYS';
-- delete from BRAHMA_V1_SIT.CONFIG where CONFIG_KEY = 'TRADING_START_TIME';
-- delete from BRAHMA_V1_SIT.CONFIG where CONFIG_KEY = 'JOB_SCHEDULE_INTERVAL';

-- INSERT INTO BRAHMA_V1_SIT.OOPS_POSITIONS_AUDIT( SELECT * FROM BRAHMA_V1_SIT.OOPS_POSITIONS);
-- INSERT INTO BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP( SELECT * FROM BRAHMA_V1_SIT_NA.POSITIONS_TRACKING);
-- update BRAHMA_V1_SIT.CONFIG set CONFIG_VALUE = 'h165ebp3a6aubegs' where CONFIG_KEY = 'Z_API_KEY';
-- update BRAHMA_V1_SIT.CONFIG set CONFIG_VALUE = '5M6RK8ZiJioKHmXe6D60fuT6g3mwt5vI' where CONFIG_KEY = 'TOKEN';
-- update BRAHMA_V1_SIT.CONFIG set CONFIG_VALUE = 'Y' where CONFIG_KEY = 'IS_MANUAL_OVERRIDE_NIFTY';
-- update BRAHMA_V1_SIT.CONFIG set CONFIG_VALUE = 'Y' where CONFIG_KEY = 'IS_MANUAL_OVERRIDE_BANKNIFTY';

-- ######################################################## BRAHMA_V1_SIT.POSITIONS_TRACKING_BACK_UP [ BAK TABLES ]  ##################################################
SELECT POSITION_TRACKING_GROUP_ID, POSITION_GROUP_ID, CREATED_DATE_TIME, CURRENT_UNDERLYING, ORDER_MANIFEST,ENTRY_PRICE, CURRENT_PRICE,
CURRENT_PRICE_PNL_PCT,NET_PNL_OVERALL, UNREALIZED_PNL,
REALIZED_PNL,  ENTRY_IV, CURRENT_IV, ENTRY_VIX, UNREALIZED_PNL_GROUP, REALIZED_PNL_GROUP, REALIZED_PNL_OVERALL, MONEYNESS, 
SYMBOL, EXPIRY_DATE, STRIKE_PRICE, INSTRUMENT_TYPE, TRANSACTION_TYPE, ORDER_TYPE, CONTRACT_TYPE, NUM_LOTS, LOT_SIZE, QUANTITY, 
IS_ACTIVE, IS_SQUARE_OFF, MARGIN_OVERALL, ENTRY_PRICE, EXIT_PRICE, CURRENT_PRICE, EXECUTION_PRICE, PARAMS,  
ENTRY_ATM_PUT_PRICE, ENTRY_ATM_CALL_PRICE, ENTRY_ATM_AVG_PRICE,  ENTRY_ATM_PRICE_DIFF, ENTRY_VIX, ENTRY_DELTA, ENTRY_NET_DELTA, 
ENTRY_NET_DELTA_OVERALL, CURRENT_DELTA, CURRENT_NET_DELTA, CURRENT_NET_DELTA_OVERALL, ENTRY_GAMMA, ENTRY_NET_GAMMA, CURRENT_GAMMA, 
CURRENT_NET_GAMMA, ENTRY_IV, ENTRY_IV_DIFF_PCT, CURRENT_IV, CURRENT_IV_DIFF_PCT, NET_DELTA_THRESHOLD, ENTRY_THETA, CURRENT_THETA, 
ENTRY_NET_THETA, CURRENT_NET_THETA, ENTRY_VEGA, CURRENT_VEGA, ENTRY_NET_VEGA, CURRENT_NET_VEGA, CONTRACT_EXPIRY_DATE, ENTRY_TIME_TO_EXPIRY,
CURRENT_TIME_TO_EXPIRY, ORDER_MANIFEST, TIME_VALUE_OPTIONS, ENTRY_UNDERLYING, CURRENT_UNDERLYING, EXPECTED_THETA_PNL_PENDING, 
CURRENT_THETA_PNL_PENDING, NET_PNL_THRESHOLD, CREATED_DATE_TIME, UPDATED_DATE_TIME
FROM BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP where symbol ='BANKNIFTY'
-- and POSITION_TRACKING_GROUP_ID = 220123133532840
-- and POSITION_GROUP_ID = 220124035634266
ORDER BY CREATED_DATE_TIME DESC;

-- ANALYSIS DAILY EOD PNL --
-- ###################################################################################################################################
select POSITION_GROUP_ID, CREATED_DATE_TIME, UPDATED_DATE_TIME,ENTRY_UNDERLYING, CURRENT_UNDERLYING,
CONTRACT_EXPIRY_DATE,REALIZED_PNL_OVERALL, ENTRY_PRICE, EXIT_PRICE, ENTRY_VIX, ENTRY_ATM_AVG_PRICE, ENTRY_ATM_PUT_PRICE, ENTRY_ATM_CALL_PRICE,
ENTRY_ATM_PRICE_DIFF, SUBSTRING(CONTRACT_EXPIRY_DATE,3,3) AS MONTHNAME,
STRIKE_PRICE, INSTRUMENT_TYPE, TRANSACTION_TYPE,  SYMBOL, 
QUANTITY,  ENTRY_IV, CURRENT_IV, ENTRY_ATM_AVG_PRICE, ENTRY_UNDERLYING, CURRENT_UNDERLYING
FROM BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP
JOIN ( SELECT max(POSITION_TRACKING_GROUP_ID) POSITION_TRACKING_GROUP_ID FROM BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP
 where ORDER_MANIFEST like '%_SQUAREOFF%' and NET_PNL_OVERALL = REALIZED_PNL_OVERALL
GROUP BY POSITION_TRACKING_GROUP_ID
) last ON POSITIONS_TRACKING_BACK_UP.POSITION_TRACKING_GROUP_ID = last.POSITION_TRACKING_GROUP_ID
and  ORDER_MANIFEST like '%_SQUAREOFF%' and NET_PNL_OVERALL = REALIZED_PNL_OVERALL;
-- ##################################################################################################################################


select distinct(POSITION_GROUP_ID), CONTRACT_EXPIRY_DATE FROM BRAHMA_V1_SIT_NA.POSITIONS_TRACKING_BACK_UP order by POSITION_GROUP_ID desc;
-- #####################################################################################################################################

