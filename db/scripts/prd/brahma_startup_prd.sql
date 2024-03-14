
-- PRD 2022 ENV
-- -------------------- CREATE DB ---------------------------------------

CREATE DATABASE BRAHMA_V1_PRD;
CREATE DATABASE BRAHMA_V1_PRD_NA;

-- --------------------- GRANT DB ----------------------------------------
-- GRANT ALL ON BRICS1_V1_DEV.* TO 'devFinnovesh'@'%' IDENTIFIED BY '6aGkGtfNnGz294m';
GRANT ALL PRIVILEGES ON BRAHMA_V1_PRD.* TO 'dbprduser'@'%' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON BRAHMA_V1_PRD_NA.* TO 'dbprduser'@'%' WITH GRANT OPTION;


-- ------------------- SHOW DB ------------------------------------------
SHOW DATABASES;

-- ------------------- DROP DB --------------------------------------------
DROP DATABASE BRAHMA_V1_PRD;
DROP DATABASE BRAHMA_V1_PRD_NA;