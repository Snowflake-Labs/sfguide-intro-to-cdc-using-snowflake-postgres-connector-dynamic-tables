USE ROLE securityadmin;

-- create churn_data_scientist
CREATE OR REPLACE ROLE cdc_data_scientist;

USE ROLE accountadmin;

/*---------------------------*/
-- Create our Database
/*---------------------------*/
CREATE OR REPLACE DATABASE cdc_prod;

/*---------------------------*/
-- Create our Schema
/*---------------------------*/
CREATE OR REPLACE SCHEMA cdc_prod.analytics;

/*---------------------------*/
-- Create our Warehouse
/*---------------------------*/

-- data science warehouse
CREATE OR REPLACE WAREHOUSE cdc_ds_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data science warehouse for cdc';

-- Use our Warehouse
USE WAREHOUSE cdc_ds_wh;

-- grant cdc_ds_wh privileges to cdc_data_scientist role
GRANT USAGE ON WAREHOUSE cdc_ds_wh TO ROLE cdc_data_scientist;
GRANT OPERATE ON WAREHOUSE cdc_ds_wh TO ROLE cdc_data_scientist;
GRANT MONITOR ON WAREHOUSE cdc_ds_wh TO ROLE cdc_data_scientist;
GRANT MODIFY ON WAREHOUSE cdc_ds_wh TO ROLE cdc_data_scientist;

-- grant cdc_ds_wh database privileges
GRANT ALL ON DATABASE cdc_prod TO ROLE cdc_data_scientist;

GRANT ALL ON SCHEMA cdc_prod.analytics TO ROLE cdc_data_scientist;
GRANT CREATE STAGE ON SCHEMA cdc_prod.analytics TO ROLE cdc_data_scientist;

GRANT ALL ON ALL STAGES IN SCHEMA cdc_prod.analytics TO ROLE cdc_data_scientist;

-- set my_user_var variable to equal the logged-in user
SET my_user_var = (SELECT  '"' || CURRENT_USER() || '"' );

-- grant the logged in user the cdc_data_scientist role
GRANT ROLE cdc_data_scientist TO USER identifier($my_user_var);

USE ROLE cdc_data_scientist;

/*---------------------------*/
-- sql completion note
/*---------------------------*/
SELECT 'cdc sql is now complete' AS note;