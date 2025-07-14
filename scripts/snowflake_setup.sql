USE ROLE accountadmin;
CREATE OR REPLACE DATABASE cdc_prod;
CREATE OR REPLACE SCHEMA cdc_prod.analytics;

CREATE OR REPLACE WAREHOUSE cdc_ds_wh
   WAREHOUSE_SIZE = 'xsmall'
   WAREHOUSE_TYPE = 'standard'
   AUTO_SUSPEND = 60
   AUTO_RESUME = TRUE
   INITIALLY_SUSPENDED = TRUE
   COMMENT = 'data science warehouse for cdc';

GRANT USAGE ON WAREHOUSE cdc_ds_wh TO ROLE accountadmin;
GRANT OPERATE ON WAREHOUSE cdc_ds_wh TO ROLE accountadmin;
USE WAREHOUSE cdc_ds_wh;
SELECT 'cdc sql is now complete' AS note;