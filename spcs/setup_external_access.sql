-- Optional setup (not available on trial accounts).
-- Enable outbound egress from SPCS service to:
-- 1) Snowflake account hostname (for username/password connector login)
-- 2) CoinGecko API (public API panel in Streamlit app)
--
-- Execute as ACCOUNTADMIN.

USE ROLE ACCOUNTADMIN;
USE DATABASE POC_SPCS_DB;
USE SCHEMA POC_SPCS_SCHEMA;

CREATE OR REPLACE NETWORK RULE SPCS_APP_EGRESS_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'mlwwzgb-yr87884.snowflakecomputing.com:443',
    'api.coingecko.com:443'
  );

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION SPCS_APP_EAI
  ALLOWED_NETWORK_RULES = (POC_SPCS_DB.POC_SPCS_SCHEMA.SPCS_APP_EGRESS_RULE)
  ENABLED = TRUE;

-- Service owner role needs integration usage
GRANT USAGE ON INTEGRATION SPCS_APP_EAI TO ROLE SPCS_STREAMLIT_APP_ROLE;
