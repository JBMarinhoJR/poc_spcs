-- Deploy Streamlit on Snowpark Container Services (SPCS)
-- Prerequisite image tag expected:
--   /poc_spcs_db/poc_spcs_schema/poc_repo/ds-repo-docker-custom-image:streamlit-ui
--
-- Run section A once as ACCOUNTADMIN (or privileged admin).
-- Run section B as the service owner role.

-- ============================================================
-- A) One-time role/privilege setup
-- ============================================================
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS SPCS_STREAMLIT_APP_ROLE;
CREATE ROLE IF NOT EXISTS SPCS_STREAMLIT_VIEWER_ROLE;

-- Optional: attach roles to your user (adjust username if needed)
GRANT ROLE SPCS_STREAMLIT_APP_ROLE TO USER JOHNPOC022026;
GRANT ROLE SPCS_STREAMLIT_VIEWER_ROLE TO USER JOHNPOC022026;

-- Required privileges for creating a public SPCS service
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE SPCS_STREAMLIT_APP_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE SPCS_STREAMLIT_APP_ROLE;

GRANT USAGE ON DATABASE POC_SPCS_DB TO ROLE SPCS_STREAMLIT_APP_ROLE;
GRANT USAGE ON SCHEMA POC_SPCS_DB.POC_SPCS_SCHEMA TO ROLE SPCS_STREAMLIT_APP_ROLE;
GRANT CREATE SERVICE ON SCHEMA POC_SPCS_DB.POC_SPCS_SCHEMA TO ROLE SPCS_STREAMLIT_APP_ROLE;
GRANT READ ON IMAGE REPOSITORY POC_SPCS_DB.POC_SPCS_SCHEMA.POC_REPO TO ROLE SPCS_STREAMLIT_APP_ROLE;

-- Viewer role needs basic object visibility
GRANT USAGE ON DATABASE POC_SPCS_DB TO ROLE SPCS_STREAMLIT_VIEWER_ROLE;
GRANT USAGE ON SCHEMA POC_SPCS_DB.POC_SPCS_SCHEMA TO ROLE SPCS_STREAMLIT_VIEWER_ROLE;

-- ============================================================
-- B) Create compute pool + service
-- ============================================================
USE ROLE SPCS_STREAMLIT_APP_ROLE;
USE DATABASE POC_SPCS_DB;
USE SCHEMA POC_SPCS_SCHEMA;

CREATE COMPUTE POOL IF NOT EXISTS POC_STREAMLIT_POOL
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = FALSE
  AUTO_SUSPEND_SECS = 3600;

CREATE OR REPLACE SERVICE POC_STREAMLIT_SERVICE
  IN COMPUTE POOL POC_STREAMLIT_POOL
  MIN_INSTANCES = 1
  MAX_INSTANCES = 1
  FROM SPECIFICATION $$
spec:
  containers:
  - name: streamlit
    image: /poc_spcs_db/poc_spcs_schema/poc_repo/ds-repo-docker-custom-image:streamlit-ui
    env:
      STREAMLIT_SERVER_PORT: "8501"
      STREAMLIT_SERVER_ADDRESS: "0.0.0.0"
      STREAMLIT_SERVER_HEADLESS: "true"
      SNOWFLAKE_ACCOUNT: "MLWWZGB-YR87884"
      SNOWFLAKE_ROLE: "SPCS_DEMO_RLS_ROLE"
      SNOWFLAKE_WAREHOUSE: "COMPUTE_WH"
      SNOWFLAKE_DB: "POC_SPCS_DB"
      SNOWFLAKE_SCHEMA: "POC_SPCS_SCHEMA"
      APP_ORDERS_TABLE: "POC_SPCS_DB.POC_SPCS_SCHEMA.APP_DEMO_ORDERS"
      SPCS_SERVICE_DB: "POC_SPCS_DB"
      SPCS_SERVICE_SCHEMA: "POC_SPCS_SCHEMA"
      SPCS_SERVICE_NAME: "POC_STREAMLIT_SERVICE"
    readinessProbe:
      port: 8501
      path: /_stcore/health
  endpoints:
  - name: ui
    port: 8501
    public: true
serviceRoles:
- name: ui_role
  endpoints:
  - ui
$$;

-- Allow viewer role to access the service endpoint
GRANT SERVICE ROLE POC_STREAMLIT_SERVICE!ui_role TO ROLE SPCS_STREAMLIT_VIEWER_ROLE;
