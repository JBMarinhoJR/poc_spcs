-- =============================================================================
-- Pipeline Service — SPCS Deployment
-- Deploys the SQL CI/CD runner as an internal SPCS service.
-- Run Section A as ACCOUNTADMIN, then Section B as SPCS_PIPELINE_ROLE.
-- =============================================================================


-- =============================================================================
-- SECTION A — Privileges (run as ACCOUNTADMIN)
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE POC_SPCS_DB;
USE SCHEMA POC_SPCS_SCHEMA;

-- Create dedicated role for the pipeline service
CREATE ROLE IF NOT EXISTS SPCS_PIPELINE_ROLE;
GRANT ROLE SPCS_PIPELINE_ROLE TO USER JOHNPOC022026;

-- Compute pool + service endpoint privileges
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE SPCS_PIPELINE_ROLE;
GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE SPCS_PIPELINE_ROLE;

-- Database/schema access
GRANT USAGE ON DATABASE POC_SPCS_DB    TO ROLE SPCS_PIPELINE_ROLE;
GRANT USAGE ON SCHEMA POC_SPCS_SCHEMA  TO ROLE SPCS_PIPELINE_ROLE;
GRANT CREATE SERVICE ON SCHEMA POC_SPCS_SCHEMA TO ROLE SPCS_PIPELINE_ROLE;

-- Image repository read access
GRANT READ ON IMAGE REPOSITORY POC_REPO TO ROLE SPCS_PIPELINE_ROLE;

-- Warehouse usage (for EXPLAIN + SQL execution)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SPCS_PIPELINE_ROLE;

-- Audit table write access (created by setup_audit_table.sql)
GRANT INSERT, SELECT ON TABLE POC_SPCS_DB.POC_SPCS_SCHEMA.PIPELINE_AUDIT_LOG
    TO ROLE SPCS_PIPELINE_ROLE;

-- Allow pipeline role to create/alter objects in the schema (for running migrations)
GRANT ALL PRIVILEGES ON SCHEMA POC_SPCS_SCHEMA TO ROLE SPCS_PIPELINE_ROLE;


-- =============================================================================
-- SECTION B — Compute pool + Service (run as SPCS_PIPELINE_ROLE)
-- =============================================================================

USE ROLE SPCS_PIPELINE_ROLE;
USE DATABASE POC_SPCS_DB;
USE SCHEMA POC_SPCS_SCHEMA;

-- Compute pool — minimal footprint, internal workload only
CREATE COMPUTE POOL IF NOT EXISTS POC_PIPELINE_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = FALSE
    AUTO_SUSPEND_SECS = 3600;

-- Wait for compute pool to be IDLE before creating service
-- (run DESCRIBE COMPUTE POOL POC_PIPELINE_POOL until state = IDLE)

-- Pipeline runner service — internal endpoint (not public)
DROP SERVICE IF EXISTS POC_PIPELINE_SERVICE;
CREATE SERVICE POC_PIPELINE_SERVICE
    IN COMPUTE POOL POC_PIPELINE_POOL
    QUERY_WAREHOUSE = COMPUTE_WH
    MIN_INSTANCES = 1
    MAX_INSTANCES = 1
    FROM SPECIFICATION $$
spec:
  containers:
  - name: pipeline
    image: /poc_spcs_db/poc_spcs_schema/poc_repo/ds-repo-docker-custom-image:pipeline
    env:
      SNOWFLAKE_ACCOUNT: "MLWWZGB-YR87884"
      SNOWFLAKE_WAREHOUSE: "COMPUTE_WH"
      SNOWFLAKE_DB: "POC_SPCS_DB"
      SNOWFLAKE_SCHEMA: "POC_SPCS_SCHEMA"
    readinessProbe:
      port: 8080
      path: /health
  endpoints:
  - name: api
    port: 8080
    public: false
$$ ;

-- =============================================================================
-- Verify deployment
-- =============================================================================

SHOW SERVICES LIKE 'POC_PIPELINE_SERVICE';
SHOW SERVICE CONTAINERS IN SERVICE POC_PIPELINE_SERVICE;
SHOW ENDPOINTS IN SERVICE POC_PIPELINE_SERVICE;

-- Get the internal DNS name for use in GitHub Actions
-- Format: <service>.<schema>.<db>.snowflakecomputing.internal
SELECT SYSTEM$GET_SERVICE_DNS_DOMAIN('POC_SPCS_DB', 'POC_SPCS_SCHEMA', 'POC_PIPELINE_SERVICE');
