-- =============================================================================
-- Pipeline Audit Log Table
-- Run once as ACCOUNTADMIN or role with CREATE TABLE privilege on the schema.
-- =============================================================================

USE DATABASE POC_SPCS_DB;
USE SCHEMA POC_SPCS_SCHEMA;

CREATE TABLE IF NOT EXISTS PIPELINE_AUDIT_LOG (
    id             NUMBER AUTOINCREMENT PRIMARY KEY,
    migration_file VARCHAR(255)   NOT NULL,
    status         VARCHAR(20)    NOT NULL,  -- VALIDATED | DEPLOYED | FAILED
    executed_by    VARCHAR(100),
    executed_at    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    sql_content    TEXT,
    error_message  TEXT,
    git_sha        VARCHAR(40)
);

-- Grant read access to the viewer role so the Streamlit app can query audit logs
GRANT SELECT ON TABLE PIPELINE_AUDIT_LOG TO ROLE SPCS_STREAMLIT_VIEWER_ROLE;
-- Grant write access to the pipeline service role
GRANT INSERT, SELECT ON TABLE PIPELINE_AUDIT_LOG TO ROLE SPCS_PIPELINE_ROLE;
