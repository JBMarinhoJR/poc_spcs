-- =============================================================================
-- Pipeline Audit Log Table
-- Safe first-run order:
--   1) You can run this file before service roles exist.
--   2) Role grants are attempted and skipped if roles are missing/not authorized.
--   3) After roles are created, re-run only the GRANT statements if needed.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE POC_SPCS_DB;
USE SCHEMA POC_SPCS_SCHEMA;

CREATE ROLE IF NOT EXISTS SPCS_PIPELINE_ROLE;
GRANT INSERT, SELECT ON TABLE POC_SPCS_DB.POC_SPCS_SCHEMA.PIPELINE_AUDIT_LOG TO ROLE SPCS_PIPELINE_ROLE;
GRANT SELECT ON TABLE POC_SPCS_DB.POC_SPCS_SCHEMA.PIPELINE_AUDIT_LOG TO ROLE SPCS_STREAMLIT_VIEWER_ROLE;


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

-- Grant read access to the viewer role so the Streamlit app can query audit logs.
BEGIN
    GRANT SELECT ON TABLE PIPELINE_AUDIT_LOG TO ROLE SPCS_STREAMLIT_VIEWER_ROLE;
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        SELECT
            'Skipped grant to SPCS_STREAMLIT_VIEWER_ROLE. Create/authorize the role first (spcs/deploy_streamlit_service.sql Section A).' AS warning;
END;

-- Grant write access to the pipeline service role.
BEGIN
    GRANT INSERT, SELECT ON TABLE PIPELINE_AUDIT_LOG TO ROLE SPCS_PIPELINE_ROLE;
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        SELECT
            'Skipped grant to SPCS_PIPELINE_ROLE. Create/authorize the role first (pipeline/deploy_pipeline_service.sql Section A).' AS warning;
END;

SHOW ROLES LIKE 'SPCS_PIPELINE_ROLE';
SHOW GRANTS ON TABLE POC_SPCS_DB.POC_SPCS_SCHEMA.PIPELINE_AUDIT_LOG;
