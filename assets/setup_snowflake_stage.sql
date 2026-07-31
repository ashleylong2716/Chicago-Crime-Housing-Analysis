/* ============================================================
   Snowflake External Stage Setup (GCS)
   ============================================================
   Run this script in the Snowflake UI (Snowsight) using ACCOUNTADMIN.
   
   STEP 1: Copy values from your .env into the CONFIGURATION block below.
   Mapping:
   - GCP_BUCKET_URL        -> bucket_url
   - SNOWFLAKE_INTEGRATION -> integration_name
   - SNOWFLAKE_DATABASE    -> database_name
   - SNOWFLAKE_SCHEMA      -> schema_name
   - SNOWFLAKE_STAGE       -> stage_name
   - SNOWFLAKE_ROLE        -> pipeline_role
   STEP 2: Highlight the entire script and run it.
*/

-- ==========================================
-- 1. CONFIGURATION (Set your variables here)
-- ==========================================
SET bucket_url       = 'gcs://chicago_crime_data_pipeline/';
SET integration_name = 'gcs_crime_integration';
SET database_name    = 'CHICAGO_HOUSING_PROJECT';
SET schema_name      = 'PUBLIC';
SET stage_name       = 'gcp_chicago_stage';
SET pipeline_role    = 'SYSADMIN';


-- ==========================================
-- 2. AUTOMATED SETUP (Do not change below)
-- ==========================================

-- A. Create a Storage Integration
CREATE STORAGE INTEGRATION IF NOT EXISTS IDENTIFIER($integration_name)
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ($bucket_url);

-- B. Retrieve the Service Account created by Snowflake
DESC STORAGE INTEGRATION IDENTIFIER($integration_name);

/* == GCP IAM SETUP INSTRUCTIONS ==
   Before proceeding, you MUST grant the Service Account access to GCS:
   1. Look at the results of the DESC command above.
   2. Copy the value of "STORAGE_GCP_SERVICE_ACCOUNT".
   3. Go to Google Cloud Console -> IAM & Admin -> IAM.
   4. Click "Grant Access", paste the account, and assign "Storage Object Viewer".
*/

-- C. Create the Database and Schema
CREATE DATABASE IF NOT EXISTS IDENTIFIER($database_name);
USE DATABASE IDENTIFIER($database_name);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($schema_name);
USE SCHEMA IDENTIFIER($schema_name);

-- D. Create the External Stage (Using Dynamic SQL to bypass parameter limitations)
SET create_stage_sql = 
  'CREATE STAGE IF NOT EXISTS ' || $stage_name || 
  ' URL = ''' || $bucket_url || '''' || 
  ' STORAGE_INTEGRATION = ' || $integration_name;
  
EXECUTE IMMEDIATE $create_stage_sql;

-- E. Grant privileges to the pipeline role
GRANT USAGE ON INTEGRATION IDENTIFIER($integration_name) TO ROLE IDENTIFIER($pipeline_role);
GRANT USAGE ON DATABASE IDENTIFIER($database_name) TO ROLE IDENTIFIER($pipeline_role);
GRANT USAGE ON SCHEMA IDENTIFIER($schema_name) TO ROLE IDENTIFIER($pipeline_role);
GRANT USAGE ON STAGE IDENTIFIER($stage_name) TO ROLE IDENTIFIER($pipeline_role);

/* F. Test the Stage
   If GCP permissions and integration are correct, this will list the files in your bucket.
*/
SET list_sql = 'LIST @' || $stage_name;
EXECUTE IMMEDIATE $list_sql;
