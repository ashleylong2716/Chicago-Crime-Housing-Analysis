/* ============================================================
   Set context
   ============================================================ */
USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};
USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};

/* ============================================================
   File format for CSV files with header row
   Required for MATCH_BY_COLUMN_NAME
   ============================================================ */
CREATE OR REPLACE FILE FORMAT chicago_csv_ff
  TYPE = CSV
  FIELD_DELIMITER = ','
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('', 'NULL', 'null');

/* ============================================================
   Optional: inspect staged files
   ============================================================ */
LIST @{SNOWFLAKE_STAGE};
LIST @{SNOWFLAKE_STAGE}/pipeline.csv/;

/* ============================================================
   Create CHICAGO_ANALYTICAL_TABLE by inferring schema
   ============================================================ */
CREATE OR REPLACE TABLE CHICAGO_ANALYTICAL_TABLE
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@{SNOWFLAKE_STAGE}/pipeline.csv/',
      FILE_FORMAT => 'chicago_csv_ff',
      IGNORE_CASE => TRUE
    )
  )
);

/* ============================================================
   Load analytical CSV files
   ============================================================ */
COPY INTO CHICAGO_ANALYTICAL_TABLE
FROM @{SNOWFLAKE_STAGE}/pipeline.csv/
FILE_FORMAT = (FORMAT_NAME = 'chicago_csv_ff')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*[.]csv'
ON_ERROR = 'SKIP_FILE';

/* ============================================================
   Verify analytical table
   ============================================================ */
SELECT *
FROM CHICAGO_ANALYTICAL_TABLE
LIMIT 20;

/* ============================================================
   Create CHICAGO_POPULATION by inferring schema
   ============================================================ */
CREATE OR REPLACE TABLE CHICAGO_POPULATION
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@{SNOWFLAKE_STAGE}/historical_zip_population_10yrs.csv',
      FILE_FORMAT => 'chicago_csv_ff',
      IGNORE_CASE => TRUE
    )
  )
);

/* ============================================================
   Load population CSV file
   ============================================================ */
COPY INTO CHICAGO_POPULATION
FROM @{SNOWFLAKE_STAGE}/historical_zip_population_10yrs.csv
FILE_FORMAT = (FORMAT_NAME = 'chicago_csv_ff')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';

/* ============================================================
   Verify population table
   ============================================================ */
SELECT *
FROM CHICAGO_POPULATION
LIMIT 20;

/* ============================================================
   Inspect created tables
   ============================================================ */
SHOW TABLES;

DESCRIBE TABLE CHICAGO_ANALYTICAL_TABLE;
DESCRIBE TABLE CHICAGO_POPULATION;