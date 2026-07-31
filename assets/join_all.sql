-- ============================================================
-- Create FINAL_TABLE
-- Using ZIP_Code + Year to JOIN
-- If the year has population data, it will be filled into each month of that year
-- If the year has no population data, it will be automatically excluded by INNER JOIN
-- ============================================================

CREATE OR REPLACE TABLE final_table AS
WITH analytical_window AS (
    SELECT
        ZIP_CODE,
        Month_Date,
        Year,
        Property_Crime_Count,
        Violent_Crime_Count,
        ZHVI_Value
    FROM CHICAGO_ANALYTICAL_TABLE
    WHERE Year BETWEEN 2014 AND 2024
      AND ZHVI_Value           IS NOT NULL
      AND Violent_Crime_Count  IS NOT NULL
      AND Property_Crime_Count IS NOT NULL
),
population_window AS (
    SELECT
        ZIP_CODE,
        Year,
        Population
    FROM CHICAGO_POPULATION
    WHERE Year BETWEEN 2014 AND 2024
      AND Population IS NOT NULL
)
SELECT
    a.ZIP_CODE,
    a.Month_Date,
    a.Year,
    a.Property_Crime_Count,
    a.Violent_Crime_Count,
    a.ZHVI_Value,
    p.Population
FROM analytical_window a
INNER JOIN population_window p
    ON  a.ZIP_CODE = p.ZIP_CODE
    AND a.Year     = p.Year;



-- Verify
SELECT *
FROM final_table;
