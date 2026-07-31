-- =========================================================
-- Tableau Dashboard Views
-- Based on: CHICAGO_HOUSING_PROJECT.PUBLIC.FINAL_TABLE
-- =========================================================

USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};

-- =========================================================
-- 1) Property Crime Count Distribution
-- =========================================================
CREATE OR REPLACE VIEW vw_property_crime_distribution AS
SELECT
    ZIP_CODE AS zip_code,
    AVG(Property_Crime_Count) AS avg_property_crime_count
FROM final_table
GROUP BY ZIP_CODE
;

-- =========================================================
-- 2) Violent Crime Count Distribution
-- =========================================================
CREATE OR REPLACE VIEW vw_violent_crime_distribution AS
SELECT
    ZIP_CODE AS zip_code,
    AVG(Violent_Crime_Count) AS avg_violent_crime_count
FROM final_table
GROUP BY ZIP_CODE
;

-- =========================================================
-- 3) Investment Efficiency by Zone (Property)
-- =========================================================
CREATE OR REPLACE VIEW vw_investment_efficiency_property AS
SELECT
    ZIP_CODE AS zip_code,
    SUM(Property_Crime_Count) AS total_property_crime_count,
    SUM(ZHVI_Value) AS total_zhvi_value
FROM final_table
GROUP BY ZIP_CODE
;

-- =========================================================
-- 4) Crime Seasonality
-- =========================================================
CREATE OR REPLACE VIEW vw_crime_seasonality AS
SELECT
    DATE_PART('MONTH', Month_Date) AS month_num,
    AVG(Property_Crime_Count) AS avg_property_crime_count,
    AVG(ZHVI_Value) AS avg_zhvi_value,
    CASE
        WHEN DATE_PART('MONTH', Month_Date) BETWEEN 6 AND 8 THEN TRUE
        ELSE FALSE
    END AS peak_season
FROM final_table
GROUP BY DATE_PART('MONTH', Month_Date)
;

-- =========================================================
-- 5) Lag Effect - Property Crime
-- =========================================================
CREATE OR REPLACE VIEW vw_lag_effect_property_crime AS
WITH monthly_data AS (
    SELECT
        DATE_PART('MONTH', Month_Date) AS month_num,
        SUM(ZHVI_Value) AS total_zhvi_value,
        SUM(Property_Crime_Count) AS total_property_crime_count
    FROM final_table
    WHERE Month_Date >= '2017-01-01'::DATE
      AND Month_Date <= '2024-12-01'::DATE
    GROUP BY DATE_PART('MONTH', Month_Date)
)
SELECT
    month_num,
    total_property_crime_count,
    total_zhvi_value,
    (
        total_zhvi_value
        - LAG(total_zhvi_value) OVER (ORDER BY month_num)
    ) / NULLIF(
        ABS(LAG(total_zhvi_value) OVER (ORDER BY month_num)),
        0
    ) AS monthly_appreciation_rate
FROM monthly_data
;

-- =========================================================
-- 6) Crime Rate & Housing Appreciation - Base View
-- =========================================================
CREATE OR REPLACE VIEW vw_crime_rate_housing_appreciation_base AS
SELECT
    ZIP_CODE AS zip_code,
    DATE_PART('YEAR', Month_Date) AS year_num,
    DATE_PART('MONTH', Month_Date) AS month_num,
    SUM(Property_Crime_Count) AS total_property_crime_count,
    SUM(Population) AS total_population,
    SUM(ZHVI_Value) AS total_zhvi_value,
    SUM(Property_Crime_Count) / NULLIF(SUM(Population) / 1000.0, 0) AS property_crime_rate_per_1000
FROM final_table
WHERE NOT (
    ZIP_CODE = 60602
    AND (
        (DATE_PART('MONTH', Month_Date) = 7 AND DATE_PART('YEAR', Month_Date) IN (2021, 2022))
        OR
        (DATE_PART('MONTH', Month_Date) = 8 AND DATE_PART('YEAR', Month_Date) IN (2017, 2019))
    )
)
GROUP BY
    ZIP_CODE,
    DATE_PART('YEAR', Month_Date),
    DATE_PART('MONTH', Month_Date)
;

-- =========================================================
-- 7) Crime Rate & Housing Appreciation - Final View
-- =========================================================
CREATE OR REPLACE VIEW vw_crime_rate_housing_appreciation AS
SELECT
    zip_code,
    year_num,
    month_num,
    total_property_crime_count,
    total_population,
    total_zhvi_value,
    property_crime_rate_per_1000,
    (
        total_zhvi_value
        - LAG(total_zhvi_value) OVER (
            PARTITION BY zip_code
            ORDER BY year_num, month_num
        )
    ) / NULLIF(
        ABS(
            LAG(total_zhvi_value) OVER (
                PARTITION BY zip_code
                ORDER BY year_num, month_num
            )
        ),
        0
    ) AS monthly_appreciation_rate
FROM vw_crime_rate_housing_appreciation_base
;

-- =========================================================
-- 8) Crime Rate Slopes Comparison (Violent vs Property)
-- Translated from crime_rate_slopes.py
-- =========================================================
CREATE OR REPLACE VIEW vw_crime_slope_compare_tableau AS
WITH monthly_growth AS (
    SELECT
        ZIP_CODE,
        Month_Date,
        Property_Crime_Count,
        Violent_Crime_Count,
        Population,
        ZHVI_Value,
        -- Calculate housing growth percentage month-over-month per ZIP code
        (ZHVI_Value - LAG(ZHVI_Value) OVER (PARTITION BY ZIP_CODE ORDER BY Month_Date)) 
        / NULLIF(LAG(ZHVI_Value) OVER (PARTITION BY ZIP_CODE ORDER BY Month_Date), 0) * 100 AS housing_growth_pct,
        -- Calculate crime rates per 1000 people
        (Violent_Crime_Count / NULLIF(Population, 0)) * 1000 AS violent_rate,
        (Property_Crime_Count / NULLIF(Population, 0)) * 1000 AS property_rate
    FROM final_table
),
zip_aggregated AS (
    SELECT
        ZIP_CODE,
        AVG(violent_rate) AS avg_violent_rate,
        AVG(property_rate) AS avg_property_rate,
        AVG(housing_growth_pct) AS avg_housing_growth_pct,
        AVG(Population) AS avg_population
    FROM monthly_growth
    GROUP BY ZIP_CODE
),
unpivoted_rates AS (
    -- Violent Crime
    SELECT
        ZIP_CODE,
        'Violent Crime' AS crime_type,
        avg_violent_rate AS crime_rate,
        avg_housing_growth_pct AS housing_growth_pct,
        avg_population,
        'H1' AS hypothesis
    FROM zip_aggregated
    
    UNION ALL
    
    -- Property Crime
    SELECT
        ZIP_CODE,
        'Property Crime' AS crime_type,
        avg_property_rate AS crime_rate,
        avg_housing_growth_pct AS housing_growth_pct,
        avg_population,
        'H1' AS hypothesis
    FROM zip_aggregated
)
SELECT * FROM unpivoted_rates;
