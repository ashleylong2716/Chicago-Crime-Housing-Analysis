"""
Create dataset for comparing slopes:
Violent Crime vs Property Crime impact on housing growth
"""

import pandas as pd
import numpy as np
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# ── 1 Load data ─────────────────────────────────────────────
df = session.sql(
    'SELECT * FROM CHICAGO_HOUSING_PROJECT.PUBLIC."FINAL_TABLE"'
).to_pandas()


df["Month_Date"] = pd.to_datetime(df["Month_Date"])

# ── 2 Create crime rates ────────────────────────────────────
df["Violent_Rate"] = (df["Violent_Crime_Count"] / df["Population"]) * 1000
df["Property_Rate"] = (df["Property_Crime_Count"] / df["Population"]) * 1000

# ── 3 Housing growth ─────────────────────────────────────────
df = df.sort_values(["ZIP_Code", "Month_Date"])

df["ZHVI_Prev"] = df.groupby("ZIP_Code")["ZHVI_Value"].shift(1)

df["Housing_Growth_Pct"] = (
    (df["ZHVI_Value"] - df["ZHVI_Prev"]) / df["ZHVI_Prev"]
) * 100

# ── 4 Aggregate to ZIP level (average relationship) ─────────
zip_df = (
    df.groupby("ZIP_Code")
    .agg({
        "Violent_Rate": "mean",
        "Property_Rate": "mean",
        "Housing_Growth_Pct": "mean",
        "Population": "mean"
    })
    .reset_index()
)

# ── 5 Convert to long format for Tableau ─────────────────────
violent = zip_df[[
    "ZIP_Code",
    "Violent_Rate",
    "Housing_Growth_Pct",
    "Population"
]].copy()

violent["CRIME_TYPE"] = "Violent Crime"
violent = violent.rename(columns={"Violent_Rate": "CRIME_RATE"})

property_df = zip_df[[
    "ZIP_Code",
    "Property_Rate",
    "Housing_Growth_Pct",
    "Population"
]].copy()

property_df["CRIME_TYPE"] = "Property Crime"
property_df = property_df.rename(columns={"Property_Rate": "CRIME_RATE"})

scatter_compare = pd.concat([violent, property_df])

scatter_compare = scatter_compare.rename(columns={
    "ZIP_Code": "ZIP_CODE",
    "Housing_Growth_Pct": "HOUSING_GROWTH_PCT",
    "Population": "AVG_POPULATION"
})

scatter_compare["HYPOTHESIS"] = "H1"

scatter_compare = scatter_compare[[
    "ZIP_CODE",
    "CRIME_TYPE",
    "CRIME_RATE",
    "HOUSING_GROWTH_PCT",
    "AVG_POPULATION",
    "HYPOTHESIS"
]]

print(scatter_compare.head())

# ── 6 Write to Snowflake ─────────────────────────────────────
scatter_sp = session.create_dataframe(scatter_compare)

scatter_sp.write.mode("overwrite").save_as_table(
    "CHICAGO_HOUSING_PROJECT.PUBLIC.CRIME_SLOPE_COMPARE_TABLEAU"
)

print("Saved table: CRIME_SLOPE_COMPARE_TABLEAU")
