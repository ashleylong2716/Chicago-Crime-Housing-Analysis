"""
Public Safety-Housing Elasticity Analytics Pipeline
PySpark ETL Script with Apache Sedona Integration
"""

import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, trunc, year, when, expr,
    count, round, broadcast, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

TARGET_START_YEAR = 2014
TARGET_END_YEAR = 2024

# ==========================================
# 1. Automatic Environment Fix
# ==========================================
def install_packages():
    print(">>> Installing Python Dependencies...")
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        "apache-sedona==1.5.1",
        "snowflake-connector-python[pandas]"
    ])

try:
    from sedona.spark import *
except ImportError:
    install_packages()
    from sedona.spark import *

# ==========================================
# 2. Spark and Sedona Initialization
# ==========================================
def initialize_spark():
    spark = (
        SparkSession.builder
        .appName("Chicago_Safety_Housing_ETL")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "128")
        .getOrCreate()
    )
    SedonaContext.create(spark)
    return spark

# ==========================================
# 3. Data Cleaning Module
# ==========================================
def clean_boundary_data(spark, boundary_path):
    print(">>> Processing Boundary Data...")
    raw_df = spark.read.csv(boundary_path, header=True, inferSchema=False)

    potential_cols = [c for c in raw_df.columns if "geom" in c.lower() or "shape" in c.lower()]
    geom_col = potential_cols[0] if potential_cols else "the_geom"

    clean_df = raw_df.dropna(subset=[geom_col, "ZIP"])
    clean_df = clean_df.withColumn("ZIP_Code", col("ZIP").cast(StringType()))
    clean_df = clean_df.withColumn("geometry", expr(f"ST_GeomFromWKT({geom_col})"))
    clean_df = clean_df.filter(expr("ST_IsValid(geometry)"))

    return clean_df.select("ZIP_Code", "geometry")

def clean_crime_data(spark, crime_path):
   print(">>> Processing Crime Data...")

   # 1. read head
   raw_df = spark.read.csv(crime_path, header=True)

   # 2. know columns
   df = raw_df.select(
       col("ID"),
       col("Date"),
       col("Primary Type"),
       col("Latitude").cast(DoubleType()),
       col("Longitude").cast(DoubleType())
   )

   # 3. remove null and duplicate
   df = df.dropDuplicates(["ID"]).dropna(subset=["Latitude", "Longitude", "Date", "Primary Type"])

   # 4. Chicago lat/lon range filter
   df = df.filter(
       (col("Latitude") >= 41.5) & (col("Latitude") <= 42.1) &
       (col("Longitude") >= -88.0) & (col("Longitude") <= -87.5)
   )

   # 5. generate Sedona spatial point
   df = df.withColumn("point_geom", expr("ST_Point(Longitude, Latitude)"))
   
   # 6. time parsing (with double insurance: try 12-hour format with AM/PM first, then 24-hour format)
   df = df.withColumn("Parsed_Date", to_timestamp(col("Date"), "MM/dd/yyyy hh:mm:ss a"))
   df = df.withColumn(
       "Parsed_Date",
       when(col("Parsed_Date").isNotNull(), col("Parsed_Date"))
       .otherwise(to_timestamp(col("Date"), "MM/dd/yyyy HH:mm:ss"))
   ).dropna(subset=["Parsed_Date"])

   df = df.withColumn("Month_Date", trunc(col("Parsed_Date"), "month"))
   df = df.withColumn("Year", year(col("Month_Date")))
   df = df.filter(
       (col("Year") >= TARGET_START_YEAR) & (col("Year") <= TARGET_END_YEAR)
   )

   # 7. violent crime classification
   violent_types = [
       "BATTERY", "ASSAULT", "HOMICIDE", "ROBBERY",
       "CRIM SEXUAL ASSAULT", "KIDNAPPING", "SEX OFFENSE"
   ]

   df = df.withColumn(
       "Crime_Category",
       when(col("Primary Type").isin(violent_types), "Violent").otherwise("Property")
   )

   return df.select("ID", "Month_Date", "Year", "Crime_Category", "point_geom")

def clean_zillow_data(spark, zillow_path):
    print(">>> Processing Zillow ZHVI Data...")
    raw_df = spark.read.csv(zillow_path, header=True, inferSchema=False)

    df = raw_df.filter((col("State") == "IL") & (col("City") == "Chicago"))
    df = df.withColumn("ZIP_Code", col("RegionName").cast(StringType()))

    date_cols = [c for c in df.columns if "/" in c or "-" in c or c.startswith("20")]
    if not date_cols:
        raise ValueError("Failed to identify date columns from Zillow data, please check data Schema.")

    stack_items = ", ".join([f"'{c}', `{c}`" for c in date_cols])
    stack_expr = f"stack({len(date_cols)}, {stack_items}) as (Raw_Date, ZHVI_Value_Str)"

    melted_df = df.select("ZIP_Code", expr(stack_expr))
    melted_df = melted_df.withColumn("ZHVI_Value", col("ZHVI_Value_Str").cast(DoubleType()))
    melted_df = melted_df.filter((col("ZHVI_Value").isNotNull()) & (col("ZHVI_Value") > 0))

    melted_df = melted_df.withColumn(
        "Parsed_Date",
        when(col("Raw_Date").contains("/"), to_timestamp(col("Raw_Date"), "MM/dd/yyyy"))
        .otherwise(to_timestamp(col("Raw_Date"), "yyyy-MM-dd"))
    )

    melted_df = melted_df.withColumn("Month_Date", trunc(col("Parsed_Date"), "month"))
    melted_df = melted_df.filter(
        (year(col("Month_Date")) >= TARGET_START_YEAR)
        & (year(col("Month_Date")) <= TARGET_END_YEAR)
    )

    return melted_df.select("ZIP_Code", "Month_Date", "ZHVI_Value")

# ==========================================
# 4. Spatial Calculation and Merging
# ==========================================
def perform_spatial_join_and_aggregate(spark, crime_df, boundary_df):
    print(">>> Executing Spatial Join...")

    crime_df.createOrReplaceTempView("crimes")
    broadcast(boundary_df).createOrReplaceTempView("boundaries")

    joined_sql = """
        SELECT /*+ BROADCAST(b) */
            c.ID, c.Month_Date, c.Year, c.Crime_Category, b.ZIP_Code
        FROM crimes c
        JOIN boundaries b
        ON ST_Intersects(c.point_geom, b.geometry)
    """

    joined_df = spark.sql(joined_sql)

    print(">>> Aggregating Crime Metrics...")
    agg_df = joined_df.groupBy("ZIP_Code", "Month_Date", "Year").agg(
        spark_sum(when(col("Crime_Category") == "Violent", 1).otherwise(0)).alias("Violent_Crime_Count"),
        spark_sum(when(col("Crime_Category") == "Property", 1).otherwise(0)).alias("Property_Crime_Count")
    )

    return agg_df

def merge_master_dataset(crime_agg_df, zillow_df, population_df=None):
    print(">>> Merging Master Dataset...")

    master_df = crime_agg_df.join(
        zillow_df,
        on=["ZIP_Code", "Month_Date"],
        how="inner"
    )

    if population_df is not None:
        master_df = master_df.join(population_df, on=["ZIP_Code", "Year"], how="left")

        master_df = (
            master_df.withColumn(
                "Violent_Crime_Density_per_1k",
                when(
                    (col("Population").isNotNull()) & (col("Population") > 0),
                    round((col("Violent_Crime_Count") / col("Population")) * 1000, 4)
                ).otherwise(None)
            )
            .withColumn(
                "Property_Crime_Density_per_1k",
                when(
                    (col("Population").isNotNull()) & (col("Population") > 0),
                    round((col("Property_Crime_Count") / col("Population")) * 1000, 4)
                ).otherwise(None)
            )
        )

    return master_df


# ==========================================
# 5. Main Program Entry (includes Debug Monitoring)
# ==========================================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark_job.py <gs_bucket_name> [crime_object] [zillow_object] [boundary_object]")
        sys.exit(1)

    bucket_name = sys.argv[1]
    crime_object = sys.argv[2] if len(sys.argv) > 2 else "chicago_crime.csv"
    zillow_object = sys.argv[3] if len(sys.argv) > 3 else "chicago_zillow_home_values.csv"
    boundary_object = sys.argv[4] if len(sys.argv) > 4 else "chicago_zip_boundaries.csv"
    
    CRIME_PATH = f"gs://{bucket_name}/{crime_object}"
    ZILLOW_PATH = f"gs://{bucket_name}/{zillow_object}"
    BOUNDARY_PATH = f"gs://{bucket_name}/{boundary_object}"

    OUTPUT_CSV_PATH = f"gs://{bucket_name}/pipeline.csv"

    spark = initialize_spark()

    try:
        # 1. Check basic data cleaning
        boundary_df = clean_boundary_data(spark, BOUNDARY_PATH)
        boundary_count = boundary_df.count()
        print(f"✅ [Debug] Boundary Data Row Count: {boundary_count}")
        
        crime_df = clean_crime_data(spark, CRIME_PATH)
        crime_count = crime_df.count()
        print(f"✅ [Debug] Crime Data Row Count: {crime_count}")
        # If the above prints 0, Date Parsing or Lat/Lon filtering failed!

        zillow_df = clean_zillow_data(spark, ZILLOW_PATH)
        zillow_count = zillow_df.count()
        print(f"✅ [Debug] Zillow Data Row Count: {zillow_count}")
        # If this prints 0, maybe City filtering (== "Chicago") did not match case.

        # 2. Check spatial merging
        crime_agg_df = perform_spatial_join_and_aggregate(spark, crime_df, boundary_df)
        crime_agg_count = crime_agg_df.count()
        print(f"✅ [Debug] Spatial Merge (Crime + Boundary) Row Count: {crime_agg_count}")
        # If this prints 0, coordinate systems mismatch, point and polygon did not intersect!

        # 3. Check final Inner Join
        final_master_df = merge_master_dataset(crime_agg_df, zillow_df, population_df=None)
        final_count = final_master_df.count()
        print(f"✅ [Debug] Final Inner Join Row Count: {final_count}")
        # If the above has data, but this becomes 0, ZIP_Code or time format is definitely misaligned!

        final_years = final_master_df.selectExpr(
            "MIN(Year) AS min_year",
            "MAX(Year) AS max_year",
            "COUNT(DISTINCT Year) AS distinct_years"
        ).collect()[0]
        print(
            "✅ [Debug] Final Dataset Year Range: "
            f"{final_years['min_year']} -> {final_years['max_year']} "
            f"({final_years['distinct_years']} distinct years)"
        )

        if final_count > 0:
            if (
                final_years["min_year"] is None
                or final_years["max_year"] is None
                or final_years["min_year"] < TARGET_START_YEAR
                or final_years["max_year"] > TARGET_END_YEAR
            ):
                raise ValueError(
                    f"Final dataset escaped the supported year window "
                    f"{TARGET_START_YEAR}-{TARGET_END_YEAR}: {final_years}"
                )

            print(">>> Writing final dataset as CSV...")
            (
                final_master_df
                .write
                .mode("overwrite")
                .option("header", "true")
                .csv(OUTPUT_CSV_PATH)
            )
            print(f">>> CSV output saved to: {OUTPUT_CSV_PATH}")
        else:
            raise ValueError(
                "Final dataset is empty after restricting to 2014-2024. "
                "Check whether the crime and Zillow sources both contain Chicago ZIP data "
                "for that year window."
            )
    finally:
        spark.stop()
