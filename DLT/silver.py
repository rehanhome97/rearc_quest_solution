# Silver Layer - Rearc Data Quest
# This module implements the Silver layer of the Spark Declarative Pipeline.
#
# Silver Tables (Cleaned and Type-Cast Data):
# - silver_bls_productivity: BLS productivity time-series data with data quality checks
# - silver_population: US population data from World Bank API
#
# Dependencies:
# Silver tables read from Bronze tables:
# - bronze_bls_productivity
# - bronze_population

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, LongType


@dp.table(
    name="silver_bls_productivity",
    comment="Cleaned BLS productivity time-series data with data quality checks",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL")
@dp.expect_or_drop("valid_year", "year IS NOT NULL")
@dp.expect_or_drop("valid_period", "period IS NOT NULL")
@dp.expect_or_drop("valid_value", "value IS NOT NULL")
def silver_bls_productivity():
    """
    Clean and transform BLS productivity data from Bronze layer.
    
    Transformations:
    - Strip whitespace from string values (BLS data contains padding)
    - Cast year to INT and value to DOUBLE
    - Enforce non-null constraints on key fields (via expectations)
    
    Data Quality:
    - Drop rows with null series_id, year, period, or value
    """
    # Read from Bronze table
    df = spark.read.table("bronze_bls_productivity")
    
    # Strip whitespace from string columns and cast types
    df_cleaned = df.select(
        F.trim(F.col("series_id")).alias("series_id"),
        F.trim(F.col("year")).cast(IntegerType()).alias("year"),
        F.trim(F.col("period")).alias("period"),
        F.trim(F.col("value")).cast(DoubleType()).alias("value"),
        F.trim(F.col("footnote_codes")).alias("footnote_codes")
    )
    
    return df_cleaned


@dp.table(
    name="silver_population",
    comment="US population data from World Bank API with type conversions",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("valid_year", "year IS NOT NULL")
@dp.expect_or_drop("valid_population", "population IS NOT NULL")
def silver_population():
    """
    Clean and transform population data from Bronze layer.
    
    Transformations:
    - Extract year from 'date' field
    - Extract population from 'value' field
    - Cast to appropriate types (INT for year, LONG for population)
    
    Data Quality:
    - Drop rows with null year or population (via expectations)
    """
    # Read from Bronze table
    df = spark.read.table("bronze_population")
    
    # Extract year and population, casting to appropriate types
    df_cleaned = df.select(
        F.col("date").cast(IntegerType()).alias("year"),
        F.col("value").cast(LongType()).alias("population")
    )
    
    return df_cleaned
