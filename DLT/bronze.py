# Bronze Layer - Rearc Data Quest
# This module implements the Bronze layer of the Spark Declarative Pipeline.
#
# Bronze Tables (Raw Ingestion with Basic Expectations):
# - bronze_bls_productivity: Raw BLS productivity data with schema validation
# - bronze_population: Raw population data with schema validation
#
# Data Sources:
# - BLS Data: /Volumes/rearc_quest/quest_1/raw_data/pr.data.0.Current
# - Population Data: /Volumes/rearc_quest/quest_1/raw_data/population.json

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import json


@dp.table(
    name="bronze_bls_productivity",
    comment="Raw BLS productivity time-series data with basic schema validation",
    table_properties={"quality": "bronze", "layer": "raw"}
)
@dp.expect_or_fail("has_required_columns", "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL")
def bronze_bls_productivity():
    """
    Ingest raw BLS productivity data from tab-delimited file.
    
    Expectations:
    - Required columns must be present (series_id, year, period)
    - This fails the pipeline if schema is invalid
    
    No transformations applied at Bronze - data stored as-is from source.
    """
    # Read tab-delimited file with minimal processing
    df = spark.read.format("csv") \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load("/Volumes/rearc_quest/quest_1/raw_data/pr.data.0.Current")
    
    # Strip whitespace from column names (BLS files have padded headers)
    column_names = df.columns
    df_bronze = df.toDF(*[col.strip() for col in column_names])
    
    return df_bronze


@dp.table(
    name="bronze_population",
    comment="Raw US population data from World Bank API with schema validation",
    table_properties={"quality": "bronze", "layer": "raw"}
)
@dp.expect_or_fail("has_required_fields", "date IS NOT NULL AND value IS NOT NULL")
def bronze_population():
    """
    Ingest raw population data from World Bank API JSON file.
    
    Expectations:
    - Required fields must be present (date, value)
    - This fails the pipeline if API response structure changes
    
    Note: World Bank API returns nested JSON array structure:
    [[metadata_object], [data_records...]]
    
    We extract the data records array and create a DataFrame preserving
    the original API response structure.
    """
    # Read and parse JSON using Python (Spark's JSON reader struggles with this structure)
    with open("/Volumes/rearc_quest/quest_1/raw_data/population.json", "r") as f:
        json_data = json.load(f)
    
    # Extract the data array (second element)
    data_records = json_data[1]
    
    # Create DataFrame from raw API response
    df_bronze = spark.createDataFrame(data_records)
    
    return df_bronze
