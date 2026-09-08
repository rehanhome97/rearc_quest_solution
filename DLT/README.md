## Overview
This is a complete Bronze → Silver → Gold data pipeline built using Databricks Lakeflow Spark Declarative Pipelines (SDP).

## Architecture

### Data Flow
```
Raw Data (Unity Catalog Volume)
    ↓
Bronze Layer (Raw Ingestion)
    ↓
Silver Layer (Cleaned & Typed)
    ↓
Gold Layer (Business Metrics)
```

### Data Source
* **Location**: `/Volumes/rearc_quest/quest_1/raw_data/`
* **BLS Data**: `pr.data.0.Current` (tab-delimited)
* **Population Data**: `population.json` (World Bank API)

## Pipeline Structure

### Bronze Layer (`bronze.py`)
Raw data ingestion with basic schema validation.

**Tables:**
* `bronze_bls_productivity` (38,469 rows)
  - Raw BLS productivity time-series data
  - Expectations: series_id, year, period must be non-null (FAIL on violation)
  
* `bronze_population` (66 rows)
  - Raw population data from World Bank API
  - Expectations: date, value must be non-null (FAIL on violation)

### Silver Layer (`silver.py`)
Cleaned and type-cast data with data quality checks.

**Tables:**
* `silver_bls_productivity` (38,469 rows)
  - Cleaned BLS data with proper types
  - Transformations: strip whitespace, cast year to INT, value to DOUBLE
  - Expectations: DROP rows with null series_id, year, period, or value
  
* `silver_population` (66 rows)
  - Population data with proper types
  - Transformations: extract year, cast to INT; extract population, cast to LONG
  - Expectations: DROP rows with null year or population

### Gold Layer (`gold.py`)
Business metrics and analytical results.

**Tables:**

1. **`gold_population_stats_2013_2018`** (1 row)
   - **Question**: Population statistics for 2013-2018
   - **Metrics**: mean_population, stddev_population
   - **Result**: Mean = 322,881,748; StdDev = 4,474,712

2. **`gold_bls_best_year_per_series`** (237 rows)
   - **Question**: Best year per BLS series based on quarterly performance
   - **Logic**: Sum Q01-Q04 values per series/year, rank by total
   - **Output**: series_id, best_year, total_value

3. **`gold_bls_prs30006032_population`** (32 rows)
   - **Question**: Join BLS series PRS30006032 with population data
   - **Logic**: Left join on year, filter for Q01 period
   - **Output**: year, productivity_value, population

## Data Quality Framework

### Expectations at Bronze
* **Strategy**: FAIL pipeline on invalid schema
* **Purpose**: Catch data source changes immediately

### Expectations at Silver
* **Strategy**: DROP invalid rows
* **Purpose**: Clean data while preserving valid records

### Results
* All 7 tables created successfully
* All expectations passed
* No data quality violations detected

## Implementation Details

### Modern SDP Syntax
* Import: `from pyspark import pipelines as dp`
* Tables: `@dp.table()` for streaming, `@dp.materialized_view()` for batch
* Expectations: `@dp.expect_or_drop()`, `@dp.expect_or_fail()`
* Reads: `spark.read.table()` for batch, `spark.readStream.table()` for streaming

### Pipeline Configuration
* **Catalog**: `rearc_quest`
* **Schema**: `quest_1`
* **Channel**: CURRENT
* **Compute**: Serverless (with Photon)
* **Mode**: Batch (not continuous)

## Files
* `bronze.py` - Bronze layer definitions
* `silver.py` - Silver layer definitions
* `gold.py` - Gold layer definitions (PySpark)
* `gold_sql.sql` - Gold layer alternatives (SQL)

## Running the Pipeline

### Full Refresh
```python
# Run from pipeline editor or monitoring page
startPipelineUpdate(pipelineId="...", fullRefresh=False)
```

### Dry Run (Validation)
```python
startPipelineDryRun(pipelineId="...")
```

## Results Summary

| Layer | Tables | Total Rows |
|-------|--------|------------|
| Bronze | 2 | 38,535 |
| Silver | 2 | 38,535 |
| Gold | 3 | 270 |
| **Total** | **7** | **77,340** |

## Analytical Questions

### Q1: Population Statistics (2013-2018)
**Answer**: Mean = 322,881,748; StdDev = 4,474,712

### Q2: Best Year per BLS Series
**Answer**: 237 series analyzed, best year identified for each

### Q3: BLS PRS30006032 + Population
**Answer**: 32 years of joined productivity and population data
