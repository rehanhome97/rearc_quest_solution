# Gold Layer - Rearc Data Quest
# This module implements the Gold layer of the Spark Declarative Pipeline.
#
# Gold Tables (Aggregated Business Metrics - PySpark Implementation):
# - gold_population_stats_2013_2018: Population statistics (mean, stddev) for 2013-2018
# - gold_bls_best_year_per_series: Best performing year for each BLS series
# - gold_bls_prs30006032_population: Join specific BLS series with population data
#
# Dependencies:
# Gold tables read from Silver tables:
# - silver_bls_productivity
# - silver_population
#
# Note: SQL alternatives for these tables are available in gold_sql.sql

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="gold_population_stats_2013_2018",
    comment="Mean and standard deviation of US population for years 2013-2018 (PySpark)",
    table_properties={"quality": "gold", "implementation": "pyspark"}
)
def gold_population_stats_2013_2018():
    """
    ANALYTICAL QUESTION 1: Population Statistics (2013-2018)
    
    Calculate population statistics for the period 2013-2018.
    
    Metrics:
    - mean_population: Average population across the period
    - stddev_population: Sample standard deviation (stddev_samp)
    
    Implementation: PySpark (Primary)
    Alternative: See gold_sql.sql for Spark SQL implementation
    
    SQL Equivalent:
    --------------
    SELECT 
        AVG(population) AS mean_population,
        STDDEV_SAMP(population) AS stddev_population
    FROM silver_population
    WHERE year BETWEEN 2013 AND 2018
    """
    # Read from silver population table
    df = spark.read.table("silver_population")
    
    # PySpark implementation
    result = df.filter(
        (F.col("year") >= 2013) & (F.col("year") <= 2018)
    ).agg(
        F.avg("population").alias("mean_population"),
        F.stddev_samp("population").alias("stddev_population")
    )
    
    return result


@dp.materialized_view(
    name="gold_bls_best_year_per_series",
    comment="Best performing year (highest total value) for each BLS series based on quarterly data (PySpark)",
    table_properties={"quality": "gold", "implementation": "pyspark"}
)
def gold_bls_best_year_per_series():
    """
    ANALYTICAL QUESTION 2: Best Year per BLS Series
    
    Find the best year for each BLS series based on quarterly performance.
    
    Logic:
    1. Filter for quarterly periods (Q01, Q02, Q03, Q04)
    2. Sum quarterly values per series_id and year
    3. Use window function to rank years by total value (descending) within each series
    4. Select the top year (rank = 1) for each series
    
    Implementation: PySpark (Primary)
    Alternative: See gold_sql.sql for Spark SQL implementation
    
    SQL Equivalent:
    --------------
    WITH quarterly_totals AS (
        SELECT 
            series_id,
            year,
            SUM(value) AS total_value
        FROM silver_bls_productivity
        WHERE period IN ('Q01', 'Q02', 'Q03', 'Q04')
        GROUP BY series_id, year
    ),
    ranked_years AS (
        SELECT 
            series_id,
            year,
            total_value,
            ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_value DESC) AS rank
        FROM quarterly_totals
    )
    SELECT 
        series_id,
        year AS best_year,
        total_value
    FROM ranked_years
    WHERE rank = 1
    """
    # Read from silver table
    df_productivity = spark.read.table("silver_bls_productivity")
    
    # PySpark implementation
    # Step 1: Filter for quarterly periods and sum by series and year
    quarterly_totals = df_productivity.filter(
        F.col("period").isin(["Q01", "Q02", "Q03", "Q04"])
    ).groupBy("series_id", "year").agg(
        F.sum("value").alias("total_value")
    )
    
    # Step 2: Apply window function to rank years within each series
    window_spec = Window.partitionBy("series_id").orderBy(F.desc("total_value"))
    
    ranked = quarterly_totals.select(
        "*",
        F.row_number().over(window_spec).alias("rank")
    )
    
    # Step 3: Filter for top year (rank = 1) per series
    result = ranked.filter(F.col("rank") == 1).select(
        "series_id",
        F.col("year").alias("best_year"),
        "total_value"
    )
    
    return result


@dp.materialized_view(
    name="gold_bls_prs30006032_population",
    comment="BLS series PRS30006032 (Q01 period) joined with population data by year (PySpark)",
    table_properties={"quality": "gold", "implementation": "pyspark"}
)
def gold_bls_prs30006032_population():
    """
    ANALYTICAL QUESTION 3: BLS Series PRS30006032 with Population
    
    Join specific BLS productivity series (PRS30006032) with population data.
    
    Logic:
    1. Filter BLS productivity for series_id = 'PRS30006032' and period = 'Q01'
    2. Left join with population data on year
    3. Select year, productivity value, and population
    
    Implementation: PySpark (Primary)
    Alternative: See gold_sql.sql for Spark SQL implementation
    
    SQL Equivalent:
    --------------
    SELECT 
        p.year,
        p.value AS productivity_value,
        pop.population
    FROM silver_bls_productivity p
    LEFT JOIN silver_population pop 
        ON p.year = pop.year
    WHERE p.series_id = 'PRS30006032'
        AND p.period = 'Q01'
    ORDER BY p.year
    """
    # Read from silver tables
    df_productivity = spark.read.table("silver_bls_productivity")
    df_population = spark.read.table("silver_population")
    
    # PySpark implementation
    # Step 1: Filter for specific series and period
    filtered_bls = df_productivity.filter(
        (F.col("series_id") == "PRS30006032") & 
        (F.col("period") == "Q01")
    )
    
    # Step 2: Left join with population on year
    result = filtered_bls.join(
        df_population,
        "year",
        "left"
    ).select(
        "year",
        F.col("value").alias("productivity_value"),
        "population"
    ).orderBy("year")
    
    return result
