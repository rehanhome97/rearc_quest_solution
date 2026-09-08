## Architecture

### Bronze/Silver/Gold Design

I chose the medallion architecture pattern for this pipeline because it provides clear separation of concerns:

**Bronze Layer (Raw Ingestion):**
- Direct ingestion from source files with minimal transformation
- `bronze_bls_productivity`: All BLS productivity series data as strings, preserving original formatting
- `bronze_population`: Raw JSON structure from World Bank API
- Schema enforcement via Auto Loader with explicit schema hints
- Rationale: Keep raw data immutable for audit trail and reprocessing capability

**Silver Layer (Cleaned & Typed):**
- Type conversions and data cleaning
- `silver_bls_productivity`: Trimmed strings, proper numeric types (INT for year, DOUBLE for value)
- `silver_population`: Extracted year and population from JSON structure, cast to INT and LONG
- Data quality expectations that DROP invalid rows (non-null constraints on keys)
- Rationale: Create clean, reliable datasets for downstream analytics while filtering bad data early

**Gold Layer (Analytics):**
- Business-specific aggregations and joins
- Three analytical tables answering specific questions
- Rationale: Pre-computed analytics for fast querying, separated from data cleaning logic

### SQL vs PySpark Choices

For each Gold table, I implemented both SQL and PySpark versions and chose one as primary:

**Table 1 - `gold_population_stats_2013_2018` (Primary: SQL)**
- Simple aggregation (MIN, MAX, AVG) over filtered data
- SQL is more readable for straightforward aggregations
- PySpark alternative included for teams preferring programmatic control

**Table 2 - `gold_bls_best_year_per_series` (Primary: PySpark)**
- Window functions with partitioning and ranking
- PySpark provides better control over complex transformations
- SQL alternative included for analysts comfortable with window functions

**Table 3 - `gold_bls_prs30006032_population` (Primary: SQL)**
- Simple join between two tables with filtering
- SQL join syntax is clearer for this straightforward operation
- PySpark alternative demonstrates programmatic join patterns

The dual implementation approach serves two purposes:
1. Team flexibility: data engineers can choose their preferred style
2. Documentation: each alternative shows equivalent patterns for learning

### Safe Re-Ingestion

The ingestion notebook handles re-runs safely:

1. **Volume Setup**: Uses `CREATE VOLUME IF NOT EXISTS` - idempotent, won't fail on reruns
2. **File Downloads**: Overwrites existing files, ensuring latest data
3. **Verification Step**: Always checks what's actually in the volume after write
4. **Pipeline Separation**: Ingestion is a separate notebook from DLT pipeline, so I can refresh source data without triggering full pipeline rebuild

This design lets me re-ingest source data (e.g., updated BLS series) without breaking existing pipeline state.

### Data Flow

```
Source Files (Volume: raw_data)
    ↓
Bronze Tables (Auto Loader, schema enforced)
    ↓
Silver Tables (Type conversions, data quality expectations)
    ↓
Gold Tables (Analytics: aggregations, joins, window functions)
```

## Trade-offs

This implementation is suitable for a data engineering assessment, but a real production system would require additional considerations:

### Schema Drift

**Current Approach:**
- Bronze uses explicit Auto Loader schema hints
- Schema is hardcoded in pipeline files

**Production Enhancement:**
- Implement `schemaEvolutionMode` in Auto Loader for graceful schema changes
- Add Bronze → Silver schema validation to catch unexpected columns
- Set up alerts when new columns appear
- Version schema definitions separately from pipeline code
- Consider schema registry integration for cross-pipeline consistency

### Data Volume

**Current Approach:**
- Full refresh of small datasets (< 100KB)
- No partitioning strategy

**Production Enhancement:**
- Partition Bronze tables by ingestion date or year
- Implement incremental processing for large time-series data
- Add liquid clustering for tables with high query concurrency
- Consider compaction strategies for frequently updated tables
- Size analytical processing appropriately (not serverless for huge volumes)

### Cost Optimization

**Current Approach:**
- Serverless compute for simplicity
- Full table scans acceptable for small data

**Production Enhancement:**
- Use REPLACE WHERE flows for incremental updates (only recent data)
- Partition pruning in Gold layer queries
- Separate hot (recent) and cold (historical) data into different tables
- Schedule pipeline runs during off-peak hours
- Right-size compute: use Classic clusters for predictable workloads

### Access Control

**Current Approach:**
- Single user workspace, no access restrictions
- All tables in one catalog/schema

**Production Enhancement:**
- Implement Unity Catalog governance with granular permissions
- Bronze: restricted to data engineers only
- Silver: read access for analytics team
- Gold: broad access for business users
- Row-level security for sensitive data (e.g., state-specific population views)
- Column masking for PII if present
- Audit logging via Unity Catalog lineage

### Monitoring & Alerting

**Current Approach:**
- Manual pipeline run verification
- Expectations log failures but don't alert

**Production Enhancement:**
- Set up pipeline failure notifications (email, Slack, PagerDuty)
- Data quality alerts when expectation violations exceed threshold
- Freshness monitoring: alert if Bronze hasn't updated in X hours
- Row count anomaly detection (sudden drops might indicate upstream issues)
- Query performance monitoring on Gold tables
- Integration with Databricks SQL Alerts for business metric thresholds

### Error Handling

**Current Approach:**
- Expectations drop bad rows silently
- Pipeline fails if Bronze data is missing

**Production Enhancement:**
- Write dropped rows to quarantine table for investigation
- Implement retry logic for transient ingestion failures
- Dead letter queue for unparseable records
- Graceful degradation: allow Silver to succeed even if some Bronze partitions fail
- Detailed error logging with context (which file, which row, why it failed)

### Testing

**Current Approach:**
- Manual validation of output tables
- No automated tests

**Production Enhancement:**
- Unit tests for transformation logic
- Integration tests for end-to-end pipeline
- Data quality tests beyond basic expectations (distribution checks, referential integrity)
- Staging environment for testing before production deployment
- Sample data fixtures for development

## Retrospective

### What Was Hardest to Get Right

**1. Unified Volume Path Management**

The most frustrating part was consolidating both data sources into a single volume. Initially, I had BLS data in one volume and population.json in another. When I decided to unify them, I had to update:
- Ingestion notebook paths
- Bronze layer Auto Loader paths
- Verification logic

The challenge was ensuring consistency across two separate files (ingest_data.py notebook and bronze.py pipeline file) that both referenced the paths. I had to carefully trace through each reference and verify the final state. A production system would benefit from centralized configuration management (e.g., parameters or a config table).

**2. Schema Definition for Auto Loader**

Auto Loader's schema inference didn't work perfectly out of the box for the BLS CSV data because:
- Values had whitespace padding that needed trimming
- Type inference was conservative (everything became STRING)
- I needed to ensure schema stability across pipeline runs

I ended up using explicit `schemaHints` which worked well but required manually defining the schema. For future projects, I'd establish a schema definition pattern earlier (maybe a shared schema registry or JSON schema files).

**3. Dual SQL/PySpark Implementation**

Maintaining two implementations of the same logic is inherently fragile. During development, I had to:
- Keep both versions synchronized
- Ensure they produced identical results
- Comment one out while testing the other
- Document which is "primary" vs "alternative"

In a real project, I'd pick ONE approach per team and stick with it. The dual implementation was useful for the assessment (showing versatility) but adds maintenance burden. If I had to keep both, I'd add automated tests to verify they produce identical output.

**4. Understanding Pipeline File vs Regular Notebook Execution**

There's a mental model shift between:
- Running code in a notebook (imperative: "do this now")
- Defining datasets in a pipeline (declarative: "this is what the data should look like")

I initially tried to mix concerns (putting pipeline dataset definitions in a notebook), which doesn't work. Once I separated:
- **Ingestion notebook**: Downloads files, writes to volume (run once or on schedule)
- **Pipeline files**: Read from volume, transform, materialize tables (DLT manages lifecycle)

...everything became clearer. The pipeline doesn't "run" the ingestion; it assumes the source files exist and processes them.

### What Went Smoothly

**1. Auto Loader Worked Well**

Once I had the schema hints correct, Auto Loader "just worked" for:
- CSV parsing (BLS data)
- JSON parsing (population data)
- Directory-level reads (all BLS series files at once)

No need for manual file listing or complex parsing logic.

**2. Data Quality Expectations Were Intuitive**

The `@dp.expect_or_drop()` decorator pattern made sense immediately:
```python
@dp.expect_or_drop("valid_series_id", "series_id IS NOT NULL")
```

It's clear what's being checked and what happens on failure. I could easily add more complex rules (e.g., `"year BETWEEN 1900 AND 2100"`) if needed.

**3. Medallion Architecture Paid Off**

Even for this small dataset, the layered approach:
- Made debugging easier (I could inspect Bronze to confirm ingestion worked)
- Separated concerns (data cleaning vs analytics)
- Made it easy to add new Gold tables without touching Bronze/Silver

### Key Learnings

1. **Start with Bronze layer first**: Get raw ingestion working before any transformations. It's tempting to jump straight to analysis, but having immutable raw data is crucial for debugging.

2. **Use expectations aggressively in Silver**: Bad data should be filtered early, not propagated to Gold. The `expect_or_drop` pattern prevents garbage from flowing downstream.

3. **Document the "why" not just the "what"**: My code comments explain transformations ("Strip whitespace from BLS data"), but I should have also documented WHY the data has whitespace (BLS CSV format quirk) for future maintainers.

4. **Separate ingestion from transformation**: Decoupling the ingestion notebook from the DLT pipeline gave me flexibility to refresh source data independently. This would be even more important in production with incremental ingestion.

5. **Test with the actual pipeline, not just in notebooks**: Ad-hoc notebook exploration is useful for development, but the pipeline execution model is different (dependencies, expectations, serverless compute). Always validate in the actual pipeline environment.

### If I Started Over

Knowing what I know now, I would:

1. **Define schemas upfront** in a separate `schemas.py` file that both ingestion and Bronze layer reference
2. **Use pipeline parameters** for paths instead of hardcoding (makes it easier to test with different volumes)
3. **Add a quarantine table** in Bronze for rows that fail expectations, so I can debug without losing data
4. **Pick ONE language** (PySpark or SQL) per layer and stick with it, rather than maintaining dual implementations
5. **Add basic unit tests** even for an assessment project - it would have caught my path inconsistencies faster
6. **Document the expected runtime** for each layer (Bronze: 30s, Silver: 15s, Gold: 10s) to catch performance regressions