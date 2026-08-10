# LAB 4 — Silver Layer, Data Quality & Schema Evolution

## Project Goal

The goal of this lab was to build a reliable Silver-layer pipeline in Databricks that can handle both data changes and schema changes over time.

I used the Netflix titles dataset and designed the project as a small streaming pipeline rather than as a set of isolated examples. This allowed me to test ingestion, cleaning, deduplication, MERGE-based updates, schema enforcement, schema evolution, Delta column mapping, and table maintenance in one coherent workflow.

## Architecture

```text
Source CSV
   |
   v
Prepare simulated micro-batches
   |
   v
Landing area
   |
   v
Auto Loader
   |
   v
Bronze Delta table
   |
   v
Silver cleaning and enrichment
   |
   v
foreachBatch
   |
   v
MERGE (SCD Type 1)
   |
   v
Silver Delta table
```

Additional notebooks demonstrate schema enforcement, schema evolution, type widening, Delta column mapping, data-quality validation, OPTIMIZE, VACUUM, and reliability checks.

## Project Structure

- `00_setup` — defines paths, schemas, checkpoints, and common configuration.
- `01_prepare_stream_batches` — splits the original dataset into three batches and introduces controlled changes.
- `01a_load_batch` — moves a selected prepared batch into the landing directory.
- `02_bronze_autoloader` — ingests files incrementally with Databricks Auto Loader.
- `03_silver_cleaning` — cleans, normalizes, casts, and enriches Bronze data.
- `04_silver_merge_scd1` — deduplicates each micro-batch and applies SCD Type 1 using Delta MERGE.
- `05_schema_enforcement` — demonstrates rejection of a write with an incompatible schema.
- `06_schema_evolution` — demonstrates `mergeSchema`, adding columns, and type widening.
- `07_delta_column_mapping` — demonstrates safe rename and drop operations.
- `08_table_maintenance_reliability` — checks data quality, duplicates, table size/history, OPTIMIZE, and VACUUM.

## My Approach and Reasoning

### 1. Simulating streaming input

The original Netflix dataset is a static CSV file. To make the lab closer to a real ingestion scenario, I split it into multiple batches.

The batches were intentionally modified:

- Batch 1 contains the initial data.
- Batch 2 contains normal rows, an updated record, and a duplicate.
- Batch 3 introduces a new `imdb_rating` column.

This makes it possible to test more than simple ingestion. The pipeline must survive duplicates, updates, and a schema change.

### 2. Bronze ingestion with Auto Loader

I used Auto Loader because it is designed for incremental file ingestion.

The Bronze layer preserves source data and adds ingestion metadata:

- `ingestion_time`
- `source_file`
- `_rescued_data`

A checkpoint is used so already processed files are not processed again by the same streaming query.

Schema evolution is configured with:

```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

and the Delta write allows compatible schema changes with:

```python
.option("mergeSchema", "true")
```

### 3. Silver cleaning

The Silver layer is responsible for producing clean and analytics-ready data.

The transformations include:

- trimming string columns,
- normalizing `type`,
- normalizing ratings,
- parsing `date_added`,
- casting `release_year`,
- cleaning descriptive fields.

I also created derived columns such as:

- `audience_category`
- `release_period`
- `has_director`
- `silver_created_at`
- `silver_updated_at`

The purpose is to move data-quality and semantic preparation into Silver rather than leaving raw values for downstream consumers.

### 4. Deduplication and SCD Type 1

Before executing the MERGE, each micro-batch is deduplicated by `show_id`.

The latest record is selected using a window function and `ingestion_time`.

The Silver table then uses Delta Lake `MERGE`:

- matched `show_id` -> update the existing row,
- unmatched `show_id` -> insert a new row.

This implements SCD Type 1 because the current version replaces the previous version instead of storing historical versions.

`silver_created_at` is preserved for existing records, while `silver_updated_at` changes when a record is updated.

### 5. Schema Enforcement

Schema enforcement protects a Delta table from incompatible writes.

To demonstrate this, I intentionally changed `release_year` from an integer to a string and attempted to append it to the Silver table.

The write is rejected because the incoming schema does not match the target table schema.

After casting the value back to the expected integer type, the write succeeds.

This demonstrates the difference between accepting valid data and rejecting structurally incompatible data.

### 6. Schema Evolution

Schema evolution is useful when compatible schema changes are expected.

I demonstrated adding a new `quality_score` column. A normal append is rejected, while the write succeeds when `mergeSchema` is enabled.

The streaming scenario also introduces `imdb_rating` in a later source batch, showing schema evolution as part of the pipeline rather than only in an isolated example.

I additionally demonstrated type widening by changing an `INT` column to `BIGINT` after enabling Delta type widening.

## Data Contracts

Schema Evolution can be automatic. When a new column appears, it can be automatically added to the table. However, for example, if a data producer changes `customer_id` to `client_id`, automatic schema evolution may result in two columns: `customer_id` and `client_id`. The system does not understand that this change represents a change in the data contract.

A Data Contract is an agreement between a data producer and a data consumer. It defines the expected schema, data types, semantics, required fields, and data quality rules. Changes should be accepted only if they comply with the contract.

For this reason, automatic schema evolution is useful for compatible structural changes, while data contracts provide controlled evolution when the meaning of the data matters.

## Delta Column Mapping

Delta column mapping allows metadata-only column operations to be performed more safely.

I enabled column mapping by name on a test table and demonstrated:

- renaming `rating` to `age_rating`,
- dropping `director`.

The operations were performed on a dedicated test table so the production Silver table was not unnecessarily modified.

## Data Quality

The project validates several Silver-layer quality conditions:

- `show_id` should not be null or empty,
- `title` should not be null or empty,
- `type` should contain an accepted value,
- `release_year` should be within an expected range,
- `show_id` should remain unique after processing.

These checks are useful for detecting data-quality problems independently from schema enforcement. A value can have the correct data type and still be logically invalid.

## Reliability and Re-runs

The streaming queries use checkpoints to track processed input.

The Silver pipeline also uses MERGE instead of unconditional append. This means that processing an existing business key updates the current record instead of automatically creating another copy.

A duplicate check on `show_id` is used as an additional verification of Silver-table integrity.

The pipeline was also scheduled and executed as a Databricks Job.

## Table Maintenance

### OPTIMIZE

`OPTIMIZE` rewrites small Delta files into a more efficient physical layout. This reduces file-management overhead and can improve query performance.

I compare table details before and after OPTIMIZE and verify that the logical row count remains unchanged.

### VACUUM

`VACUUM` removes old data files that are no longer referenced by the current Delta table state after the configured retention period.

The lab uses:

```sql
VACUUM <table> RETAIN 168 HOURS
```

which keeps seven days of retention.

## Z-ORDER, Partitioning, and Liquid Clustering

Classic partitioning physically divides data according to selected partition columns. It can work well when access patterns are stable and the partition column has suitable cardinality, but poor partition choices can produce many small partitions and reduce flexibility.

Z-ORDER tries to organize data so that values frequently used in filters are stored close to each other. This can improve query performance through data skipping.

Liquid Clustering is a newer and more flexible approach to managing the physical layout of data. It provides more flexibility than traditional partitioning and Z-ORDER, especially when data and access patterns change over time.

The main trade-off is flexibility. Classic partitioning requires the layout decision earlier, Z-ORDER is an optimization applied to selected columns, while Liquid Clustering is designed to make the layout easier to evolve as workloads change.

## Final Result

The completed pipeline demonstrates:

- incremental ingestion with Auto Loader,
- Bronze and Silver layering,
- metadata columns,
- Silver cleaning and enrichment,
- deduplication,
- Delta MERGE,
- SCD Type 1,
- schema enforcement,
- schema evolution,
- adding new columns,
- type widening,
- Delta column mapping,
- data-quality validation,
- checkpoint-based reliability,
- scheduled execution,
- OPTIMIZE,
- VACUUM,
- understanding of partitioning, Z-ORDER, Liquid Clustering, and Data Contracts.

The final Silver table is designed to remain clean and usable while the incoming data and schema evolve over time.
