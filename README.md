# Data Lake Operational Logging

## Overview
This documentation provides an overview of the RedShift data lake-house operational logging system. The purpose is to gain better understanding of table usage patterns and space consumption metrics.

## Table Structure

### Reporting Tables

#### Operations.table_stats_byday
Contains daily snapshots of table metrics including:
- Table size
- Row count
- Query count
- Query duration

### Base Tables

#### Operations.sql_queries
Tracks all server query executions.

**Example Usage:**  
To find queries against `forecast_results` with duration > 60 seconds:

```sql
SELECT starttime, duration_second, usename, aborted, querytxt
FROM operations.sql_queries 
WHERE querytxt LIKE '%forecast_results%' 
AND duration_second > 60
ORDER BY starttime
```

**Load Details:**
- Populated by: `operations.sp_get_queries()`
- Load type: Daily incremental

#### Operations.tables
Type 2 slowly changing dimension (SCD) tracking historical table changes. History is built on `schema_table` rather than `table_id`.

**Example Usage:**  
To check parameter changes for a spectrum table:

```sql
SELECT start_date, end_date, most_recent, schema_table, table_id, objectsrc, parameters 
FROM operations.tables 
WHERE schema_table = 'infrabi.procurement' 
ORDER BY start_date
```

**Warning:**  
Table renames will break history tracking since it's based on `schema_table`. This approach was chosen because:
- RedShift doesn't assign table_id to spectrum tables
- Internal tables (e.g., STG tables) get new table_id on recreation

**Load Details:**
- Populated by: `operations.sp_get_tables()`
- Load type: Daily incremental (type 2 SCD)

#### Operations.object_dependencies
Tracks dependencies between views and tables, extending functionality of `admin.v_view_dependency` to include spectrum tables.

**Example Usage:**

```sql
SELECT dependent_objectsrc, dependent_schema_objectname, src_objectsrc, src_schema_table, table_id, dependent_oid
FROM operations.Object_Dependencies 
WHERE dependent_schema_objectname = 'configs.forecasts' 
ORDER BY 2,3,4
```

**Limitations:**
1. Only tracks view dependencies (not functions/stored procedures)
2. Spectrum table dependency tracking limited to one level deep

**Load Details:**
- Populated by: `operations.sp_get_object_depends()`
- Load type: Daily truncate & load

#### Operations.query_dependencies
Tracks dependencies between views and tables (both internal and spectrum).

**Warning:** Function and stored procedure dependencies are not tracked

**Load Details:**
- Populated by: `operations.sp_get_query_depends()`
- Load type: Daily truncate & load

#### Operations.internal_table_space
Daily tracking of internal table sizes and record counts.

**Load Details:**
- Populated by: `operations.sp_get_internal_table_space()`
- Load type: Daily incremental

#### Operations.spectrum_table_space
Monthly tracking of spectrum table sizes.

**Load Details:**
- Populated by: `operations.sp_get_spectrum_table_size()`
- Load type: Manual, monthly incremental

#### Operations.spectrum_table_size_excludes
Lists spectrum tables without size information, including exclusion reasons.

**Load Details:**
- Populated by: `operations.sp_get_spectrum_table_size()`
- Load type: Manual, monthly incremental

## Future Improvements

1. Review unique identification in `operations.tables`:
   - Current issue: Table renames treated as new table creation
   - Potential solution: Hybrid approach combining `table_id` and `schema_table`

2. Expand dependency tracking:
   - Add stored procedure and function dependencies
   - Extend spectrum table dependency tracking beyond one level

3. Additional Features:
   - Add `creator` field to `operations.tables`

## Known Limitations

- Spectrum table dependency tracking limited to one level
- No tracking of function/stored procedure dependencies
- Table renames break history tracking in `operations.tables`
