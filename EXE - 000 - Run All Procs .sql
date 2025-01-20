------------------------------------------------------------------------
--Get Queries
------------------------------------------------------------------------
call operations.sp_get_queries()
;
select top 100 * from operations.sql_queries
;

------------------------------------------------------------------------
--Get Tables
------------------------------------------------------------------------
call  operations.sp_get_tables()
;
select top 100 * from operations.tables
;

------------------------------------------------------------------------
--Get Object Dependencies
------------------------------------------------------------------------
call operations.sp_get_object_depends()
;
select top 100 * from operations.query_dependencies
;

------------------------------------------------------------------------
--Get Query Dependencies
------------------------------------------------------------------------
call operations.sp_get_query_depends()
;
select top 100 * from operations.object_dependencies
;

------------------------------------------------------------------------
-- Get Internal Table space used
------------------------------------------------------------------------
call operations.sp_get_internal_table_space()
;
select top 100 * from operations.internal_table_space
;

------------------------------------------------------------------------
-- Get external Table space used
------------------------------------------------------------------------
call operations.sp_get_spectrum_table_space()
;
select top 100 * from operations.spectrum_table_space
;

------------------------------------------------------------------------
--Get Table stats by day
------------------------------------------------------------------------
call operations.sp_create_table_stats_byday()
;
select top 100 * from operations.table_stats_byday
;

------------------------------------------------------------------------
--Get Data lake stats by day
------------------------------------------------------------------------
select top 100 * from operations.v_data_lake_stats_byday
;

------------------------------------------------------------------------
--Get all table space
------------------------------------------------------------------------
select top 100 * from operations.v_table_space
;

