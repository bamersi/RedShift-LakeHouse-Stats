CREATE OR REPLACE procedure operations.sp_get_query_depends()
AS
$$
/*
This matches SQL Queries to tables and other objects (initially only views)
This is not an incremental script. The dataset is recreated on each run.
*/
BEGIN

------------------------------------------------------------------------------------------------------------------------
--Match tables and views with queries
--select count(1) from tmp_query_usage_narrow --13 246 131
------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_query_usage_narrow;
create temp table tmp_query_usage_narrow as
--Match tables to queries
select  
        tb.schema_table as "schema_table"
        ,q.Query_Id
        ,charindex(tb.schema_table,q.querytxt) as "match_location"
        ,len(tb.schema_table) as "schema_table_length"        
from operations.tables tb inner join operations.sql_queries q 
                                on charindex(tb.schema_table,q.querytxt) >0 
where tb.most_recent='True' --take most recent from SCD table operations.tables
union
--Match programability (e.g. views) to queries
select tb.src_schema_table
        ,q.Query_Id
        ,charindex(tb.dependent_schema_objectname,q.querytxt) as "match_location"
        ,len(tb.dependent_schema_objectname) as "schema_table_length"            
from operations.Object_Dependencies tb inner join operations.sql_queries q 
                                on charindex(tb.dependent_schema_objectname,q.querytxt) >0
;


----------------------------------------------------------------------------------------------------------------------------------------------
--This deletes duplicate matches due to objects with suffixes. E.g.:
--It also deletes cases where a table is referenced more than once (e.g. self joins)
--To fix, if there is an object match at the same location (duplicate match), take the match with the shorter length
----------------------------------------------------------------------------------------------------------------------------------------------
with duplicates as 
(
  select Query_Id,match_location,max(schema_table_length) as "max_schema_table_length"
  from tmp_query_usage_narrow 
  group by Query_Id,match_location
  having count(1) > 1
)
delete from tmp_query_usage_narrow
using duplicates s
where s.Query_Id=tmp_query_usage_narrow.Query_Id
and s.match_location=tmp_query_usage_narrow.match_location
and s.max_schema_table_length <> tmp_query_usage_narrow.schema_table_length
;

------------------------------------------------------------------------------------------------------------------------
--Join back to query and object details
--Union needed due to different unique identifier for internal and spectrum tables
------------------------------------------------------------------------------------------------------------------------
truncate table operations.query_dependencies;
insert into operations.query_dependencies
(table_name,schema_name,schema_table,ObjectSrc,query_id,querytxt,usename,duration_second,starttime,endtime,aborted)
select  distinct 
        tb.table_name 
        ,tb.schema_name 
        ,tb.schema_table 
        ,tb.ObjectSrc
        ,u.query_id 
        ,q.querytxt 
        ,q.usename 
        ,q.duration_second 
        ,q.starttime 
        ,q.endtime 
        ,q.aborted 
from operations.tables tb inner join tmp_query_usage_narrow u 
                            on u.schema_table=tb.schema_table
                          inner join operations.sql_queries q 
                            on q.query_id = u.query_id
where tb.most_recent='True'
;

END;
$$ 
LANGUAGE plpgsql
;

