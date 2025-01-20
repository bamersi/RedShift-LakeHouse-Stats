--TODO:
-- ADD LAST MODIFIED 
-- ADD INDICATION IF QUERIES ARE MUCH SLOWER

--Total script execution time: 17m 31s

CREATE OR REPLACE procedure operations.sp_create_table_stats_byday()
AS
$$
BEGIN
------------------------------------------------------------------------------------------
--Create Calendar
------------------------------------------------------------------------------------------
drop table if exists tmp_MinMax;
create table tmp_MinMax as
select  min(Start_Date) as "FD"
        ,max(case when end_date > trunc(getdate()) then trunc(getdate()) else end_date end) as "LD"
        ,schema_table
        ,schema_name
from operations.tables
group by schema_table,schema_name
;

----------------------------------------------------------------------------------------------
--Forward fill internal table size: Create SCD
----------------------------------------------------------------------------------------------
drop table if exists tmp_internal_table_space;
create temp table tmp_internal_table_space as
select 
         min(snapshot_date) as "start_date"
        ,cast(null as date) as "end_date"
        ,schema_table
        ,tablesize_kb
        ,tbl_rows
from
      (
        select 
                snapshot_date
                ,schema_table
                ,tablesize_kb
                ,tbl_rows
                ,dense_rank() over (partition by schema_table order by snapshot_date) 
                 - dense_rank() over (partition by schema_table,tablesize_kb,tbl_rows order by snapshot_date)
                 as "Rn"
        from operations.internal_table_space                
      )
group by                 
          schema_table
          ,tablesize_kb
          ,tbl_rows
          ,Rn
;

--Set end dates
with ED as (select schema_table,start_date,lead(start_date) over (partition by schema_table order by start_date) as "LED" from tmp_internal_table_space)
update tmp_internal_table_space
set end_date = isnull(s.LED-1,'20990101')
from ED s
where s.schema_table=tmp_internal_table_space.schema_table 
and s.start_date=tmp_internal_table_space.start_date
;

----------------------------------------------------------------------------------------------
--Forward fill spectrum size: Create SCD
----------------------------------------------------------------------------------------------
drop table if exists tmp_spectrum_table_space;
create temp table tmp_spectrum_table_space as
select 
         min(snapshot_date) as "start_date"
        ,cast(null as date) as "end_date"
        ,schema_table
        ,tablesize_kb
from
      (
        select 
                snapshot_date
                ,schema_table
                ,tablesize_kb
                ,dense_rank() over (partition by schema_table order by snapshot_date) 
                 - dense_rank() over (partition by schema_table,tablesize_kb order by snapshot_date)
                 as "Rn"
        from operations.spectrum_table_space  
      )
group by                 
          schema_table
          ,tablesize_kb
          ,Rn
;

--Set end dates
with ED as (select schema_table,start_date,lead(start_date) over (partition by schema_table order by start_date) as "LED" from tmp_spectrum_table_space)
update tmp_spectrum_table_space
set end_date = isnull(s.LED-1,'20990101')
from ED s
where s.schema_table=tmp_spectrum_table_space.schema_table 
and s.start_date=tmp_spectrum_table_space.start_date
;

--------------------------------------------------------------------------------------------------
--Create filtered version of spectrum size exclude table 
--Filter out duplicates or prior filtering
--------------------------------------------------------------------------------------------------
drop table if exists tmp_spectrum_table_size_excludes;
create temp table tmp_spectrum_table_size_excludes as
with last_exclude as
(
  select schema_table,max(created_at) as "mx"
  from operations.spectrum_table_size_excludes
  where schema_table not in (select schema_table from operations.spectrum_table_space)
  group by schema_table
)
select a.schema_table,a.exclude_reason 
from operations.spectrum_table_size_excludes a inner join last_exclude b 
                                                    on a.schema_table=b.schema_table 
                                                    and a.created_at=b.mx
;

-------------------------------------------------------------------------------------------------
--Final table create
-------------------------------------------------------------------------------------------------
truncate table operations.table_stats_byday;
insert into operations.table_stats_byday
select  
         cal.date as "snapshot_date"
        ,mm.schema_table         
        ,cast(t.table_creation_time as date) as "table_creation_date"
        ,t.objectsrc as "objectsrc"
        ,isnull(cast(spi.tablesize_kb as float),sps.tablesize_kb) as "tablesize_kb"
        ,spi.tbl_rows*1.0/1000000 as "tbl_rows_mm"       
         ,isnull(count(distinct q.query_id),0) as "cnt_query"
         ,isnull(count(distinct q.usename),0) as "cntd_usename"   
         ,isnull(sum(q.aborted),0) as "sum_aborted"
         ,isnull(min(q.duration_second),0) as "min_query_duration_sec"
         ,isnull(max(q.duration_second),0) as "max_query_duration_sec"
         ,isnull(avg(q.duration_second),0) as "avg_query_duration_sec"  
         ,ex.exclude_reason as "size_exclude_reason"                
        --IDS and other table properties
        ,isnull(t.table_id,-1) as "table_id"
        ,t.encoded
        ,t.diststyle
        ,t.sortkey_num
        ,t.input_format
        ,t.output_format
        ,t.parameters
from tmp_MinMax mm inner join mappings.calendar cal
                          on cal.date between mm.FD and mm.LD
                          and cal.date < getdate()
                          and cal.date > '20211111' --Date we started to collect data
                    inner join operations.tables t
                          on  t.schema_table = mm.schema_table 
                          and cal.date between t.start_date and t.end_date
                    left join operations.query_dependencies q
                          on q.schema_table = mm.schema_table
                          and cal.date = cast(q.starttime as date)
                    left join tmp_internal_table_space spi
                          on cal.date between spi.start_date and spi.end_date
                          and spi.schema_table = mm.schema_table
                    left join tmp_spectrum_table_space sps
                          on cal.date between sps.start_date and sps.end_date
                          and sps.schema_table = mm.schema_table
                    left join tmp_spectrum_table_size_excludes ex
                          on mm.schema_table = ex.schema_table
group by
         cal.date
        ,mm.schema_table         
        ,cast(t.table_creation_time as date)
        ,t.objectsrc
        ,isnull(cast(spi.tablesize_kb as float),sps.tablesize_kb)
        ,spi.tbl_rows*1.0/1000000
         ,ex.exclude_reason
        ,isnull(t.table_id,-1)
        ,t.encoded
        ,t.diststyle
        ,t.sortkey_num
        ,t.input_format
        ,t.output_format
        ,t.parameters        
--order by 1 desc

;
END;
$$ 
LANGUAGE plpgsql
;
