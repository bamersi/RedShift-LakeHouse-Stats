CREATE OR REPLACE procedure operations.sp_get_internal_table_space()
AS
$$
DECLARE 
  var_last_snapshot_date date;
  var_snapshot_date date:= cast(getdate() as date);  
BEGIN

--Get last Snapshot Date
select into var_last_snapshot_date isnull(max(snapshot_date),'19000101') from operations.internal_table_space;

--If a snapshot already exists, delete it
if var_last_snapshot_date = var_snapshot_date
then
  delete operations.internal_table_space where snapshot_date=var_snapshot_date;
end if
;

----------------------------------------------------------------------------------------------------------------------
--Insert Size data
----------------------------------------------------------------------------------------------------------------------
--drop table if exists operations.internal_table_space;
--create table operations.internal_table_space as
--alter table operations.internal_table_space add column created_at datetime default sysdate;
insert into operations.internal_table_space
(snapshot_date,table_id,schema_table,TableSize_kb,pct_used,empty,unsorted,stats_off,tbl_rows,skew_sortkey1,skew_rows,estimated_visible_rows,risk_event,vacuum_sort_benefit)
select  
        cast(getdate() as date) as "snapshot_date"
        ,cast(table_id as INT) as "table_id"        
        ,"schema" || '.' || "table" as "schema_table"
        ,size*1024.0 as "TableSize_kb" 
        ,pct_used
        ,empty
        ,unsorted
        ,stats_off
        ,tbl_rows
        ,skew_sortkey1
        ,skew_rows
        ,estimated_visible_rows
        ,risk_event
        ,vacuum_sort_benefit
from svv_table_info
;
END;
$$ 
LANGUAGE plpgsql
;


/*TEST
--Execution time: 2m 6s
call admin.sp_get_internal_table_space();

--2021-11-10 13:49:24
select distinct convert_timezone('America/New_York',created_at) as "created_at_NY" from operations.internal_table_space;

--2021-11-10	1	1078
select snapshot_date, count(distinct created_at) as "cnt_created_at", count(1) as "cnt" from operations.internal_table_space group by snapshot_date;
*/
