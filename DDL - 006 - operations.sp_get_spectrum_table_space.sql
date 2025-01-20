CREATE OR REPLACE procedure operations.sp_get_spectrum_table_space()
AS
$$
BEGIN
/*
This takes the data from admin.stg_spectrum_table_size and inserts it into the final table: operations.spectrum_table_space
NOTE: Populating space for external tables is a script that must be run manually: EXE - 001 - Populate Spectrum Table Size (Manual).sql
*/

------------------------------------------------------
--Create temp table with source data
------------------------------------------------------
drop table if exists tmp_spectrum_table_space;
create temp table tmp_spectrum_table_space as
select  
        cast(convert_timezone('America/New_York',a.created_at) as date) as "snapshot_date"
        ,cast(-1 as INT) as "table_id"        
        ,a.schema_table
        ,a.TableSize_kb
from operations.stg_spectrum_table_size a
where cast(a.created_at as date)>='2021-11-15'
;

----------------------------------------------------------------------------------------------------------------------
--Insert Size data to final destination
----------------------------------------------------------------------------------------------------------------------
--drop table if exists operations.spectrum_table_space;
--create table operations.spectrum_table_space as
--alter table operations.spectrum_table_space add column created_at datetime default sysdate;
insert into operations.spectrum_table_space
(snapshot_date,table_id,schema_table,TableSize_kb)
select  
        snapshot_date
        ,table_id  
        ,schema_table
        ,TableSize_kb
from tmp_spectrum_table_space s
--Only insert if it does not exist for this snapshot date
where not exists (select 1 from operations.spectrum_table_space t where s.schema_table=t.schema_table and s.snapshot_date=t.snapshot_date)
;
END;
$$ 
LANGUAGE plpgsql
;



