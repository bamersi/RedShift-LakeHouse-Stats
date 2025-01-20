CREATE OR REPLACE procedure operations.sp_get_tables()
AS
$$
/*
This collects internal and external table info from the RedShift cluster
*/
BEGIN
--TODO REPLACE getDate() with variable
-- DECLARE 
--  var_cur_date date;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Collect internal table information
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------
--Table info
----------------
drop table if exists tmp_internal_tables;
create temp table tmp_internal_tables as 
select  cast(a.table_id as INT) as "table_id"
        ,trim(a."table") as "table_name"
        ,trim(a."schema") as "schema_name"
        ,trim(a."schema") || '.' || trim(a."table") as "schema_table"
        ,cast('cluster' as varchar) as "location"
        ,a.encoded
        ,a.diststyle
        ,a.sortkey1
        ,a.max_varchar
        ,a.sortkey1_enc
        ,a.sortkey_num
from svv_table_info a
where "schema" not in ('pg_catalog')--Exclude catalog tables
and "schema" not like 'pg_temp%'--Exclude temp tables
;

--Get Table Create DateTime
drop table if exists tmp_internal_tables_create_date;
create temp table tmp_internal_tables_create_date
as
select
cast(a.reloid as int) as "table_id"
,trim(nspname) || '.' || trim(relname) as "schema_table"
,relcreationtime as "table_creation_time"
from pg_class_info a left join pg_namespace b
                      on a.relnamespace = b.oid
where trim(nspname) not in ('pg_catalog')--Exclude catalog tables
and trim(nspname) not like 'pg_temp%'--Exclude temp tables
;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Collect spectrum table information
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_spectrum_tables;
create temp table tmp_spectrum_tables as
select  -1 as "table_id" 
        ,trim("tablename") as "table_name"
        ,trim("schemaname") as "schema_name"
        ,trim("schemaname") || '.' || trim("tablename") as "schema_table"
        ,tabletype
        ,location
        ,input_format
        ,output_format
        ,serialization_lib
        ,serde_parameters
        ,compressed
        ,parameters
from svv_external_tables
;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Union spectrum and internal tables
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_tables;
create temp table tmp_tables as 
select   a.table_id
        ,a.table_name
        ,a.schema_name
        ,a.schema_table
        ,b.table_creation_time
        ,'internal_table' as "ObjectSrc" 
        ,a.location
        ,a.encoded
        ,a.diststyle
        ,a.sortkey1
        ,a.max_varchar
        ,a.sortkey1_enc
        ,a.sortkey_num
        ,cast(null as text) as "input_format"
        ,cast(null as text) as "output_format"
        ,cast(null as text) as "serialization_lib"
        ,cast(null as text) as "serde_parameters"
        ,cast(null as int) as "compressed"
        ,cast(null as text) as "parameters"            
from tmp_internal_tables a left join tmp_internal_tables_create_date b on a.table_id = b.table_id
union
select  table_id
        ,table_name
        ,schema_name
        ,schema_table
        ,null as "table_creation_time"
        ,'spectrum_table' as "ObjectSrc"
        ,location       
        ,null
        ,null
        ,null
        ,null
        ,null
        ,null
        ,input_format
        ,output_format
        ,serialization_lib
        ,serde_parameters
        ,compressed
        ,parameters        
from tmp_spectrum_tables
;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Insert new data
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
insert into operations.tables
(table_id,start_date,end_date,most_recent
,table_name,schema_name,schema_table,table_creation_time,ObjectSrc,location,encoded,diststyle,sortkey1,max_varchar,sortkey1_enc,sortkey_num,input_format,output_format,serialization_lib,serde_parameters,compressed,parameters)
select 
table_id
,cast(getdate() as date) as "start_date"
,cast('29990101' as date) as "end_date"
,1
,table_name,schema_name,schema_table,table_creation_time,ObjectSrc,location,encoded,diststyle,sortkey1,max_varchar,sortkey1_enc,sortkey_num,input_format,output_format,serialization_lib,serde_parameters,compressed,parameters 
from tmp_tables s
where schema_table not in (select schema_table from operations.tables)
;

--------------------------------------------------------------------------------------------------
--Identify modified records
--------------------------------------------------------------------------------------------------
drop table if exists tmp_tables_modified;
create temp table tmp_tables_modified as 
select
         table_id,table_name,schema_name,schema_table,table_creation_time,ObjectSrc,location,encoded,diststyle,sortkey1,max_varchar,sortkey1_enc,sortkey_num
         ,input_format,output_format,serialization_lib,serde_parameters,compressed,parameters 
from tmp_tables
except
select
         table_id,table_name,schema_name,schema_table,table_creation_time,ObjectSrc,location,encoded,diststyle,sortkey1,max_varchar,sortkey1_enc,sortkey_num
         ,input_format,output_format,serialization_lib,serde_parameters,compressed,parameters  
from operations.tables
where most_recent='True'
;


--------------------------------------------------------------------------------------------------
--In the event that a record already exists for today's start date, delete it
--This could happen if we run the proc twice in a single day and data has changed between runs
--------------------------------------------------------------------------------------------------
delete operations.tables
where schema_table in (select schema_table from tmp_tables_modified)
and start_date = cast(getdate() as date)
;

--------------------------------------------------------------------------------------------------
--Insert modified records
--------------------------------------------------------------------------------------------------
insert into operations.tables
(start_date
,table_id,table_name,schema_name,schema_table,table_creation_time,ObjectSrc,location,encoded,diststyle,sortkey1,max_varchar,sortkey1_enc,sortkey_num,input_format,output_format,serialization_lib,serde_parameters,compressed,parameters)
select 
cast(getdate() as date)as "start_date"
,table_id,table_name,schema_name,schema_table,table_creation_time,ObjectSrc,location,encoded,diststyle,sortkey1,max_varchar,sortkey1_enc,sortkey_num,input_format,output_format,serialization_lib,serde_parameters,compressed,parameters 
from tmp_tables_modified
;

--------------------------------------------------------------------------------------------------
--SET ENDDATES
--------------------------------------------------------------------------------------------------
--Reset all
update operations.tables set end_date=null where end_date is not null;

--Set EndDates Spectrum
with lsd as 
(
  select  schema_table
          ,start_date
          ,lead(start_date) over (partition by schema_table order by start_date) as "lead_start_date"
  from operations.tables
)
update operations.tables
set end_date=isnull(lead_start_date-1,'29990101')
from operations.tables t inner join lsd s 
                            on t.schema_table=s.schema_table 
                            and s.start_date=t.start_date
where operations.tables.schema_table=s.schema_table 
and operations.tables.start_date = s.start_date
;

--------------------------------------------------------------------------------------------------
--Reset all mostRecent
--------------------------------------------------------------------------------------------------
update operations.tables set most_recent='False' where most_recent<>'False';

--Spectrum
with LastRec as 
(
    select schema_table,max(start_date) as "start_date" 
    from operations.tables 
    group by schema_table
)
update operations.tables
set most_recent='True'
from operations.tables t inner join LastRec s 
                            on t.schema_table=s.schema_table 
                            and s.start_date=t.start_date
where operations.tables.schema_table=s.schema_table 
and operations.tables.start_date = s.start_date
;

--------------------------------------------------------------------------------------------------
--set end date for tables no longer present
--------------------------------------------------------------------------------------------------
update operations.tables set end_date=cast(getdate() as date)
where table_id not in (select table_id from tmp_tables)
and most_recent='True'
;

END;
$$ 
LANGUAGE plpgsql
;
