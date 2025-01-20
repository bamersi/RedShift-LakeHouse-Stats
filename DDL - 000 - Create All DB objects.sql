CREATE SCHEMA IF NOT EXISTS operations
;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.sql_queries
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists operations.sql_queries
;
CREATE TABLE operations.sql_queries (
    query_id bigint ENCODE az64,
    userid integer ENCODE az64,
    querytxt character varying(65535) ENCODE lzo,
    usename character varying(128) ENCODE lzo,
    duration_second bigint ENCODE az64,
    starttime timestamp without time zone ENCODE az64,
    endtime timestamp without time zone ENCODE az64,
    aborted integer ENCODE az64,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64,
    UNIQUE (query_id)
)
DISTSTYLE EVEN
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.v_view_dependency  
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create view operations.v_view_dependency  
as
select distinct srcobj.oid as src_oid
       ,srcnsp.nspname as src_schemaname
       ,srcobj.relname as src_objectname
       ,tgtobj.oid as dependent_viewoid
       ,tgtnsp.nspname as dependent_schemaname
       ,tgtobj.relname as dependent_objectname
from (((((pg_class srcobj
  join pg_depend srcdep on ( (srcobj.oid = srcdep.refobjid)))
  join pg_depend tgtdep on ( (srcdep.objid = tgtdep.objid)))
  join pg_class tgtobj
    on ( ( (tgtdep.refobjid = tgtobj.oid)
   and (srcobj.oid <> tgtobj.oid))))
  left join pg_namespace srcnsp on ( (srcobj.relnamespace = srcnsp.oid)))
  left join pg_namespace tgtnsp on ( (tgtobj.relnamespace = tgtnsp.oid)))
where ((tgtdep.deptype = 'i'::"char") and (tgtobj.relkind = 'v'::"char"))
;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.tables
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE operations.tables (
    start_date date ENCODE az64,
    end_date date ENCODE az64,
    most_recent boolean ENCODE raw,
    table_id integer ENCODE az64,
    table_name character varying(128) ENCODE lzo,
    schema_name character varying(128) ENCODE lzo,
    schema_table character varying(257) ENCODE lzo,
    table_creation_time timestamp without time zone ENCODE az64,
    objectsrc character varying(14) ENCODE lzo,
    location character varying(128) ENCODE lzo,
    encoded character varying(15) ENCODE lzo,
    diststyle character varying(139) ENCODE lzo,
    sortkey1 character varying(143) ENCODE lzo,
    max_varchar integer ENCODE az64,
    sortkey1_enc character(32) ENCODE lzo,
    sortkey_num integer ENCODE az64,
    input_format character varying(128) ENCODE lzo,
    output_format character varying(128) ENCODE lzo,
    serialization_lib character varying(128) ENCODE lzo,
    serde_parameters character varying(128) ENCODE lzo,
    compressed integer ENCODE az64,
    parameters character varying(128) ENCODE lzo,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64,
    UNIQUE (schema_table, start_date)
)
DISTSTYLE EVEN
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.object_dependencies
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE operations.object_dependencies (
    dependent_schema_objectname character varying(257) ENCODE lzo,
    src_schema_table character varying(257) ENCODE lzo,
    src_objectsrc character varying(14) ENCODE lzo,
    table_id integer ENCODE az64,
    dependent_oid integer ENCODE az64,
    dependent_objectsrc character varying(4) ENCODE lzo
)
DISTSTYLE EVEN
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.query_dependencies
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE operations.query_dependencies (
    table_name character varying(128) ENCODE lzo,
    schema_name character varying(128) ENCODE lzo,
    schema_table character varying(257) ENCODE lzo,
    objectsrc character varying(14) ENCODE lzo,
    query_id bigint ENCODE az64,
    querytxt character varying(65535) ENCODE lzo,
    usename character varying(128) ENCODE lzo,
    duration_second bigint ENCODE az64,
    starttime timestamp without time zone ENCODE az64,
    endtime timestamp without time zone ENCODE az64,
    aborted integer ENCODE az64
)
DISTSTYLE EVEN
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.internal_table_space
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE operations.internal_table_space (
    snapshot_date date ENCODE az64,
    table_id integer ENCODE az64,
    schema_table character varying(257) ENCODE lzo,
    tablesize_kb numeric(25,1) ENCODE az64,
    pct_used numeric(10,4) ENCODE az64,
    empty bigint ENCODE az64,
    unsorted numeric(5,2) ENCODE az64,
    stats_off numeric(5,2) ENCODE az64,
    tbl_rows numeric(38,0) ENCODE az64,
    skew_sortkey1 numeric(19,2) ENCODE az64,
    skew_rows numeric(19,2) ENCODE az64,
    estimated_visible_rows numeric(38,0) ENCODE az64,
    risk_event character varying(13312) ENCODE lzo,
    vacuum_sort_benefit numeric(12,2) ENCODE az64,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.stg_spectrum_table_size
------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE operations.stg_spectrum_table_size (
    schema_table character varying(256) ENCODE lzo,
    tablesize_kb integer ENCODE az64,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.spectrum_table_space
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE operations.spectrum_table_space (
    snapshot_date date ENCODE az64,
    table_id integer ENCODE az64,
    schema_table character varying(256) ENCODE lzo,
    tablesize_kb integer ENCODE az64,
    created_at timestamp without time zone DEFAULT ('now'::text)::timestamp without time zone ENCODE az64
)
DISTSTYLE EVEN
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--operations.table_stats_byday
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE operations.table_stats_byday (
    snapshot_date date ENCODE az64,
    schema_table character varying(257) ENCODE lzo,
    table_creation_date date ENCODE az64,
    objectsrc character varying(14) ENCODE lzo,
    tablesize_kb double precision ENCODE raw,
    tbl_rows_mm numeric(38,4) ENCODE az64,
    cnt_query bigint ENCODE az64,
    cntd_usename bigint ENCODE az64,
    sum_aborted bigint ENCODE az64,
    min_query_duration_sec bigint ENCODE az64,
    max_query_duration_sec bigint ENCODE az64,
    avg_query_duration_sec bigint ENCODE az64,
    size_exclude_reason character varying(256) ENCODE lzo,
    table_id integer ENCODE az64,
    encoded character varying(15) ENCODE lzo,
    diststyle character varying(139) ENCODE lzo,
    sortkey_num integer ENCODE az64,
    input_format character varying(128) ENCODE lzo,
    output_format character varying(128) ENCODE lzo,
    parameters character varying(128) ENCODE lzo
)
DISTSTYLE EVEN
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--View operations.v_data_lake_stats_byday
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop view if exists operations.v_data_lake_stats_byday;
create view operations.v_data_lake_stats_byday
as
select 
        Snapshot_Date
        ,sum(case when objectsrc='internal_table' then 1 else 0 end) as "table_count_internal"
        ,sum(case when objectsrc='spectrum_table' then 1 else 0 end) as "table_count_spectrum"
        ,sum(case when objectsrc='spectrum_table' and size_exclude_reason is not null then 1 else 0 end) as "table_count_spectrum_no_size"
        ,round(sum(case when objectsrc='internal_table' then tablesize_kb/1024.0/1024.0 else 0 end),0) as "size_GB_Internal"        
        ,round(sum(case when objectsrc='spectrum_table' then tablesize_kb/1024.0/1024.0 else 0 end),0) as "size_GB_Spectrum"

        ,avg(case when objectsrc='internal_table' then avg_query_duration_sec else null end) as "avg_query_duration_sec_Internal"
        ,avg(case when objectsrc='spectrum_table' then avg_query_duration_sec else null end) as "avg_query_duration_sec_External"
                
        ,cast(sum(case when objectsrc='internal_table' then sum_aborted else null end)*1.0/sum(case when objectsrc='internal_table' then cnt_query else null end) as decimal(18,6)) as "Pct_queries_aborted_Internal"
        ,cast(sum(case when objectsrc='spectrum_table' then sum_aborted else null end)*1.0/sum(case when objectsrc='spectrum_table' then cnt_query else null end) as decimal(18,6)) as "Pct_queries_aborted_External"
from operations.table_stats_byday 
group by Snapshot_Date
;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--View operations.v_table_space
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop view if exists operations.v_table_space;
create view operations.v_table_space
as
with i_last_flag as
(
    select table_id,max(snapshot_date) as "snapshot_date"
    from operations.internal_table_space
    group by table_id
)
,ex_last_flag as
(
    select schema_table,max(snapshot_date) as "snapshot_date"
    from operations.spectrum_table_space
    group by schema_table
)
select   snapshot_date
        ,table_id
        ,schema_table
        ,objectsrc
        ,cast (tablesize_kb*1.0/1024/1024 as decimal(18,2)) as "tablesize_GB" 
        ,tbl_rows
        ,last_snd_flag
from 
(
select   a.snapshot_date
        ,a.table_id
        ,a.schema_table
        ,'internal' as "objectsrc"
        ,a.tablesize_kb 
        ,a.tbl_rows
        ,case when b.snapshot_date is null then 0 else 1 end as "last_snd_flag"       
from operations.internal_table_space a left join i_last_flag b 
                                          on a.table_id=b.table_id 
                                          and a.snapshot_date=b.snapshot_date
union all
select   a.snapshot_date
        ,a.table_id
        ,a.schema_table
        ,'external' as "objectsrc"        
        ,a.tablesize_kb
        ,null as "tbl_rows"
        ,case when b.snapshot_date is null then 0 else 1 end  as "last_snd_flag"
from operations.spectrum_table_space a left join ex_last_flag b 
                                          on a.schema_table=b.schema_table
                                          and a.snapshot_date=b.snapshot_date
)
;
