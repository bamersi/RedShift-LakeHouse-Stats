CREATE OR REPLACE procedure operations.sp_get_object_depends()
AS
$$
/*
This builds relationships between tables and views
It is used to track table usage
*/
DECLARE
  row record;
BEGIN

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Get dependencies between views referencing internal tables
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_view_dependency_internal;
create temp table tmp_view_dependency_internal as
select  cast(src_oid as INT) as "table_id" 
        ,cast(src_schemaname as varchar) as "src_schemaname" 
        ,cast(src_objectname as varchar) as "src_objectname" 
        ,cast(src_schemaname as varchar) || '.' || cast(src_objectname as varchar) as "src_schema_table" 
        ,cast(dependent_viewoid as INT) as "dependent_oid" 
        ,cast(dependent_schemaname as varchar) as "dependent_schemaname" 
        ,cast(dependent_objectname as varchar) as "dependent_objectname" 
        ,cast(dependent_schemaname as varchar) || '.' || cast(dependent_objectname as varchar) as "dependent_schema_objectname" 
from operations.v_view_dependency     
where "src_schemaname" not in ('pg_catalog' )
;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--Get dependencies between views referencing spectrum tables
--Views depending on spectrum tables are not stored in operations.v_view_dependency  
--select * from operations.v_view_dependency   where 'forecast_results' in (src_schemaname,dependent_schemaname) order by src_objectname

--An additional complication is that system tables with a PG prefix, such as pg_views reside on the leader node
--Data from these views on the leader node cannot be easily combined with user data that exists on compute nodes
--The only way I found how to do it is to create a cursor that pulls the data from pg_views
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_ViewDef;
create table tmp_ViewDef(schemaname varchar,viewname varchar, schema_view varchar, "DDL" varchar(max));
for row in 
      select 
           cast(schemaname as varchar) as "schemaname"
          ,cast(viewname as varchar) as "viewname"
          ,cast(schemaname as varchar) || '.' || cast(viewname as varchar) as "schema_view"             
          ,cast(definition as varchar(max)) as "DDL"
      from pg_views
  LOOP
  INSERT INTO tmp_ViewDef(schemaname,viewname,schema_view,DDL) VALUES (row.schemaname,row.viewname,row.schema_view,row.DDL);
END LOOP
;

----------------------------------------------------------------------------------------------------------------------------
--Create tmp_view_dependency_external
--Lists the dependencies between views and spectrum tables
----------------------------------------------------------------------------------------------------------------------------
drop table if exists tmp_view_dependency_external;
create temp table tmp_view_dependency_external as
select  cast(st.table_id as INT) as "table_id" 
        ,cast(st.schema_name as varchar) as "src_schemaname" 
        ,cast(st.table_name as varchar) as "src_objectname" 
        ,cast(st.schema_table as varchar)as "src_schema_table" 
        ,cast(-1 as INT) as "dependent_oid" --updated below
        ,cast(vd.schemaname as varchar) as "dependent_schemaname" 
        ,cast(vd.viewname as varchar) as "dependent_objectname" 
        ,vd.schema_view as "dependent_schema_objectname"
        ,charindex(st.schema_table,vd.ddl) as "match_location"
        ,len(st.schema_table) as "schema_table_length"
from tmp_ViewDef vd inner join operations.tables st 
                            on charindex(st.schema_table,vd.ddl) > 0 
where st.most_recent=1 --take most recent from SCD table operations.tables
and st.ObjectSrc='spectrum_table' --Only take spectrum tables
;

----------------------------------------------------------------------------------------------------------------------------------------------
--This deletes duplicate matches due to objects with suffixes. E.g.:
--It also deletes cases where a table is referenced more than once (e.g. self joins)
--To fix, if there is an object match at the same location (duplicate match), take the match with the shorter length
--Example:
/*
select dependent_schema_objectname,src_schema_table,match_location,schema_table_length 
from tmp_view_dependency_external 
where dependent_schema_objectname='aggregations_fifteen_min.ec2_usage_and_unused_fifteen_min_by_account_az_family_instance'
order by dependent_schema_objectname,src_schema_table
*/
----------------------------------------------------------------------------------------------------------------------------------------------
with duplicates as 
(
  select dependent_schema_objectname,match_location,max(schema_table_length) as "max_schema_table_length"
  from tmp_view_dependency_external 
  group by dependent_schema_objectname,match_location
  having count(1) > 1
)
delete from tmp_view_dependency_external
using duplicates s
where s.dependent_schema_objectname=tmp_view_dependency_external.dependent_schema_objectname
and s.match_location=tmp_view_dependency_external.match_location
and s.max_schema_table_length <> tmp_view_dependency_external.schema_table_length
;

------------------------------------------------------------------------------------------
--Update ViewId
------------------------------------------------------------------------------------------
update tmp_view_dependency_external
set dependent_oid=s.dependent_oid
from tmp_view_dependency_internal s inner join tmp_view_dependency_external t
on s.dependent_schema_objectname=t.dependent_schema_objectname
where tmp_view_dependency_external.dependent_schema_objectname=t.dependent_schema_objectname
;

----------------------------------------------------------------------------------------------------------------------------
--Union both external and internal
----------------------------------------------------------------------------------------------------------------------------
-- create table operations.Object_Dependencies as
truncate table operations.Object_Dependencies;
insert into operations.Object_Dependencies
(dependent_schema_objectname,src_schema_table,src_ObjectSrc,table_id,dependent_oid,dependent_ObjectSrc)
select  
        dependent_schema_objectname
        ,src_schema_table
        ,'internal_table' as "src_ObjectSrc"
        ,"table_id" 
        ,"dependent_oid"        
        ,'view' as "dependend_ObjectSrc"
from tmp_view_dependency_internal
UNION ALL
select  
        dependent_schema_objectname
        ,src_schema_table
        ,'spectrum_table'
        ,"table_id" 
        ,"dependent_oid"
        ,'view'
from tmp_view_dependency_external
;

END;
$$ 
LANGUAGE plpgsql
;


/*For debug only - same as above in stored proc
CREATE OR REPLACE procedure operations.sp_GetViewDefinition()
AS
$$
DECLARE 
  row record;
BEGIN
  drop table if exists tmp_ViewDef;
  create temp table tmp_ViewDef (schemaname varchar,viewname varchar, schema_view varchar, "DDL" varchar(max));
  for row in 
        select 
             cast(schemaname as varchar) as "schemaname"
            ,cast(viewname as varchar) as "viewname"
            ,cast(schemaname as varchar) || '.' || cast(viewname as varchar) as "schema_view"             
            ,cast(definition as varchar(max)) as "DDL"
        from pg_views
    LOOP
    INSERT INTO tmp_ViewDef(schemaname,viewname,schema_view,DDL) VALUES (row.schemaname,row.viewname,row.schema_view,row.DDL);
  END LOOP;
END;
$$ 
LANGUAGE plpgsql;

call operations.sp_GetViewDefinition();
*/
