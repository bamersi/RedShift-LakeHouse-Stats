CREATE OR REPLACE procedure operations.sp_get_queries()
AS
$$
/*
This collects the query data from the cluster by doing a union of the archived and current query data
To assign a "query_id" to each query I am using row_number
For incremental runs, I get the max query_id and use that to increment

For full run:
Execution time: 4m 35s
*/

DECLARE 
  rec_MaxValues record;
BEGIN

----------------------------------------------------------------------------------------------------------------
--Collect max values from target table
----------------------------------------------------------------------------------------------------------------
select into rec_MaxValues 
        isnull(max(starttime),'19000101') as "last_starttime" 
        ,isnull(max(query_id),0)  as "max_query_id"  
from operations.sql_queries;

-----------------------------------------------------------------------------------------------------------------
--Make local copies to prevent errors due to data not existing on worker node
-----------------------------------------------------------------------------------------------------------------
drop table if exists tmp_query_current;
create temp table tmp_query_current as 
--Recent Queries
select  cast(querytxt as varchar(max)) as "querytxt"
        ,datediff(seconds,starttime,endtime) as "duration_second"
        ,starttime
        ,endtime        
        ,aborted
        ,userid        
from stl_query
where starttime>rec_MaxValues."last_starttime"
;

--This requires that historical queries be archived to a table called 'statistics.stl_query"
--drop table if exists tmp_query_hist;
--create temp table tmp_query_hist as 
--select  cast(querytxt as varchar(max)) as "querytxt"
--        ,datediff(seconds,starttime,endtime) as "duration_second"
--        ,starttime
--        ,endtime        
--        ,aborted
--        ,userid
--from statistics.stl_query
--where starttime>rec_MaxValues."last_starttime"
--;

drop table if exists tmp_user;
create temp table tmp_user as 
select  
      cast(usename as varchar) as "usename"
      ,usesysid
from pg_user
;

--------------------------------------------------------------------------------------------------      
--Final insert
--------------------------------------------------------------------------------------------------
insert into operations.sql_queries(query_id,userid,querytxt,usename,duration_second,starttime,endtime,aborted,created_at)
select 
      row_number() over (order by a.starttime,a.endtime,a.userid) + rec_MaxValues."max_query_id"
      ,a.userid
      ,cast(a.querytxt as varchar(max)) as "querytxt"
      ,isnull(cast(b.usename as varchar),'unknown') as "usename"
      ,datediff(seconds,a.starttime,a.endtime) as "duration_second"
      ,a.starttime
      ,a.endtime        
      ,a.aborted
      ,getdate()
from 
(
  select * from tmp_query_current
--  union
--  select * from tmp_query_hist
) a 
    left join tmp_user b on a.userid=b.usesysid 
;
END;
$$ 
LANGUAGE plpgsql
;

/*Testing
--truncate table operations.sql_queries;
call operations.sp_get_queries();
select convert_timezone('America/New_York',created_at) as "created_at_NY",min(starttime),max(startTime),min(query_id),max(query_id),count(1) from operations.sql_queries group by created_at order by 1;

select top 100 * from operations.sql_queries
select * from operations.sql_queries where query_id=16185285;
select * from operations.sql_queries where query_id=16185286;
*/




