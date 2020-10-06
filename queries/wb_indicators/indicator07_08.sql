DROP TABLE IF EXISTS {provider_prefix}_indicator07_date_imei_distance;
DROP TABLE IF EXISTS {provider_prefix}_indicator07_firstcdr_daily;
DROP TABLE IF EXISTS {provider_prefix}_indicator07_date_imei_distance_first_obs;
DROP TABLE IF EXISTS {provider_prefix}_indicator07_date_imei_distance_first_obs_rank1;
DROP TABLE IF EXISTS {provider_prefix}_indicator07_date_imei_distance_combine_home;
DROP TABLE IF EXISTS {provider_prefix}_indicator07_date_home_admin3_result;
DROP TABLE IF EXISTS {provider_prefix}_indicator07_date_home_admin2_result;
DROP TABLE IF EXISTS {provider_prefix}_indicator08_week_home_admin3_result;
DROP TABLE IF EXISTS {provider_prefix}_indicator08_week_home_admin2_result;


create table {provider_prefix}_indicator07_date_imei_distance as 
select pdate,year(pdate) as yearr,weekofyear(pdate) as weekk,IMEI,
round(sum(GreatCircleDistance(LATITUDE,LONGITUDE,N_LATITUDE,N_LONGITUDE)),2) as distance_m,count(*) as totalpoints
from {provider_prefix}_indicator10_cdr_data_lead_trip 
where length(admin1) > 0 and length(N_admin1) > 0
group by pdate,IMEI;

create table {provider_prefix}_indicator07_firstcdr_daily (imei string,imsi string,call_datetime string,cell_id string,
LONGITUDE double,LATITUDE double,admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string, rank int)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator07_firstcdr_daily partition (pdate)  
select imei,imsi,call_datetime,cell_id,
longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,rank,pdate
from (select imei,imsi,call_datetime,cell_id,
             longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,pdate,
             row_number() over (partition by imei,pdate order by call_datetime asc) as rank
      from {provider_prefix}_indicator10_cdr_data_lead_trip t1
     ) t2
where rank = 1;

create table {provider_prefix}_indicator07_date_imei_distance_first_obs as 
select t1.pdate,year(t1.pdate) as yearr,weekofyear(t1.pdate) as weekk,t1.IMEI,t1.call_datetime,t1.LONGITUDE,t1.LATITUDE,
t2.pdate as P_pdate,t2.call_datetime as P_call_datetime,t2.LONGITUDE as P_LONGITUDE,t2.LATITUDE as P_LATITUDE,
round(GreatCircleDistance(t2.LATITUDE,t2.LONGITUDE,t1.LATITUDE,t1.LONGITUDE),2) as distance_m,
row_number() over (partition by t1.IMEI,t1.pdate order by t2.call_datetime desc) as rank
from {provider_prefix}_indicator07_firstcdr_daily t1 inner join {provider_prefix}_indicator06_lastcdr_daily t2 on t1.imei =t2.imei
where t2.pdate < t1.pdate  and datediff(t1.pdate, t2.pdate) <=7;

create table {provider_prefix}_indicator07_date_imei_distance_first_obs_rank1 as 
select * from {provider_prefix}_indicator07_date_imei_distance_first_obs where rank=1 and P_pdate is not null and length(P_call_datetime)>0;

create table {provider_prefix}_indicator07_date_imei_distance_combine_home as 
select t1.pdate,t1.yearr,t1.weekk,t1.imei,t1.distance_m,t1.totalpoints,
t2.distance_m as first_obs_distance_m,
nvl(t1.distance_m,0)+nvl(t2.distance_m, 0) as distance_m_sum,
if(length(t2.pdate)>0,t1.totalpoints+1,t1.totalpoints) as totalpoints_sum,
t3.admin1 as H_admin1,t3.admin1_code as H_admin1_code,t3.admin2 as H_admin2,t3.admin2_code as H_admin2_code,t3.admin3 as H_admin3,t3.admin3_code as H_admin3_code
from {provider_prefix}_indicator07_date_imei_distance t1 left outer join  {provider_prefix}_indicator07_date_imei_distance_first_obs_rank1 t2
on t1.pdate = t2.pdate and t1.imei =t2.imei inner join {provider_prefix}_indicator06_week_imei_rank1 t3 on t1.imei=t3.imei and t1.yearr=t3.yearr and  t1.weekk=t3.weekk;

create TABLE {provider_prefix}_indicator07_date_home_admin3_result as 
select pdate,H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code,
count(*) as total_uid,
round(min(cast(distance_m_sum as BIGINT)),2) as min_dist,
round(max(cast(distance_m_sum as BIGINT)),2) as max_dist,
round(avg(cast(distance_m_sum as BIGINT)),2) as avg_dist,
round(stddev_pop(cast(distance_m_sum as BIGINT)),2) as stddev,
percentile(cast(distance_m_sum as BIGINT), array(0.01,0.05,0.1,0.25,0.5,0.75,0.90,0.95,0.99)) as percentile_arr
from {provider_prefix}_indicator07_date_imei_distance_combine_home
group by pdate,H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code;

create TABLE {provider_prefix}_indicator07_date_home_admin2_result as 
select pdate,H_admin1,H_admin1_code,H_admin2,H_admin2_code,
count(*) as total_uid,
round(min(cast(distance_m_sum as BIGINT)),2) as min_dist,
round(max(cast(distance_m_sum as BIGINT)),2) as max_dist,
round(avg(cast(distance_m_sum as BIGINT)),2) as avg_dist,
round(stddev_pop(cast(distance_m_sum as BIGINT)),2) as stddev,
percentile(cast(distance_m_sum as BIGINT), array(0.01,0.05,0.1,0.25,0.5,0.75,0.90,0.95,0.99)) as percentile_arr
from {provider_prefix}_indicator07_date_imei_distance_combine_home
group by pdate,H_admin1,H_admin1_code,H_admin2,H_admin2_code;

create TABLE {provider_prefix}_indicator08_week_home_admin3_result as 
select yearr,weekk,min(pdate) as first_day_of_week,H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code,
count(*) as total_uid,
round(min(cast(distance_m_sum as BIGINT)),2) as min_dist,
round(max(cast(distance_m_sum as BIGINT)),2) as max_dist,
round(avg(cast(distance_m_sum as BIGINT)),2) as avg_dist,
round(stddev_pop(cast(distance_m_sum as BIGINT)),2) as stddev,
percentile(cast(distance_m_sum as BIGINT), array(0.01,0.05,0.1,0.25,0.5,0.75,0.90,0.95,0.99)) as percentile_arr
from {provider_prefix}_indicator07_date_imei_distance_combine_home
group by yearr,weekk,H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code;

create TABLE {provider_prefix}_indicator08_week_home_admin2_result as 
select yearr,weekk,min(pdate) as first_day_of_week,H_admin1,H_admin1_code,H_admin2,H_admin2_code,
count(*) as total_uid,
round(min(cast(distance_m_sum as BIGINT)),2) as min_dist,
round(max(cast(distance_m_sum as BIGINT)),2) as max_dist,
round(avg(cast(distance_m_sum as BIGINT)),2) as avg_dist,
round(stddev_pop(cast(distance_m_sum as BIGINT)),2) as stddev,
percentile(cast(distance_m_sum as BIGINT), array(0.01,0.05,0.1,0.25,0.5,0.75,0.90,0.95,0.99)) as percentile_arr
from {provider_prefix}_indicator07_date_imei_distance_combine_home
group by yearr,weekk,H_admin1,H_admin1_code,H_admin2,H_admin2_code;
