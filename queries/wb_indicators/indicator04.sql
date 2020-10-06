DROP TABLE IF EXISTS {provider_prefix}_indicator04_lastcdr_daily;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_baseline_imei;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_baseline_imei_rank1;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_admin3_baseline;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_admin3_active;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_admin3_day;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_admin2_baseline;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_admin2_active;
DROP TABLE IF EXISTS {provider_prefix}_indicator04_admin2_day;


create table {provider_prefix}_indicator04_lastcdr_daily (imei string,imsi string,call_datetime string,cell_id string,longitude double,latitude double,
admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator04_lastcdr_daily partition (pdate)  
select imei,imsi,call_datetime,cell_id,longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,pdate
from (select t1.*, row_number() over (partition by imei,pdate order by call_datetime desc) as rank
      from {cdr_data_table} t1
     ) t2
where rank = 1;

create table {provider_prefix}_indicator04_baseline_imei (imei string,admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string,
totalcount bigint, maxdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator04_baseline_imei
select imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
count(*) as totalcount,max(call_datetime) as maxdate
from {provider_prefix}_indicator04_lastcdr_daily
where pdate>='{baseline_start_date}' and pdate <='{baseline_end_date}'
group by imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

create table {provider_prefix}_indicator04_baseline_imei_rank1 as 
select imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,totalcount,maxdate,rank
from (select t1.*, row_number() over (partition by IMEI order by totalcount desc,maxdate desc) as rank
      from {provider_prefix}_indicator04_baseline_imei t1
     ) t2
where rank = 1;

create table {provider_prefix}_indicator04_admin3_baseline as 
select admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,count(*) as base_totalcount
from {provider_prefix}_indicator04_baseline_imei_rank1
group by admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

create table {provider_prefix}_indicator04_admin3_active as 
select pdate,t2.admin1,t2.admin1_code,t2.admin2,t2.admin2_code,t2.admin3,t2.admin3_code,count(*) as totalcount
from {provider_prefix}_indicator04_lastcdr_daily t1 inner join {provider_prefix}_indicator04_baseline_imei_rank1 t2 on t1.imei = t2.imei
group by pdate,t2.admin1,t2.admin1_code,t2.admin2,t2.admin2_code,t2.admin3,t2.admin3_code;

create table {provider_prefix}_indicator04_admin3_day as 
select pdate,t4.admin1,t4.admin1_code,t4.admin2,t4.admin2_code,t4.admin3,t4.admin3_code,totalcount,base_totalcount,
round(totalcount/base_totalcount,2) as ratio,round((totalcount*100)/base_totalcount,2) as ratio_per
from {provider_prefix}_indicator04_admin3_baseline t4 inner join {provider_prefix}_indicator04_admin3_active t3
on t4.admin1=t3.admin1 and t4.admin1_code=t3.admin1_code and t4.admin2=t3.admin2 and t4.admin2_code=t3.admin2_code and t4.admin3=t3.admin3 and t4.admin3_code=t3.admin3_code
where length(t3.admin1)>0 and t3.admin1 is not null;

create table {provider_prefix}_indicator04_admin2_baseline as 
select admin1,admin1_code,admin2,admin2_code,count(*) as base_totalcount
from {provider_prefix}_indicator04_baseline_imei_rank1
group by admin1,admin1_code,admin2,admin2_code;

create table {provider_prefix}_indicator04_admin2_active as 
select pdate,t2.admin1,t2.admin1_code,t2.admin2,t2.admin2_code,count(*) as totalcount
from {provider_prefix}_indicator04_lastcdr_daily t1 inner join {provider_prefix}_indicator04_baseline_imei_rank1 t2 on t1.imei = t2.imei
group by pdate,t2.admin1,t2.admin1_code,t2.admin2,t2.admin2_code;

create table {provider_prefix}_indicator04_admin2_day as 
select pdate,t4.admin1,t4.admin1_code,t4.admin2,t4.admin2_code,totalcount,base_totalcount,
round(totalcount/base_totalcount,2) as ratio,round((totalcount*100)/base_totalcount,2) as ratio_per
from {provider_prefix}_indicator04_admin2_baseline t4 inner join {provider_prefix}_indicator04_admin2_active t3
on t4.admin1=t3.admin1 and t4.admin1_code=t3.admin1_code and t4.admin2=t3.admin2 and t4.admin2_code=t3.admin2_code
where length(t3.admin1)>0 and t3.admin1 is not null;
