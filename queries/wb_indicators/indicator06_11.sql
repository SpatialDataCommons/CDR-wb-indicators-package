DROP TABLE IF EXISTS {provider_prefix}_indicator06_lastcdr_daily;
DROP TABLE IF EXISTS {provider_prefix}_indicator06_week_imei;
DROP TABLE IF EXISTS {provider_prefix}_indicator06_week_imei_rank1;
DROP TABLE IF EXISTS {provider_prefix}_indicator11_month_imei;
DROP TABLE IF EXISTS {provider_prefix}_indicator11_month_imei_rank1;

DROP TABLE IF EXISTS {provider_prefix}_indicator06_admin3_week;
DROP TABLE IF EXISTS {provider_prefix}_indicator06_admin2_week;
DROP TABLE IF EXISTS {provider_prefix}_indicator11_admin3_month;
DROP TABLE IF EXISTS {provider_prefix}_indicator11_admin2_month;

create table {provider_prefix}_indicator06_lastcdr_daily (imei string,imsi string,call_datetime string,cell_id string,longitude double,latitude double,
admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator06_lastcdr_daily partition (pdate)  
select imei,imsi,call_datetime,cell_id,longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,pdate
from (select t1.*, row_number() over (partition by imei,pdate order by call_datetime desc) as rank
      from {cdr_data_table} t1
     ) t2
where rank = 1;

create table {provider_prefix}_indicator06_week_imei (yearr int,weekk int,imei string,
admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string, totalcount bigint, maxdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator06_week_imei
select year(call_datetime) as yearr,weekofyear(call_datetime) as weekk,imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
count(*) as totalcount,max(call_datetime) as maxdate
from {provider_prefix}_indicator06_lastcdr_daily
group by year(call_datetime),weekofyear(call_datetime),imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

create table {provider_prefix}_indicator06_week_imei_rank1 as 
select yearr,weekk,imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,totalcount,maxdate,rank
from (select t1.*, row_number() over (partition by yearr,weekk,imei order by totalcount desc,maxdate desc) as rank
      from {provider_prefix}_indicator06_week_imei t1
     ) t2
where rank = 1;

create table {provider_prefix}_indicator11_month_imei (yearr int,monthh int,imei string,
admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string, totalcount bigint, maxdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator11_month_imei
select year(call_datetime) as yearr,month(call_datetime) as monthh,imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
count(*) as totalcount,max(call_datetime) as maxdate
from {provider_prefix}_indicator06_lastcdr_daily
group by year(call_datetime),month(call_datetime),imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

create table {provider_prefix}_indicator11_month_imei_rank1 as 
select yearr,monthh,imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,totalcount,maxdate,rank
from (select t1.*, row_number() over (partition by yearr,monthh,imei order by totalcount desc,maxdate desc) as rank
      from {provider_prefix}_indicator11_month_imei t1
     ) t2
where rank = 1;

create table {provider_prefix}_indicator06_admin3_week as 
select yearr,weekk,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code, count(*) as total
from {provider_prefix}_indicator06_week_imei_rank1 t1
group by yearr,weekk,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

create table {provider_prefix}_indicator06_admin2_week as 
select yearr,weekk,admin1,admin1_code,admin2,admin2_code, count(*) as total
from {provider_prefix}_indicator06_week_imei_rank1 t1
group by yearr,weekk,admin1,admin1_code,admin2,admin2_code;

create table {provider_prefix}_indicator11_admin3_month as 
select yearr,monthh,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code, count(*) as total
from {provider_prefix}_indicator11_month_imei_rank1 t1
group by yearr,monthh,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

create table {provider_prefix}_indicator11_admin2_month as 
select yearr,monthh,admin1,admin1_code,admin2,admin2_code, count(*) as total
from {provider_prefix}_indicator11_month_imei_rank1 t1 
group by yearr,monthh,admin1,admin1_code,admin2,admin2_code;


