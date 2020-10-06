DROP TABLE IF EXISTS {provider_prefix}_indicator09_cdr_data_lead;
DROP TABLE IF EXISTS {provider_prefix}_indicator09_date_imei_admin2;
DROP TABLE IF EXISTS {provider_prefix}_indicator09_date_imei_admin2_home;
DROP TABLE IF EXISTS {provider_prefix}_indicator09_date_admin2_result;

create table {provider_prefix}_indicator09_cdr_data_lead (imei string,imsi string,call_datetime string,cell_id string,
longitude double,latitude double,admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string,
N_longitude double,N_latitude double,N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string,N_admin3 string,N_admin3_code string,N_call_datetime string,N_min double)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator09_cdr_data_lead partition (pdate)  
select imei,imsi,call_datetime,cell_id,
longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
lead(longitude) over (partition by pdate,imei order by call_datetime asc) as N_longitude,
lead(latitude) over (partition by pdate,imei order by call_datetime asc) as N_latitude,
lead(admin1) over (partition by pdate,imei order by call_datetime asc) as N_admin1,
lead(admin1_code) over (partition by pdate,imei order by call_datetime asc) as N_admin1_code,
lead(admin2) over (partition by pdate,imei order by call_datetime asc) as N_admin2,
lead(admin2_code) over (partition by pdate,imei order by call_datetime asc) as N_admin2_code,
lead(admin3) over (partition by pdate,imei order by call_datetime asc) as N_admin3,
lead(admin3_code) over (partition by pdate,imei order by call_datetime asc) as N_admin3_code,
lead(call_datetime) over (partition by pdate,imei order by call_datetime asc) as N_call_datetime,
round((unix_timestamp(lead(call_datetime) over (partition by pdate,imei order by call_datetime asc)) - unix_timestamp(call_datetime))/60,2) as N_min,
pdate
from {cdr_data_table};

create table {provider_prefix}_indicator09_date_imei_admin2 (yearr int,weekk int,imei string,
admin1 string,admin1_code string,admin2 string,admin2_code string,totalpoint bigint,totalstay double)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator09_date_imei_admin2 partition (pdate)  
select  year(pdate) as yearr,weekofyear(pdate) as weekk,imei,admin1,admin1_code,admin2,admin2_code,count(*) as totalpoints, sum(N_min) as totalstay,pdate
from {provider_prefix}_indicator09_cdr_data_lead 
group by pdate,imei,admin1,admin1_code,admin2,admin2_code;

create table {provider_prefix}_indicator09_date_imei_admin2_home (yearr int,weekk int,imei string,admin1 string,admin1_code string,admin2 string,admin2_code string,
H_admin1 string,H_admin1_code string,H_admin2 string,H_admin2_code string,
totalpoint bigint,totalstay double)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator09_date_imei_admin2_home partition (pdate)
select t1.yearr,t1.weekk,t1.imei,t1.admin1,t1.admin1_code,t1.admin2,t1.admin2_code,
t2.admin1 as H_admin1,t2.admin1_code as H_admin1_code,t2.admin2 as H_admin2,t2.admin2_code as H_admin2_code,
t1.totalpoint,t1.totalstay,pdate
from {provider_prefix}_indicator09_date_imei_admin2 t1 inner join {provider_prefix}_indicator06_week_imei_rank1 t2 on t1.imei=t2.imei and t1.yearr=t2.yearr and  t1.weekk=t2.weekk;

create table {provider_prefix}_indicator09_date_admin2_result as 
select pdate,admin1,admin1_code,admin2,admin2_code,H_admin1,H_admin1_code,H_admin2,H_admin2_code,
count(*) as totalcount,avg(totalstay) as mean_duration,stddev_pop(totalstay) as stdev_duration
from {provider_prefix}_indicator09_date_imei_admin2_home t1
group by pdate,admin1,admin1_code,admin2,admin2_code,H_admin1,H_admin1_code,H_admin2,H_admin2_code;
