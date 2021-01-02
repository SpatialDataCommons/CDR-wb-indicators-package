DROP TABLE IF EXISTS {provider_prefix}_indicator10_cdr_data_lead_trip;
DROP TABLE IF EXISTS {provider_prefix}_indicator10_date_imei_admin3;
DROP TABLE IF EXISTS {provider_prefix}_indicator10_date_imei_admin3_od;
DROP TABLE IF EXISTS {provider_prefix}_indicator10_date_admin3_result;
DROP TABLE IF EXISTS {provider_prefix}_indicator10_date_imei_admin2;
DROP TABLE IF EXISTS {provider_prefix}_indicator10_date_imei_admin2_od;
DROP TABLE IF EXISTS {provider_prefix}_indicator10_date_admin2_result;

create table {provider_prefix}_indicator10_cdr_data_lead_trip (imei string,imsi string,call_datetime string,cell_id string,
longitude double,latitude double,admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string,
N_longitude double,N_latitude double,N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string,N_admin3 string,N_admin3_code string,N_call_datetime string,N_min double,
tripid_admin3 int,tripid_admin2 int)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator10_cdr_data_lead_trip partition (pdate)
select t1.imei,imsi,call_datetime,cell_id,
longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
N_longitude,N_latitude,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code,N_call_datetime,N_min,
sum(if(admin3_code=lag(admin3_code),0,1)) over (partition by pdate,t1.imei order by call_datetime asc) as tripid_admin3,
sum(if(admin2_code=lag(admin2_code),0,1)) over (partition by pdate,t1.imei order by call_datetime asc)  as tripid_admin2,
pdate
from {provider_prefix}_indicator09_cdr_data_lead t1;

create table {provider_prefix}_indicator10_date_imei_admin3 (imei string,tripid_admin3 int,starttime string,endtime string,todaystay double,totalpoints int,
admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator10_date_imei_admin3 partition (pdate)
select imei,tripid_admin3,min(call_datetime) as starttime,max(call_datetime) as endtime,sum(N_min) as todaystay,count(*) totalpoints,
admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,pdate
from {provider_prefix}_indicator10_cdr_data_lead_trip
group by pdate,imei,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,tripid_admin3;

create table {provider_prefix}_indicator10_date_imei_admin3_od (imei string,tripid_admin3 int,starttime string,endtime string,todaystay double,totalpoints int,
admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string,
N_tripid_admin3 int,N_starttime string,N_endtime string,N_todaystay double,N_totalpoints int,
N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string,N_admin3 string,N_admin3_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator10_date_imei_admin3_od partition (pdate)
select imei,tripid_admin3,starttime,endtime,todaystay,totalpoints,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
lead(tripid_admin3) over (partition by pdate,imei order by tripid_admin3 asc) as N_tripid_admin3,
lead(starttime) over (partition by pdate,imei order by tripid_admin3 asc) as N_starttime,
lead(endtime) over (partition by pdate,imei order by tripid_admin3 asc) as N_endtime,
lead(todaystay) over (partition by pdate,imei order by tripid_admin3 asc) as N_todaystay,
lead(totalpoints) over (partition by pdate,imei order by tripid_admin3 asc) as N_totalpoints,
lead(admin1) over (partition by pdate,imei order by tripid_admin3 asc) as N_admin1,
lead(admin1_code) over (partition by pdate,imei order by tripid_admin3 asc) as N_admin1_code,
lead(admin2) over (partition by pdate,imei order by tripid_admin3 asc) as N_admin2,
lead(admin2_code) over (partition by pdate,imei order by tripid_admin3 asc) as N_admin2_code,
lead(admin3) over (partition by pdate,imei order by tripid_admin3 asc) as N_admin3,
lead(admin3_code) over (partition by pdate,imei order by tripid_admin3 asc) as N_admin3_code,
pdate
from {provider_prefix}_indicator10_date_imei_admin3;

create table {provider_prefix}_indicator10_date_admin3_result as 
select pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code,
count(*) as total_count,
round(sum(todaystay),2) as total_duration_origin,
round(avg(todaystay),2) as avg_duration_origin,
round(stddev_pop(todaystay),2) as stddev_duration_origin,
round(sum(N_todaystay),2) as total_duration_destination,
round(avg(N_todaystay),2) as avg_duration_destination,
round(stddev_pop(N_todaystay),2) as stddev_duration_destination
from {provider_prefix}_indicator10_date_imei_admin3_od
where length(admin1)>0 and length(N_admin1)>0
group by pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code;

create table {provider_prefix}_indicator10_date_imei_admin2 (imei string,tripid_admin2 int,starttime string,endtime string,todaystay double,totalpoints int,
admin1 string,admin1_code string,admin2 string,admin2_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator10_date_imei_admin2 partition (pdate)
select imei,tripid_admin2,min(call_datetime) as starttime,max(call_datetime) as endtime,sum(N_min) as todaystay,count(*) totalpoints,
admin1,admin1_code,admin2,admin2_code,pdate
from {provider_prefix}_indicator10_cdr_data_lead_trip
group by pdate,imei,admin1,admin1_code,admin2,admin2_code,tripid_admin2;

create table {provider_prefix}_indicator10_date_imei_admin2_od (imei string,tripid_admin2 int,starttime string,endtime string,todaystay double,totalpoints int,
admin1 string,admin1_code string,admin2 string,admin2_code string,
N_tripid_admin2 int,N_starttime string,N_endtime string,N_todaystay double,N_totalpoints int,
N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator10_date_imei_admin2_od partition (pdate)
select IMEI,tripid_admin2,starttime,endtime,todaystay,totalpoints,admin1,admin1_code,admin2,admin2_code,
lead(tripid_admin2) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_tripid_admin2,
lead(starttime) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_starttime,
lead(endtime) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_endtime,
lead(todaystay) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_todaystay,
lead(totalpoints) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_totalpoints,
lead(admin1) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_admin1,
lead(admin1_code) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_admin1_code,
lead(admin2) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_admin2,
lead(admin2_code) over (partition by pdate,IMEI order by tripid_admin2 asc) as N_admin2_code,
pdate
from {provider_prefix}_indicator10_date_imei_admin2;

create table {provider_prefix}_indicator10_date_admin2_result as 
select pdate,admin1,admin1_code,admin2,admin2_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,
count(*) as total_count,
round(sum(todaystay),2) as total_duration_origin,
round(avg(todaystay),2) as avg_duration_origin,
round(stddev_pop(todaystay),2) as stddev_duration_origin,
round(sum(N_todaystay),2) as total_duration_destination,
round(avg(N_todaystay),2) as avg_duration_destination,
round(stddev_pop(N_todaystay),2) as stddev_duration_destination
from {provider_prefix}_indicator10_date_imei_admin2_od
where length(admin1)>0 and length(N_admin1)>0
group by pdate,admin1,admin1_code,admin2,admin2_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code;



