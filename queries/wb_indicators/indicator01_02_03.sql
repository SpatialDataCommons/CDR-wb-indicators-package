DROP TABLE IF EXISTS {provider_prefix}_indicator01_admin2_hour;
DROP TABLE IF EXISTS {provider_prefix}_indicator01_admin3_hour;
DROP TABLE IF EXISTS {provider_prefix}_indicator03_admin0_date;
DROP TABLE IF EXISTS {provider_prefix}_indicator03_admin1_date;
DROP TABLE IF EXISTS {provider_prefix}_indicator03_admin2_date;
DROP TABLE IF EXISTS {provider_prefix}_indicator03_admin3_date;

create table {provider_prefix}_indicator01_admin2_hour as 
select pdate,hour(call_datetime) as hh,admin1,admin1_code,admin2,admin2_code, count(*) as total, count(distinct IMEI) as totalimei, count(distinct IMSI) as totalimsi
from {cdr_data_table} t1
group by pdate,hour(call_datetime),admin1,admin1_code,admin2,admin2_code;

create table {provider_prefix}_indicator01_admin3_hour as 
select pdate,hour(call_datetime) as hh,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
count(*) as total, count(distinct IMEI) as totalimei,count(distinct IMSI) as totalimsi
from {cdr_data_table} t1
group by pdate,hour(call_datetime),admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

create table {provider_prefix}_indicator03_admin0_date as 
select pdate, count(*) as total, count(distinct IMEI) as totalimei, count(distinct IMSI) as totalimsi
from {cdr_data_table} t1
group by pdate;

create table {provider_prefix}_indicator03_admin1_date as 
select pdate,admin1,admin1_code, count(*) as total, count(distinct IMEI) as totalimei, count(distinct IMSI) as totalimsi
from {cdr_data_table} t1
group by pdate,admin1,admin1_code;

create table {provider_prefix}_indicator03_admin2_date as 
select pdate,admin1,admin1_code,admin2,admin2_code, count(*) as total, count(distinct IMEI) as totalimei, count(distinct IMSI) as totalimsi
from {cdr_data_table} t1
group by pdate,admin1,admin1_code,admin2,admin2_code;

create table {provider_prefix}_indicator03_admin3_date as 
select pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code, count(*) as total, count(distinct IMEI) as totalimei, count(distinct IMSI) as totalimsi
from {cdr_data_table} t1
group by pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code;

