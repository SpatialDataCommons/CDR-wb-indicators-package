DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_imei_admin2_od;
DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_imei_admin3_od;
DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_imei_admin2_od_prev;
DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_imei_admin3_od_prev;
DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_imei_admin2_od_prev_1;
DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_imei_admin3_od_prev_1;
DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_admin2_result;
DROP TABLE IF EXISTS {provider_prefix}_indicator05_date_admin3_result;

create table {provider_prefix}_indicator05_date_imei_admin2_od (imei string,tripid_admin2 int,N_tripid_admin2 int,starttime string,N_starttime string,totalpoints int,N_totalpoints int,
admin1 string,admin1_code string,admin2 string,admin2_code string,
N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator05_date_imei_admin2_od partition (pdate)
select t1.imei,t1.tripid_admin2,t2.tripid_admin2 as N_tripid_admin2,t1.starttime,t2.starttime as N_starttime,t1.totalpoints,t2.totalpoints as N_totalpoints,
t1.admin1,t1.admin1_code,t1.admin2,t1.admin2_code,
t2.admin1 as N_admin1,t2.admin1_code as N_admin1_code,t2.admin2 as N_admin2,t2.admin2_code as N_admin2_code,t1.pdate
from {provider_prefix}_indicator10_date_imei_admin2 t1 inner join {provider_prefix}_indicator10_date_imei_admin2 t2 on t1.pdate=t2.pdate and t1.imei = t2.imei
where t1.tripid_admin2<t2.tripid_admin2;

create table {provider_prefix}_indicator05_date_imei_admin3_od (imei string,tripid_admin3 int,N_tripid_admin3 int,starttime string,N_starttime string,totalpoints int,N_totalpoints int,
admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string,
N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string,N_admin3 string,N_admin3_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator05_date_imei_admin3_od partition (pdate)
select t1.imei,t1.tripid_admin3,t2.tripid_admin3 as N_tripid_admin3,t1.starttime,t2.starttime as N_starttime,t1.totalpoints,t2.totalpoints as N_totalpoints,
t1.admin1,t1.admin1_code,t1.admin2,t1.admin2_code,t1.admin3,t1.admin3_code,
t2.admin1 as N_admin1,t2.admin1_code as N_admin1_code,t2.admin2 as N_admin2,t2.admin2_code as N_admin2_code,t2.admin3 as N_admin3,t2.admin3_code as N_admin3_code,t1.pdate
from {provider_prefix}_indicator10_date_imei_admin3 t1 inner join {provider_prefix}_indicator10_date_imei_admin3 t2 on t1.pdate=t2.pdate and t1.imei = t2.imei
where t1.tripid_admin3<t2.tripid_admin3;

create table {provider_prefix}_indicator05_date_imei_admin2_od_prev as 
select pdate,imei,tripid_admin2,starttime,admin1,admin1_code,admin2,admin2_code,
lag(pdate) over (partition by imei order by pdate asc,tripid_admin2 asc) as P_pdate,
lag(tripid_admin2) over (partition by imei order by pdate asc,tripid_admin2 asc) as P_tripid_admin2,
lag(starttime) over (partition by imei order by pdate asc,tripid_admin2 asc) as P_starttime,
lag(admin1) over (partition by imei order by pdate asc,tripid_admin2 asc) as P_admin1,
lag(admin1_code) over (partition by imei order by pdate asc,tripid_admin2 asc) as P_admin1_code,
lag(admin2) over (partition by imei order by pdate asc,tripid_admin2 asc) as P_admin2,
lag(admin2_code) over (partition by imei order by pdate asc,tripid_admin2 asc) as P_admin2_code
from {provider_prefix}_indicator10_date_imei_admin2;

create table {provider_prefix}_indicator05_date_imei_admin3_od_prev as 
select pdate,imei,tripid_admin3,starttime,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,
lag(pdate) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_pdate,
lag(tripid_admin3) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_tripid_admin3,
lag(starttime) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_starttime,
lag(admin1) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_admin1,
lag(admin1_code) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_admin1_code,
lag(admin2) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_admin2,
lag(admin2_code) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_admin2_code,
lag(admin3) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_admin3,
lag(admin3_code) over (partition by imei order by pdate asc,tripid_admin3 asc) as P_admin3_code
from {provider_prefix}_indicator10_date_imei_admin3;

create table {provider_prefix}_indicator05_date_imei_admin2_od_prev_1 as 
select * from {provider_prefix}_indicator05_date_imei_admin2_od_prev
where tripid_admin2=1 and P_tripid_admin2 is not null and datediff(pdate, P_pdate) <=7;

create table {provider_prefix}_indicator05_date_imei_admin3_od_prev_1 as 
select * from {provider_prefix}_indicator05_date_imei_admin3_od_prev
where tripid_admin3=1 and P_tripid_admin3 is not null and datediff(pdate, P_pdate) <=7;;

create table {provider_prefix}_indicator05_date_admin2_result (pdate string, admin1 string,admin1_code string,admin2 string,admin2_code string,
N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string,totalimei int,totalOD int,totalFO int,totalcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

create table {provider_prefix}_indicator05_date_admin3_result (pdate string, admin1 string,admin1_code string,admin2 string,admin2_code string,admin3 string,admin3_code string,
N_admin1 string,N_admin1_code string,N_admin2 string,N_admin2_code string,N_admin3 string,N_admin3_code string,totalimei int,totalOD int,totalFO int,totalcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;

INSERT OVERWRITE table {provider_prefix}_indicator05_date_admin2_result
select pdate,admin1,admin1_code,admin2,admin2_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,
       count(distinct imei) as totalimei, sum(if(odtype='OD',1,0)) as totalOD,sum(if(odtype='FO',1,0)) as totalFO, count(*) as totalcount
from (select 'OD' as odtype,imei, pdate,admin1,admin1_code,admin2,admin2_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code
      from {provider_prefix}_indicator05_date_imei_admin2_od
      UNION ALL
      select 'FO' as odtype,imei, pdate,P_admin1 as admin1,P_admin1_code as admin1_code,P_admin2 as admin2,P_admin2_code as admin2_code,
      admin1 as N_admin1,admin1_code as N_admin1_code,admin2 as N_admin2,admin2_code as N_admin2_code 
      from {provider_prefix}_indicator05_date_imei_admin2_od_prev_1
     ) as t1
group by pdate,admin1,admin1_code,admin2,admin2_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code;


INSERT OVERWRITE table {provider_prefix}_indicator05_date_admin3_result
select pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code,
       count(distinct IMEI) as totalimei, sum(if(odtype='OD',1,0)) as totalOD,sum(if(odtype='FO',1,0)) as totalFO,count(*) as totalcount
from (select 'OD' as odtype,IMEI, pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code
      from {provider_prefix}_indicator05_date_imei_admin3_od
      UNION ALL
      select 'FO' as odtype,IMEI, pdate,P_admin1 as admin1,P_admin1_code as admin1_code,P_admin2 as admin2,P_admin2_code as admin2_code,P_admin3 as admin3,P_admin3_code as admin3_code,
      admin1 as N_admin1,admin1_code as N_admin1_code,admin2 as N_admin2,admin2_code as N_admin2_code,admin3 as N_admin3,admin3_code as N_admin3_code
      from {provider_prefix}_indicator05_date_imei_admin3_od_prev_1
     ) as t1
group by pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code;

