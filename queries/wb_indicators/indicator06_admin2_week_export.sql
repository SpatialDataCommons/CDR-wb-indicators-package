select yearr,weekk,to_date(date_add(from_unixtime(unix_timestamp(concat(yearr,' ',weekk), 'yyyy w')),1)) as first_week_date
,admin1,admin1_code,admin2,admin2_code,total
from {provider_prefix}_indicator06_admin2_week 
where length(admin1)>0 
order by yearr,weekk,first_week_date,admin1,admin1_code,admin2,admin2_code

