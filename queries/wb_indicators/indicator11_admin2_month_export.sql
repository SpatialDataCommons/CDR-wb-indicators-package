select yearr,monthh,to_date(date_add(from_unixtime(unix_timestamp(concat(yearr,' ',monthh), 'yyyy M')),1)) as first_date
,admin1,admin1_code,admin2,admin2_code,total
from {provider_prefix}_indicator11_admin2_month
where length(admin1)>0 
order by yearr,monthh,first_date,admin1,admin1_code,admin2,admin2_code

