select pdate,admin1,admin1_code,admin2,admin2_code,H_admin1,H_admin1_code,H_admin2,H_admin2_code,
totalcount,round(mean_duration,2) as mean_duration,round(stdev_duration,2) as stdev_duration
from {provider_prefix}_indicator09_date_admin2_result 
where length(admin1)>0 and length(H_admin1)>0
order by pdate,admin1,admin1_code,admin2,admin2_code