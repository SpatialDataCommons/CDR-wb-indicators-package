select pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,totalcount,base_totalcount,ratio,ratio_per
from {provider_prefix}_indicator04_admin3_day
where length(admin1)>0 
order by pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code
