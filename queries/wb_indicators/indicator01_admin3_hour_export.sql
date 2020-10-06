select pdate, hh as hour,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,total,totalimei,totalimsi 
from {provider_prefix}_indicator01_admin3_hour t1
where length(admin1)>0 order by pdate,hour,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code


