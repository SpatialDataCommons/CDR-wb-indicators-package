select pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,total,totalimei,totalimsi 
from {provider_prefix}_indicator03_admin3_date 
where length(admin1)>0 
order by pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code
