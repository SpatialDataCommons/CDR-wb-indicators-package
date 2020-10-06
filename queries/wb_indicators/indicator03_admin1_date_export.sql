select pdate,admin1,admin1_code,total,totalimei,totalimsi 
from {provider_prefix}_indicator03_admin1_date 
where length(admin1)>0 
order by pdate,admin1,admin1_code
