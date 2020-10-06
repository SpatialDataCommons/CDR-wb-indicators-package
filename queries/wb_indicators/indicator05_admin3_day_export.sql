select pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code,
totalimei,totalOD,totalFO,totalcount
from {provider_prefix}_indicator05_date_admin3_result 
where length(admin1)>0 and length(N_admin1)>0
order by pdate,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code
