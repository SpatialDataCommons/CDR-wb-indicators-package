select pdate,admin1,admin1_code,admin2,admin2_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code,
total_count,total_duration_origin,avg_duration_origin,stddev_duration_origin, 
total_duration_destination,avg_duration_destination,stddev_duration_destination 
from {provider_prefix}_indicator10_date_admin2_result 
where length(admin1)>0 and length(N_admin1)>0
order by pdate,admin1,admin1_code,admin2,admin2_code,N_admin1,N_admin1_code,N_admin2,N_admin2_code
