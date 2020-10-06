select pdate,H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code,
total_uid ,round(max_dist/1000,2) as max_dist,round(min_dist/1000,2) as min_dist,round(percentile_arr[4]/1000,2) as median_dist,
round(avg_dist/1000,2) as avg_dist,round(stddev/1000,2) as stddev,
round(percentile_arr[0]/1000,2) as percentile_1,round(percentile_arr[1]/1000,2) as percentile_5,
round(percentile_arr[2]/1000,2) as percentile_10,round(percentile_arr[3]/1000,2) as percentile_25,round(percentile_arr[5]/1000,2) as percentile_75,
round(percentile_arr[6]/1000,2) as percentile_90,round(percentile_arr[7]/1000,2) as percentile_95,round(percentile_arr[8]/1000,2) as percentile_99
from {provider_prefix}_indicator07_date_home_admin3_result
where length(h_admin1)>0 
order by pdate,H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code
