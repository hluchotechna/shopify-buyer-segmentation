

  create or replace table `dbt-projects`.`dbt_buyer_segmentation`.`stores_proc`
  
  
  OPTIONS()
  as (
    select 
store,
store_name,
account,
platform,
max(time_of_entry) time_of_entry

from  ( 

SELECT  
store,
bigquery_name, 
name as store_name,
account,
platform,
time_of_entry,
first_value(time_of_entry) OVER (PARTITION BY store ORDER BY time_of_entry DESC) lv
FROM `dbt-projects.dbt_buyer_segmentation.data_feeds` 
where store_name != ''

) 

WHERE lv = time_of_entry
group by store, store_name, account, platform
  );
    