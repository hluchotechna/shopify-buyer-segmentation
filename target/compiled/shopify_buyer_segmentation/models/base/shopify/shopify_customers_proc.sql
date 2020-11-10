-- depends_on: `dbt-projects`.`dbt_buyer_segmentation`.`stores_proc`





with customers as (

	
	SELECT
	'Data Feeds' store_name,
	'Shopify' as lookup_platform,
	created_at,
	id,
	first_name,
	last_name,
	email,
	_sdc_sequence,
	first_value(_sdc_sequence) over (partition by id order by _sdc_sequence desc) lv
	FROM `dbt-projects.shopify_Data Feeds.customers` 
	
	

)

SELECT
b.account,
b.store,
b.platform,
created_at,
id,
first_name,
last_name,
email
FROM customers a
LEFT JOIN `dbt-projects`.`dbt_buyer_segmentation`.`stores_proc` b 
ON ( a.store_name = b.store_name
  AND a.lookup_platform = b.platform )
where a.lv = a._sdc_sequence

