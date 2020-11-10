



with orders as (

	
		SELECT
		'Data Feeds' store_name,
		created_at,
		_id order_number,
		code discount_code,
		type discount_type,
		_sdc_sequence,
		first_value(_sdc_sequence) OVER (PARTITION BY order_number, _id ORDER BY _sdc_sequence DESC) lv
		FROM `dbt-projects.shopify_Data Feeds.orders` 
		cross join unnest(discount_codes)
	
	
	

)

SELECT
store_name,
order_number,
discount_code,
discount_type
FROM orders
where lv = _sdc_sequence

