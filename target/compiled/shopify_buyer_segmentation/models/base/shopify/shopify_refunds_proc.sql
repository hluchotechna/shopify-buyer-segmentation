



with refunds as (

	
	SELECT
	'Data Feeds' store_name,
	_id order_number,
	checkout_id,
	financial_status,
	line_item_id,
	quantity,
	subtotal,
	line_item.variant_id variant_id,
	line_item.id refund_id,
 	_sdc_sequence
	FROM `dbt-projects.shopify_Data Feeds.orders` 
	cross join unnest(refunds), unnest(refund_line_items)
  	where financial_status like '%refund%'
	
	

)

SELECT * 
FROM 
	(
    SELECT
    store_name,
	order_number,
	checkout_id,
	financial_status,
	line_item_id,
	quantity,
	subtotal refund_amount,
	variant_id,
	refund_id,
 	_sdc_sequence,
    first_value(_sdc_sequence) OVER (PARTITION BY order_number, line_item_id ORDER BY _sdc_sequence DESC) lv
    FROM refunds
   	) 
WHERE lv = _sdc_sequence

