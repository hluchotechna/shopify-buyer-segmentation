-- depends_on: `dbt-projects`.`dbt_buyer_segmentation`.`stores_proc`,`dbt-projects`.`dbt_buyer_segmentation`.`mappings_ga_proc`




with ga_report as (

	    
	    	
		   	SELECT
		   	'Data Feeds' as store_name,
		   	'Google Analytics' as lookup_platform,
			lower(trim(regexp_replace(replace(replace(replace(replace(CONCAT(hostname,landingpagepath),'www.',''),'http://',''),'https://',''),'.html',''),r'\?.*$',''),'/')) as url,
			cast(date as date) date,
			lower(source) source,
			lower(medium) medium,
			lower(replace(replace(replace(campaign,' ', ''),'-',''),'_','')) campaign,
			cast(regexp_replace(transactionid, r'#|B', '') as int64) transactionid,
			_sdc_sequence,
			first_value(_sdc_sequence) OVER (PARTITION BY hostname, landingpagepath, date, source, medium, transactionid ORDER BY _sdc_sequence DESC) lv
			FROM `dbt-projects.ga_Data Feeds.report` 

		    
	   

)


SELECT  
date,
b.store store,
c.source source,
c.medium medium,
a.campaign campaign,
concat(a.source, ' / ', a.medium) source_medium,  
case when c.platform is null then "Unmapped" else c.platform end as platform,
case when c.channel is null then "Unmapped" else c.channel end as channel,
url,
transactionid
FROM (

	SELECT 
	store_name,
	lookup_platform,
	date,
	transactionid,
	max(url) url,
	max(source) source,
	max(medium) medium,
	max(campaign) campaign
	FROM ga_report
	where lv = _sdc_sequence
	group by store_name, lookup_platform, date, transactionid

) a
LEFT JOIN `dbt-projects`.`dbt_buyer_segmentation`.`stores_proc` b 
ON ( a.store_name = b.store_name 
	AND a.lookup_platform = b.platform )
LEFT JOIN `dbt-projects`.`dbt_buyer_segmentation`.`mappings_ga_proc` c
ON ( a.source = c.source
  AND a.medium = c.medium 
  AND a.store_name = c.store_name )

