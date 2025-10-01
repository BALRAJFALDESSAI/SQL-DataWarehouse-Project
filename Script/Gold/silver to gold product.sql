-- product data
select 
	row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from Silver.crm_prd_info pn
left join Silver.erp_px_cat_g1v2 pc on pn.cat_id = pc.id
where pn.prd_end_dt is null -- filter out all historical data and displaying only current data

-- cheching for duplicasy
select prd_key,count(*) from (
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pc.CAT,
pc.SUBCAT,
pc.MAINTENANCE
from Silver.crm_prd_info pn
left join Silver.erp_px_cat_g1v2 pc on pn.cat_id = pc.id
where pn.prd_end_dt is null -- filter out all historical data and displaying only current data
)t group by prd_key having count(*)>1


