-- Empolyee Table
select 
	ROW_NUMBER() over (order_by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- cst_gen is mastr for gender info
		 else Coalesce(ca.GEN,'n/a')
	end as gender,
	ca.BDATE as birthdate,
	ci.cst_create_date as create_date
from Silver.crm_cust_info ci
left join Silver.erp_cust_AZ12 ca on ci.cst_key=ca.cid
left join Silver.erp_loc_A101 la on ci.cst_key=la.cid

select * from Silver.erp_cust_AZ12
Select * from Silver.erp_loc_A101

-- check for duplicate
Select cst_id,count(*) from(
	select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ci.dwh_create_date,
	ca.BDATE,
	ca.GEN,
	la.CNTRY
	from Silver.crm_cust_info ci
	left join Silver.erp_cust_AZ12 ca on ci.cst_key=ca.cid
	left join Silver.erp_loc_A101 la on ci.cst_key=la.cid
)t group by cst_id having count(*) > 1

-- checking two sources of gender
select distinct
ci.cst_gndr,
ca.GEN,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- cst_gen is mastr for gender info
	 else Coalesce(ca.GEN,'n/a')
end as new_gen
from Silver.crm_cust_info ci
left join Silver.erp_cust_AZ12 ca on ci.cst_key=ca.cid
left join Silver.erp_loc_A101 la on ci.cst_key=la.cid order by 1,2
