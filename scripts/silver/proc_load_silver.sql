/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		*/
-- Loading silver.crm_cust_info
insert into silver.crm_cust_info(
   cst_id,
   cst_key,
   cst_firstname,
   cst_last_name,
   cst_maritalstatus,
   cst_gndr,
   cst_create_date)
SELECT 
cst_id,
       cst_key,
       trim(cst_firstname) as cst_firstname,
       trim(cst_last_name) as cst_lastname,
       case when upper(trim(cst_maritalstatus)) = 'S' then 'Single'
            when upper(trim(cst_maritalstatus)) = 'M' then 'Married'
            else 'n/a'
        end cst_maritalstatus,
       case when upper(trim(cst_gndr)) = 'F' then 'Female'
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            else 'n/a'
        end cst_gndr,
       cst_create_date
FROM(
SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC  ) AS flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    )t  WHERE flag = 1



-- Loading silver.crm_prd_info
insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
select
   prd_id,
   substring(prd_key , 7 , len(prd_key) ) as prd_key,  -- Extract product key
   replace(substring(prd_key, 1 , 5), '-', '_') as cat_id,  -- Extract category ID
   prd_nm,
   isnull(prd_cost, 0) as prd_cost,
   case when upper(trim(prd_line)) = 'R' then 'Road'
        when upper(trim(prd_line)) = 'M' then 'Mountain'
        when upper(trim(prd_line)) = 'S' then 'Other Sales'
        when upper(trim(prd_line)) = 't' then 'Tourin'
        else 'n/a'
   end as prd_line,  -- Map product line codes to descriptive values
   cast(prd_start_dt as date) as prd_start_dt,
			cast(
				lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1
				as date
			) as prd_end_dt   -- Calculate end date as one day before the next start date
from bronze.crm_prd_info
