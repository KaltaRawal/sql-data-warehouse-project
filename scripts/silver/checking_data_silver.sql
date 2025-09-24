/* Cleaning  data to store in silver layer */

/*
=============================================================================
handling duplicate or null values in the cust_info  table
=============================================================================
*/
select 
cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or count(*) is null
/* using window function to handle duplicate or null values */
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM bronze.crm_cust_info
    where flag = 1
/*  to check duplicate values */
SELECT * 
FROM(
SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC  ) AS flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    )t  WHERE flag != 1
/* to check unique values in primary key */
    SELECT * 
FROM(
SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC  ) AS flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    )t  WHERE flag =1

/*
=================================================================
for checking any unwanted spaces in string collums
=================================================================
*/

/*check for unwanted spaces */
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_last_name
from bronze.crm_cust_info
where cst_last_name != trim(cst_last_name)

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)

select cst_maritalstatus
from bronze.crm_cust_info
where cst_maritalstatus != trim(cst_maritalstatus)

/* found unwanted spaces in first and last name
-- removing unwanted spaces in first and last name*/
SELECT cst_id,
       cst_key,
       trim(cst_firstname) as cst_firstname,
       trim(cst_last_name) as cst_lastname,
       cst_maritalstatus,
       cst_gndr,
       cst_create_date
  FROM bronze.crm_cust_info

/*check the consitency for low cardinality colums
-- data standardization & consitency */
select distinct cst_gndr
from silver.crm_cust_info

select distinct cst_maritalstatus
from bronze.crm_cust_info

/*==================================================
    CLEANING IN crm_prd_info FILE *
    ===================================================*/
-- check for null or duplicates in primary key
select 
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null
select * from bronze.crm_prd_info
-- check fo unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

-- check for null or negative no is cost
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- data standardization and consistency(checking for low cardinality)
select distinct prd_line
from bronze.crm_prd_info

-- check for invalid date oredrs
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt
