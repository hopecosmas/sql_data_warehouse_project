-- (1) Here is the query that is transforming bronze.crm_cust_info by removing the duplicate key values

-- first check the duplicate
SELECT 
*,
ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info

-- Now duplicate will have flag_part > 1, thus check that and keep the no duplicate values

SELECT
*
FROM (
SELECT 
*,
ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) t WHERE flag_last = 1 ;


-- (2) Check for Unwanted Spaces
-- Expectation: No Result

/* 
Note: If the original value is not equal to the same value after trimming, it means
there are spaces!
*/

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);


-- let's trim them

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) t WHERE flag_last = 1 ;


-- (3) Data Standardization & Consistency

-- check the gender column
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- check the marital status and do the same

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

-- Let's say in our data warehouse, we aim to store clear and meaningful values rather than abbreviated terms.

SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'   --UPPER() just in case mixed=case values appear later in your column
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'   --UPPER() just in case mixed=case values appear later in your column
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) t WHERE flag_last = 1 ;




-- Now we're gonna Insert into Silver layer

INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)


SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'   --UPPER() just in case mixed=case values appear later in your column
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     ELSE 'n/a'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr))= 'F' THEN 'Female'   --UPPER() just in case mixed=case values appear later in your column
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER() OVER ( PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
) t WHERE flag_last = 1 ;


