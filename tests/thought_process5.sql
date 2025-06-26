-- Next we move to ERM systems and do data check and Transformations if needed on each column


SELECT
*
FROM bronze.erp_cust_az12

SELECT * FROM [silver].[crm_cust_info]

-- cid column is connected to cst_key column in crm.cust_info

-- Clean the "cid" column by removing unnecessary characters

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12


-- To check, compare it to the cst_key in silver.crm_cust_info table

SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END NOT In (SELECT DISTINCT cst_key FROM silver.crm_cust_info) -- it's working!!

-- check the birthdate column, 
-- if the bdate > current date, that's wrong

SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate > GETDATE()


-- Make the changes

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END bdate,
gen
FROM bronze.erp_cust_az12



-- Move to the next column "gen"

-- check for Data standardization & consistency

SELECT DISTINCT gen
FROM bronze.erp_cust_az12

-- not looking good.

-- clean to make sure we have: Male, Female and "n/a"

SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
     WHEN UPPER(TRIM(gen)) LIKE 'M%'  THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12 

-- Update our original qurery


SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove "NAS" prefix if present
     ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL -- Set future birthdates to NULL
     ELSE bdate
END AS bdate, 
CASE WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'  -- Normalize gender values and handle unknown cases
     WHEN UPPER(TRIM(gen)) LIKE 'M%'  THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12 



-- Let's Insert our new update into silver.erp_cust_az12 table ( we did not change any column data type so we're good)

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
     WHEN UPPER(TRIM(gen)) LIKE 'M%'  THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12 

--  Quality check in silver table

-- Data Standardization & Consistency

--for date
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

--for gender

SELECT DISTINCT 
gen
FROM silver.erp_cust_az12


-- Final table look

SELECT
*
FROM silver.erp_cust_az12





