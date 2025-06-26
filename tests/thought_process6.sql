-- Now, let's check the location table

SELECT
*
FROM bronze.erp_loc_a101

-- cid is connected to cst_key in cust-info

SELECT cst_key FROM silver.crm_cust_info

-- remove the "-" sign in cid and test

SELECT
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)  -- it's working!, no error found



-- check the next column

SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101
ORDER BY cntry

-- do some transformation

SELECT
REPLACE(cid, '-', '') cid,
cntry,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(cntry)
END AS cntry2
FROM bronze.erp_loc_a101

-- Our values have "newline character (\n)" or "carriage return (\r)" which sneaked in during data imports, let's remove them too


SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(clean_cntry) = 'DE' THEN 'Germany'
        WHEN UPPER(clean_cntry) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(clean_cntry) = '' OR clean_cntry IS NULL THEN 'n/a'
        ELSE clean_cntry
    END AS cntry
FROM (
    SELECT 
        cid,
        cntry,
        TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) AS clean_cntry
    FROM bronze.erp_loc_a101
) AS sub


-- Let's check


SELECT DISTINCT 
cntry AS old_cntry,
CASE 
        WHEN UPPER(clean_cntry) = 'DE' THEN 'Germany'
        WHEN UPPER(clean_cntry) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(clean_cntry) = '' OR clean_cntry IS NULL THEN 'n/a'
        ELSE clean_cntry
    END AS cntry
FROM (
    SELECT 
        cid,
        cntry,
        TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) AS clean_cntry
    FROM bronze.erp_loc_a101
) AS sub
ORDER BY cntry  -- looking good!!


-- Let's insert into silver

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)

SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(clean_cntry) = 'DE' THEN 'Germany'
        WHEN UPPER(clean_cntry) IN ('US', 'USA') THEN 'United States'
        WHEN UPPER(clean_cntry) = '' OR clean_cntry IS NULL THEN 'n/a'
        ELSE clean_cntry
    END AS cntry
FROM (
    SELECT 
        cid,
        cntry,
        TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) AS clean_cntry
    FROM bronze.erp_loc_a101
) AS sub



-- Quality check

SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

-- Final look to the table

SELECT * FROM silver.erp_loc_a101  -- nice!!
