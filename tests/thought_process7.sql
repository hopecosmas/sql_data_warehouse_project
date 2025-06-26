-- Final table

SELECT
*
FROM bronze.erp_px_cat_g1v2

-- The "id" column here is connected to the 'cat_id' in crm_prd_info table(Inside silver layer)

SELECT * FROM silver.crm_prd_info, -- after checking the column are similar, hence no changes in 'id' here

-- check other columns: 

--check for unwanted Spaces

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance) -- looking good!

-- check for Data Standardization & Consistency

SELECT DISTINCT 
cat 
FROM bronze.erp_px_cat_g1v2 -- looking good!


SELECT DISTINCT 
subcat 
FROM bronze.erp_px_cat_g1v2 -- looking good!

SELECT DISTINCT 
maintenance
FROM bronze.erp_px_cat_g1v2 -- Not looking good!

-- let's do some transformation in 'maintenance' column

SELECT
CASE WHEN UPPER(TRIM(maintenance)) LIKE 'Y%' THEN 'Yes'
     WHEN UPPER(TRIM(maintenance)) LIKE 'N%' THEN 'No'
     ELSE maintenance
END AS maintenance
FROM bronze.erp_px_cat_g1v2

-- Integrate this to our main query

SELECT
id,
cat,
subcat,
CASE WHEN UPPER(TRIM(maintenance)) LIKE 'Y%' THEN 'Yes'
     WHEN UPPER(TRIM(maintenance)) LIKE 'N%' THEN 'No'
     ELSE maintenance
END AS maintenance
FROM bronze.erp_px_cat_g1v2  -- table now looking good!



--- Insert into Silver layer

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)

SELECT
id,
cat,
subcat,
CASE WHEN UPPER(TRIM(maintenance)) LIKE 'Y%' THEN 'Yes'
     WHEN UPPER(TRIM(maintenance)) LIKE 'N%' THEN 'No'
     ELSE maintenance
END AS maintenance
FROM bronze.erp_px_cat_g1v2 


--- Quality check

SELECT * FROM silver.erp_px_cat_g1v2
