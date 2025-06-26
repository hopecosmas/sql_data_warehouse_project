

*/ 
  This contains some backgroung thought process while working on this project and some 

  quality tesr
  */



/*
Now, after creating the tables, it's time to start exploring the issues in any table.
Only then we'll do some transformations.

 */

-- Check the tables from the bronze and then do some changes before inserting to the silver layer

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

-- A primary key must be UNIQUE and NOT NULLl. Check if is there is any violation to that

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- (2) Check for Unwanted Spaces
-- Expectation: No Result

/* 
Note: If the original value is not equal to the same value after trimming, it means
there are spaces!
*/

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Data Standardization & Consistency
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info


SELECT * from silver.crm_cust_info
