-- now, product table: bronze.crm_prd_info

SELECT
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info


-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

-- A primary key must be UNIQUE and NOT NULLl. Check if is there is any violation to that

SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- Now the column: prd_key contains sub_category key, we're gonna have to split it

SELECT
prd_id,
prd_key,
SUBSTRING(prd_key,1,5) AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info


-- Again, if you check the id column in table bronze.erp_px_cat_g1v2, the id has 'underscore separation' while in our
-- case here it is separated by 'minus' sign, thus, we need to change that.

SELECT
prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info


-- Filter out un matched data after applying transformation

SELECT
prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE (SUBSTRING(prd_key,1,5), '-','_') NOT IN (
    SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2
)


-- Now, let's get the remaining prd_key

SELECT
prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info


-- (2) Check for Unwanted Spaces
-- Expectation: No Result

/* 
Note: If the original value is not equal to the same value after trimming, it means
there are spaces!
*/

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Looks like the column is fine

-- Next column is prd_cost, check if there is any negative cost(which should not), and replace Null value with 0

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- Replace Null with zero

SELECT
prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info



-- Next, check prd_line.

-- (3) Data Standardization & Consistency

-- check the prd_line column
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

--Make the changes to NOT use abbreviation

SELECT
prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info


-- last tables: prd_start_dt and prd_end_dt
-- Check for Invalid Date Orders ( end date should not be smaller than start date)

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Here, we found out that: start date > end start which is insane!!
-- There is a lot of changes we're gonna make here

--(1) The end date of previous yeear is the start of the next but with no overlapping

-- consider

SELECT
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- We're gonna use Lead() window

SELECT
  prd_id,
  prd_key,
  prd_nm,
  prd_start_dt,
  prd_end_dt,
  DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt_test  -- substract a day
FROM bronze.crm_prd_info 
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');





-- Add it to our whole dataset

 
SELECT
prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
prd_start_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info

-- Let's add it to our silver.crm_prd_info table (making some update by adding the new columns we created)

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(15),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Insert the transformed data

INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT
prd_id,
REPLACE (SUBSTRING(prd_key,1,5), '-','_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
prd_start_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info


