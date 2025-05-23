/*
-------------- DATA CLEANING -----------
This is all the data cleaning process done on the Bronze Schema before Inserting into the Silver Schema.
*/
-- Data cleaning on the bronze schema before transporting to the silver schema
-- Check for Nulls or Duplicates in the Primary key
-- Expectation: No results

-- Checking for Nulls
SELECT 
	* 
FROM bronze.crm_cust_info
WHERE cst_id IS NULL;

-- Checking for Duplicates
SELECT  
	cst_id, 
	COUNT(cst_id) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1;

-- Investigating the Duplicates
SELECT 
	*,
	row_number() over(ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info 
WHERE cst_id = 29466;

-- Flagging all duplicates
SELECT 
	* 
FROM (
	SELECT 
		*,
		row_number() over(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
) AS t1
WHERE flag_last = 1;


-- Checking for unwanted spaces
SELECT 
	cst_firstname,
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
AND cst_lastname != TRIM(cst_lastname);

-- Combining all cleaned up data
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	-- Data Normalization & Standardization and also handling missing values
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date 
FROM (
	SELECT 
		*,
		row_number() over(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
) AS t1
WHERE flag_last = 1;

----------- Inserting all cleaned up data into Silver.crm_cust_info -------------

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr,
	cst_create_date 
FROM (
	SELECT 
		*,
		row_number() over(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
) AS t1
WHERE flag_last = 1;


-- CrossChecking for Nulls in Silver schema
-- Expectation: No Results
SELECT 
	* 
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

-- CrossChecking for Duplicates
-- Expectation: No Results
SELECT  
	cst_id, 
	COUNT(cst_id) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1;

-- CrossChecking for unwanted spaces
-- Expectation: No Results
SELECT 
	cst_firstname,
	cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
AND cst_lastname != TRIM(cst_lastname);

SELECT * FROM silver.crm_cust_info;


----- DATA CLEANING FOR bronze.crm_prd_info TABLE
-- Data cleaning on the bronze schema before transporting to the silver schema
-- Check for Nulls or Duplicates in the Primary key
-- Expectation: No results

-- Checking for Nulls
SELECT 
	* 
FROM bronze.crm_prd_info
WHERE prd_id IS NULL;

SELECT 
	* 
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Checking for Duplicates
SELECT  
	prd_id, 
	COUNT(prd_id) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(prd_id) > 1;

-- Cleaned data
SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key FROM 7) AS prd_key,
	prd_nm,
	COALESCE(prd_cost, 0),
	CASE UPPER(TRIM(prd_line))
		WHEN  'M' THEN 'Mountain'
		WHEN  'R' THEN 'Road'
		WHEN  'S' THEN 'Other Sales'
		WHEN  'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS date),
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)- INTERVAL '1 day' AS date) AS prd_end_dt
FROM bronze.crm_prd_info;


------------------- INSERTING CLEANED DATA FROM BRONZE INTO SILVER.PRD_TABLE ---------------
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT 
	prd_id,
	-- Derived new columns from existing prd_key column
	REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key FROM 7) AS prd_key,
	prd_nm,
	-- Handeling the Null
	COALESCE(prd_cost, 0) AS prd_cost,
	-- Data Normalization
	CASE UPPER(TRIM(prd_line))
		WHEN  'M' THEN 'Mountain'
		WHEN  'R' THEN 'Road'
		WHEN  'S' THEN 'Other Sales'
		WHEN  'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	-- Data Type Casting
	CAST(prd_start_dt AS date),
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)- INTERVAL '1 day' AS date) AS prd_end_dt
FROM bronze.crm_prd_info;



-- CrossChecking for Nulls in Silver schema
-- Expectation: No Results

SELECT 
	* 
FROM silver.crm_prd_info;

SELECT 
	* 
FROM silver.crm_prd_info
WHERE prd_id IS NULL;

-- CrossChecking for Duplicates
-- Expectation: No Results
SELECT  
	prd_id, 
	COUNT(prd_id) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(prd_id) > 1;

-- CrossChecking for unwanted spaces
-- Expectation: No Results
SELECT 
	* 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;



----- DATA CLEANING FOR bronze.crm_sale_details TABLE
-- Data cleaning on the bronze schema before transporting to the silver schema
-- Check every single column and make sure they or standardized


-- Checking for Nulls
SELECT 
	* 
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL;

-- Working on the invalid dates
SELECT 
	NULLIF(sls_order_dt, 0) 
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8;

-- check the sales, Quantity and price columns
SELECT 
	sls_sales AS old_sls_sales,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * abs(sls_price) THEN sls_quantity * abs(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	sls_price AS old_sls_price,
	CASE 
		WHEN sls_price IS NULL OR sls_price <=0 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;

-- CLEANED DATE FROM bronze.crm_sales_details TABLE
SELECT 
	 sls_ord_num,
	 sls_prd_key,
	 sls_cust_id,
	 -- Handling invalid data and type casting
	 CASE 
	 	WHEN sls_order_dt = 0 OR length(sls_order_dt::TEXT) != 8 THEN NULL 
	 	ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	 END AS sls_order_dt,
	 CASE 
	 	WHEN sls_ship_dt = 0 OR length(sls_ship_dt::TEXT) != 8 THEN NULL 
	 	ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
	 END AS sls_ship_dt,
	 -- Handling missing data and deriving data from existing column
	 CASE 
	 	WHEN sls_due_dt = 0 OR length(sls_due_dt::TEXT) != 8 THEN NULL 
	 	ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	 END AS sls_due_dt,
	 CASE 
	 	WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * abs(sls_price) THEN sls_quantity * abs(sls_price)
	 	ELSE sls_sales
	 END AS sls_sales,
	 sls_quantity,
	 CASE 
	 	WHEN sls_price IS NULL OR sls_price <=0 THEN sls_sales / NULLIF(sls_quantity, 0)
	 	ELSE sls_price
	 END AS sls_price
FROM bronze.crm_sales_details;


----------- BEFORE INSERTING THE CLEANED DATA, MAKE SURE TO CHECK THE DDL SILVER TABLE TO MAKE SURE ALL DATA TYPES MATCH ------------

TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT 
	prd_id,
	-- Derived new columns from existing prd_key column
	REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key FROM 7) AS prd_key,
	prd_nm,
	-- Handeling the Null
	COALESCE(prd_cost, 0) AS prd_cost,
	-- Data Normalization
	CASE UPPER(TRIM(prd_line))
		WHEN  'M' THEN 'Mountain'
		WHEN  'R' THEN 'Road'
		WHEN  'S' THEN 'Other Sales'
		WHEN  'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	-- Data Type Casting
	CAST(prd_start_dt AS date),
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)- INTERVAL '1 day' AS date) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Crosscheck the health of the table
SELECT 
	sls_sales,
	sls_sales,
	sls_quantity,
	sls_price,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details;


----- DATA CLEANING FOR bronze.erp_cust_az12 TABLE
-- Data cleaning on the bronze schema before transporting to the silver schema
-- Check every single column and make sure they or standardized


-- Extract new cid from the old one that would be used in joining tables
SELECT  
	CASE 
		WHEN cid LIKE 'NAS%' THEN substring(cid FROM 4)
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > now() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;

-- identify out-of-range dates
SELECT 
	DISTINCT bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > now();

-- check the gen column
SELECT 
	DISTINCT gen 
FROM bronze.erp_cust_az12;

--------------- insert cleaned data into Silver schema --------------
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT 
-- Handling invalid values and missing values
	CASE 
		WHEN cid LIKE 'NAS%' THEN substring(cid FROM 4)
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > now() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;



----- DATA CLEANING FOR bronze.erp_loc_az12 TABLE
-- Data cleaning on the bronze schema before transporting to the silver schema
-- Check every single column and make sure they or standardized

SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry FROM bronze.erp_loc_a101;


------------- Inserting data --------------
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT 
-- Handling invalid values
	REPLACE(cid, '-', '') AS cid,
-- Data Normalization
-- Removing unwanted spaces
-- Handling missing values
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;



----- DATA CLEANING FOR bronze.erp_px_cat_g1v2 TABLE
-- Data cleaning on the bronze schema before transporting to the silver schema
-- Check every single column and make sure they or standardized

------- INSERTING DATA IN SCHEMA ----------
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT 
	* 
FROM bronze.erp_px_cat_g1v2;
