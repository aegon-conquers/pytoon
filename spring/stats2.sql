-- Step 1: Define the table and database
SET @table_name = 'your_table';
SET @db_name = 'your_database';

-- Step 2: Fetch all column names for the table and generate the dynamic query for column value extraction
SET @columns_query = (
    SELECT GROUP_CONCAT(
        CONCAT(
            "SELECT '", column_name, "' AS column_name, `", column_name, "` AS column_value FROM `", @db_name, "`.`", @table_name, "`"
        )
        SEPARATOR ' UNION ALL '
    )
    FROM information_schema.columns
    WHERE table_schema = @db_name AND table_name = @table_name
);

-- Ensure the @columns_query has content
IF @columns_query IS NULL THEN
    SIGNAL SQLSTATE '45000'
    SET MESSAGE_TEXT = 'No columns found for the specified table.';
END IF;

-- Step 3: Construct the main query to analyze data types dynamically
SET @full_query = CONCAT(
    "SELECT
        column_name,
        SUM(CASE WHEN column_value REGEXP '^-?[0-9]+$' THEN 1 ELSE 0 END) AS bigint_count,
        SUM(CASE WHEN column_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS float_count,
        SUM(CASE WHEN column_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS double_count,
        SUM(CASE WHEN column_value REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS decimal_count,
        SUM(CASE WHEN column_value REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 1 ELSE 0 END) AS date_count,
        SUM(CASE WHEN column_value REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN 1 ELSE 0 END) AS datetime_count,
        SUM(CASE WHEN column_value IS NOT NULL THEN 1 ELSE 0 END) AS total_count
    FROM (", @columns_query, ") AS subquery
    GROUP BY column_name"
);

-- Debugging: Print the @full_query to check syntax
SELECT @full_query AS debug_query;

-- Step 4: Execute the dynamic query
PREPARE stmt FROM @full_query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


-- Analyze data types for all columns in a specific table
SET @db_name = 'your_database';
SET @table_name = 'your_table';

-- Step 1: Create a query for all columns in the table using information_schema
SELECT 
    CONCAT(
        "SELECT '", column_name, "' AS column_name,",
        " SUM(CASE WHEN `", column_name, "` REGEXP '^-?[0-9]+$' THEN 1 ELSE 0 END) AS bigint_count,",
        " SUM(CASE WHEN `", column_name, "` REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS float_count,",
        " SUM(CASE WHEN `", column_name, "` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 1 ELSE 0 END) AS date_count,",
        " SUM(CASE WHEN `", column_name, "` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' THEN 1 ELSE 0 END) AS datetime_count,",
        " COUNT(*) AS total_count",
        " FROM `", @db_name, "`.`", @table_name, "`"
    )
FROM information_schema.columns
WHERE table_schema = @db_name AND table_name = @table_name;
