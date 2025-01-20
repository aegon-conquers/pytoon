-- Step 1: Define the table and database
SET @table_name = 'your_table';
SET @db_name = 'your_database';

-- Step 2: Fetch all column names for the table and generate the dynamic query
SET @columns_query = (
    SELECT GROUP_CONCAT(
        CONCAT(
            "SELECT '", column_name, "' AS column_name, ", column_name, " AS column_value FROM ", @db_name, ".", @table_name
        )
        SEPARATOR ' UNION ALL '
    )
    FROM information_schema.columns
    WHERE table_schema = @db_name AND table_name = @table_name
);

-- Step 3: Construct the main query to analyze data types
SET @full_query = CONCAT(
    "SELECT
        column_name,
        SUM(CASE WHEN TRY_CAST(column_value AS BIGINT) IS NOT NULL THEN 1 ELSE 0 END) AS bigint_count,
        SUM(CASE WHEN TRY_CAST(column_value AS FLOAT) IS NOT NULL THEN 1 ELSE 0 END) AS float_count,
        SUM(CASE WHEN TRY_CAST(column_value AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) AS double_count,
        SUM(CASE WHEN TRY_CAST(column_value AS DECIMAL) IS NOT NULL THEN 1 ELSE 0 END) AS decimal_count,
        SUM(CASE WHEN TRY_CAST(column_value AS DATE) IS NOT NULL THEN 1 ELSE 0 END) AS date_count,
        SUM(CASE WHEN TRY_CAST(column_value AS DATETIME) IS NOT NULL THEN 1 ELSE 0 END) AS datetime_count,
        SUM(CASE WHEN TRY_CAST(column_value AS CHAR) IS NOT NULL THEN 1 ELSE 0 END) AS char_count,
        SUM(CASE WHEN TRY_CAST(column_value AS TEXT) IS NOT NULL THEN 1 ELSE 0 END) AS text_count,
        SUM(CASE WHEN TRY_CAST(column_value AS ENUM) IS NOT NULL THEN 1 ELSE 0 END) AS enum_count,
        COUNT(*) AS total_count
    FROM (", @columns_query, ") subquery
    GROUP BY column_name"
);

-- Step 4: Execute the dynamic query
PREPARE stmt FROM @full_query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
