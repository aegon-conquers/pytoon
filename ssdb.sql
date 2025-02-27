DELIMITER //
CREATE PROCEDURE PrintAllDDLs(IN schema_name VARCHAR(64))
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE tbl_name VARCHAR(64);
    DECLARE cur CURSOR FOR 
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = schema_name 
        AND TABLE_TYPE = 'BASE TABLE';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO tbl_name;
        IF done THEN
            LEAVE read_loop;
        END IF;
        SET @sql = CONCAT('SHOW CREATE TABLE ', schema_name, '.', tbl_name);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END //
DELIMITER ;

-- Call the procedure with your schema name
CALL PrintAllDDLs('your_schema');


SELECT 
    TABLE_NAME,
    (SELECT GROUP_CONCAT(
        'SHOW CREATE TABLE ' || TABLE_SCHEMA || '.' || TABLE_NAME || ';'
    )) AS DDL_Command
FROM 
    information_schema.TABLES 
WHERE 
    TABLE_SCHEMA = 'your_schema'
    AND TABLE_TYPE = 'BASE TABLE';
