-- Поскольку таблица логов большая, преобразование ее в таблицу с партициями, осложнит работу с базой.
-- Поэтому создаем временную таблицу с партициями и копируем в нее данные из исходной таблицы.
-- Производим переименование таблиц.

-- Создание временной таблицы с нужными партициями. 
-- Чтобы не хардкодить название и дату первой партиции, обернем создание в процедуру
SELECT MIN(dt) INTO @start_dt FROM log_users;
SELECT @start_dt;

DELIMITER //
CREATE PROCEDURE CreateTableWithPartitions(IN start_dt TIMESTAMP)
BEGIN
	DECLARE p_name varchar(10);
    DECLARE p_limit varchar(30);
    
	-- формируем название pYYYYMMDD
    SET p_name = CONCAT('p', DATE_FORMAT(start_dt, '%Y%m%d'));
    -- формируем лимит
    SET p_limit = CONCAT(DATE(DATE_ADD(start_dt, INTERVAL 1 DAY)), ' 00:00:00');
    
    -- формируем запрос на создание таблицы
    -- поскольку в задании есть прямое указание на использование PARTITION BY RANGE COLUMNS,
    -- для поля dt используем тип DATETIME
    SET @sql_query = CONCAT(
        'CREATE TABLE log_users_new ( ',
        '  dt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ',
        '  idUser INT UNSIGNED NOT NULL, ',
        '  idAction INT UNSIGNED NOT NULL, ',
        '  Params JSON DEFAULT NULL, ',
        '  PRIMARY KEY (idUser, idAction, dt) ',
        ') ENGINE = INNODB ',
        'PARTITION BY RANGE COLUMNS(dt) ( ',
        '  PARTITION ', p_name, ' VALUES LESS THAN (\'', p_limit, '\') ',
        ')'
    );
    PREPARE stmt FROM @sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;
CALL CreateTableWithPartitions(@start_dt);

-- Создадим партиции по остальным датам log_users
DELIMITER //
CREATE PROCEDURE CreatePartitions()
BEGIN
	DECLARE start_dt DATE;
    DECLARE end_dt DATE;
    DECLARE p_name varchar(10);
    DECLARE p_limit varchar(30);
    
	SELECT MIN(dt) INTO start_dt FROM log_users; 
    SELECT MAX(dt) INTO end_dt FROM log_users; 
    
    -- Ранее уже создали стартовую партицию
    SET start_dt = DATE_ADD(start_dt, INTERVAL 1 DAY);
    
    WHILE start_dt <= end_dt DO
		-- формируем название pYYYYMMDD
		SET p_name = CONCAT('p', DATE_FORMAT(start_dt, '%Y%m%d'));
		-- формируем лимит
		SET p_limit = CONCAT(DATE(DATE_ADD(start_dt, INTERVAL 1 DAY)), ' 00:00:00');
		
        SET @sql_query = CONCAT('ALTER TABLE log_users_new ADD PARTITION (PARTITION ', 
                          p_name, ' VALUES LESS THAN (\'', p_limit, '\'))');
                          
		
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
        SET start_dt = DATE_ADD(start_dt, INTERVAL 1 DAY);
    END WHILE;
    
END //
DELIMITER ;
CALL CreatePartitions();

-- Скопируем записи из log_users в log_users_new
DELIMITER //
CREATE PROCEDURE MigrateData()
BEGIN
    DECLARE start_dt DATE;
    DECLARE end_d DATE;

    SELECT DATE(MIN(dt)) INTO start_dt FROM log_users;
    SELECT DATE(MAX(dt)) INTO end_d FROM log_users;

    WHILE start_dt <= end_d DO
        INSERT IGNORE INTO log_users_new (dt, idUser, idAction, Params)
        SELECT dt, idUser, idAction, Params 
        FROM log_users 
        WHERE dt >= CAST(CONCAT(start_dt, ' 00:00:00') AS DATETIME) 
          AND dt <  CAST(CONCAT(DATE_ADD(end_d, INTERVAL 1 DAY), ' 00:00:00') AS DATETIME);
        
        COMMIT; 
        
        SET start_dt = DATE_ADD(start_dt, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;
CALL MigrateData();

-- Проверим результат работы процедур
SELECT 
    PARTITION_NAME, 
    TABLE_ROWS, 
    PARTITION_DESCRIPTION
FROM INFORMATION_SCHEMA.PARTITIONS 
WHERE TABLE_NAME = 'log_users_new';
SELECT * FROM log_users_new PARTITION (p20260316);

-- Делаем перименование таблиц
RENAME TABLE log_users TO log_users_old, 
             log_users_new TO log_users;

-- Настраиваем ротацию
SET GLOBAL event_scheduler = ON;

DROP PROCEDURE IF EXISTS Rotation_LogUsers;
DELIMITER //
CREATE PROCEDURE Rotation_LogUsers()
BEGIN
	DECLARE i INT DEFAULT 0;
    DECLARE target_date DATE;
    DECLARE p_name VARCHAR(10);
    DECLARE p_limit VARCHAR(30);
    DECLARE old_p_name VARCHAR(10);

    -- 1. Создаем партиции на сегодня и ещём2 дня вперед
	WHILE i <= 2 DO
		SET target_date = DATE_ADD(CURDATE(), INTERVAL i DAY);
		SET p_name = CONCAT('p', DATE_FORMAT(target_date, '%Y%m%d'));
		SET p_limit = CONCAT(DATE_ADD(target_date, INTERVAL 1 DAY), ' 00:00:00');

		-- Добавляем, только если ее еще нет (чтобы избежать ошибок)
		SET @sql_add = CONCAT('ALTER TABLE log_users ADD PARTITION (PARTITION ', 
                          p_name, ' VALUES LESS THAN (\'', p_limit, '\'))');
    
		-- Используем блок продолжения на случай, если партиция уже есть
		BEGIN
			DECLARE CONTINUE HANDLER FOR 1517 BEGIN END;
			PREPARE stmt1 FROM @sql_add;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;
		END;
		SET i = i + 1;
    END WHILE;

    -- 2. Удаляем старые данные (Меньше чем CURDATE - 7 дней)
    SET SESSION group_concat_max_len = 1000000;
    SELECT GROUP_CONCAT( PARTITION_NAME SEPARATOR ',')
      INTO @partitions_to_drop
	  FROM INFORMATION_SCHEMA.PARTITIONS 
	 WHERE TABLE_NAME = 'log_users'
       AND STR_TO_DATE(PARTITION_NAME, 'p%Y%m%d') < CURDATE() - INTERVAL 7 DAY
	GROUP BY TABLE_NAME;
    
    IF @partitions_to_drop IS NOT NULL THEN
		SET @sql_drop = CONCAT('ALTER TABLE log_users DROP PARTITION ', @partitions_to_drop );
		BEGIN
			DECLARE CONTINUE HANDLER FOR 1507 BEGIN END;
			PREPARE stmt FROM @sql_drop;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		END;
	END IF;
END //
DELIMITER ;

DELIMITER //

CREATE EVENT IF NOT EXISTS daily_log_rotation
ON SCHEDULE EVERY 1 DAY
-- Запуск в 02:00 следующего дня
STARTS (CURRENT_DATE + INTERVAL 1 DAY + INTERVAL 2 HOUR) 
ON COMPLETION PRESERVE 
DO
  CALL Rotation_LogUsers() // 

DELIMITER ;