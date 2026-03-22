DELIMITER //
CREATE PROCEDURE AggregateData(IN dt_param date)
BEGIN
    INSERT INTO operations_aggregates 
    (id_country,dt_date,id_type_oper,amount_aggregate)
    SELECT 
        u.id_country, DATE(o.dt), o.id_type_oper,
        SUM(o.amount_oper)
    FROM user_operations o
    JOIN users u ON u.id_user = o.id_user
    WHERE DATE(o.dt) = dt_param
    GROUP BY u.id_country, DATE(o.dt), o.id_type_oper
	ON DUPLICATE KEY UPDATE
	amount_aggregate = VALUES(amount_aggregate);
END //
DELIMITER ;

DELIMITER //
CREATE EVENT IF NOT EXISTS daily_consolidation_event
ON SCHEDULE 
    EVERY 1 DAY 
    -- Высчитываем 3:00 утра следующего дня как точку старта
    STARTS (CURRENT_DATE + INTERVAL 1 DAY + INTERVAL 3 HOUR)
ON COMPLETION PRESERVE
DO
BEGIN
    -- В 3 ночи нужно агрегировать вчерашний день
    CALL AggregateData(CURRENT_DATE - INTERVAL 1 DAY);
END //
DELIMITER ;