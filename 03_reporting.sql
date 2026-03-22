DELIMITER //
CREATE PROCEDURE GetReport(IN p_from datetime, IN p_to datetime)
BEGIN
    SELECT IF(GROUPING(c.name_country) = 1, 'TOTAL', c.name_country) AS country_name,
		   IF(GROUPING(t.id_type_oper)=1,'TOTAL', IF(t.id_type_oper = 1, "Deposit", "Withdrawal") ) AS oper_name,
		   SUM((amount_oper/cu.base_rate)+((amount_oper/cu.base_rate)/100)*t.commission) AS amount_rub,
		   SUM(t.commission*((amount_oper/cu.base_rate)/100)) AS amount_comiss_rub,
		   SUM(o.amount_oper/cu.base_rate) AS amount_no_comiss_rub
	  FROM user_operations AS o
	  JOIN users AS u ON u.id_user = o.id_user
	  JOIN countries AS c ON c.id_country = u.id_country
	  JOIN type_opers AS t ON t.id_type_oper = o.id_type_oper
	  JOIN currencies cu ON cu.id_currency = u.id_currency
	 WHERE o.dt BETWEEN p_from AND p_to
  GROUP BY c.name_country, t.id_type_oper WITH ROLLUP;

END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE GetReportByDate(IN p_from datetime, IN p_to datetime)
BEGIN
    SELECT IF (GROUPING(c.name_country) = 1, 'TOTAL', c.name_country) AS country_name,
		   IF(GROUPING(t.id_type_oper)=1,'TOTAL', IF(t.id_type_oper = 1, "Deposit", "Withdrawal") ) AS oper_name,
		   SUM((o.amount_oper*cu.base_rate)+((o.amount_oper*cu.base_rate)/100)*t.commission) AS amount_rub,
		   SUM(t.commission*((o.amount_oper*cu.base_rate)/100)) AS amount_comiss_rub,
		   SUM(o.amount_oper*cu.base_rate) AS amount_no_comiss_rub
      FROM user_operations AS o
	  JOIN users AS u ON u.id_user = o.id_user
      JOIN countries AS c ON c.id_country = u.id_country
      JOIN type_opers AS t ON t.id_type_oper = o.id_type_oper
      JOIN currencies cu ON cu.id_currency = u.id_currency
     WHERE o.dt >= p_from AND o.dt < LEAST(p_to, ( DATE(p_from) + INTERVAL 1 DAY))
  GROUP BY c.name_country, t.id_type_oper  WITH ROLLUP

    UNION ALL 
    SELECT IF (GROUPING(c.name_country) = 1, 'TOTAL', c.name_country) AS country_name,
		   IF(GROUPING(t.id_type_oper)=1,'TOTAL', IF(t.id_type_oper = 1, "Deposit", "Withdrawal") ) AS oper_name,
		   SUM(a.amount_aggregate + (t.commission * a.amount_aggregate / 100)) AS amount_rub,
		   SUM(t.commission * (a.amount_aggregate / 100)) AS amount_comiss_rub,
		   SUM(a.amount_aggregate) AS amount_no_comiss_rub
	  FROM operations_aggregates AS a
	  JOIN countries c ON c.id_country = a.id_country
	  JOIN type_opers t ON t.id_type_oper = a.id_type_oper
     WHERE a.dt_date >= ( DATE(p_from) + INTERVAL 1 DAY) AND a.dt_date < DATE(p_to)
  GROUP BY c.name_country, t.id_type_oper  WITH ROLLUP

    UNION ALL
    SELECT IF (GROUPING(c.name_country) = 1, 'TOTAL', c.name_country) AS country_name,
		   IF(GROUPING(t.id_type_oper)=1,'TOTAL', IF(t.id_type_oper = 1, "Deposit", "Withdrawal") ) AS oper_name,
		   SUM((o.amount_oper*cu.base_rate)+((o.amount_oper*cu.base_rate)/100)*t.commission) AS amount_rub,
		   SUM(t.commission*((o.amount_oper*cu.base_rate)/100)) AS amount_comiss_rub,
		   SUM(o.amount_oper*cu.base_rate) AS amount_no_comiss_rub
      FROM user_operations AS o
      JOIN users AS u ON u.id_user = o.id_user
      JOIN countries AS c ON c.id_country = u.id_country
      JOIN type_opers AS t ON t.id_type_oper = o.id_type_oper
      JOIN currencies cu ON cu.id_currency = u.id_currency
     WHERE (o.dt >= DATE(p_to) AND o.dt <= p_to) 
       AND DATE(p_to) != DATE(p_from)
  GROUP BY c.name_country, t.id_type_oper  WITH ROLLUP;
END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE GetUserReport(IN p_from DATETIME, IN p_to DATETIME, IN id_us INT)
BEGIN
    SELECT t.name_oper AS name_oper,
		   SUM((amount_oper/cu.base_rate)+((amount_oper/cu.base_rate)/100)*t.commission) AS amount_rub,
		   SUM(t.commission*((amount_oper/cu.base_rate)/100)) AS amount_comiss_rub,
		   SUM(o.amount_oper/cu.base_rate) AS amount_no_comiss_rub
	  FROM user_operations AS o
	  JOIN users AS u ON u.id_user = o.id_user
	  JOIN type_opers AS t ON t.id_type_oper = o.id_type_oper
	  JOIN currencies AS cu ON cu.id_currency = u.id_currency
     WHERE u.id_user = id_us
       AND o.dt BETWEEN p_from AND p_to
  GROUP BY t.id_type_oper;
END //
DELIMITER ;