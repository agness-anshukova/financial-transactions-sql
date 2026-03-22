DELIMITER //
CREATE PROCEDURE  ProcessUserSum(
  IN id_us INT UNSIGNED, 
  IN amount_op DECIMAL(19, 2), 
  IN id_type_op SMALLINT UNSIGNED,
  OUT exec_state VARCHAR(10),
  OUT balance_before DECIMAL(19, 2),
  OUT balance_after DECIMAL(19, 2)
)
proc: 
  BEGIN
    -- Детали операции, которые сохраним в логе
	DECLARE log_json JSON;
    -- Время начала операции
    DECLARE start_time TIMESTAMP;
    -- Направление операции
    DECLARE op_direction TINYINT DEFAULT NULL;
    -- Вид действия для таблицы логов, поскольку not null, зададим дефолтное значение для некорректных операций
    DECLARE action_id TINYINT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SET exec_state = 'error';
    END;

    SET start_time = UTC_TIMESTAMP();
    
    -- Одним зпросом проверяем наличие идентификатора и получаем направление
    SELECT direction
      INTO op_direction
      FROM type_opers
     WHERE id_type_oper = id_type_op;
     
     -- Если ошибка, запишем лог и покинем процедуру
     IF op_direction IS NULL THEN
     	SET exec_state = 'error';
     	SET balance_before = NULL;
        SET balance_after = NULL;
        SET log_json = JSON_OBJECT(
          					'exec_state',exec_state,
        					'error','invalid id_type_oper',
        					'amount_oper', amount_op,
              				'id_type_oper',id_type_op
            				);
                            
        INSERT INTO log_users (dt,idUser,idAction,Params)
             VALUES (start_time,id_us,action_id,log_json);

		LEAVE proc;
     END IF;
     
     SET action_id = CASE
        				 WHEN op_direction = 1 THEN 1
        				 WHEN op_direction = -1 THEN 2
     			     END;
                     
     -- Т.к. исправляем таблицу пользователей, делаем select for update
     -- и открываем транзакцию
     START TRANSACTION;
     
     -- Одним запросом проверим сущестование пользователя и его баланс
     SELECT user_balance
       INTO balance_before
       FROM users
      WHERE id_user = id_us
      FOR UPDATE;
    
	-- Если ошибка, запишем лог и покинем процедуру
	IF balance_before IS NULL THEN
    	ROLLBACK;
    	SET exec_state = 'error';
     	SET balance_before = NULL;
        SET balance_after = NULL;
        SET log_json = JSON_OBJECT(
          					'exec_state',exec_state,
        					'error','invalid id_user',
        					'amount_oper', amount_op,
              				'id_type_oper',id_type_op
            				);
                            
        INSERT INTO log_users 
        			(dt,idUser,idAction,Params)
             VALUES (start_time,id_us,action_id,log_json);
                     
    	LEAVE proc;
    END IF;
    
    -- Отклоним операцию, если недостаточно средств, и покинем процедуру
	IF op_direction = -1 AND balance_before < amount_op THEN
    	ROLLBACK; 
    	SET exec_state = 'denied';
        SET balance_after = balance_before;
        SET log_json = JSON_OBJECT(
          					'exec_state',exec_state,
        					'error','not enough funds',
        					'amount_oper', amount_op,
              				'id_type_oper',id_type_op
            				);
                            
        INSERT INTO log_users 
        			(dt,idUser,idAction,Params)
             VALUES (start_time,id_us, action_id,log_json);
                    
    	LEAVE proc;
    END IF;
    
    -- Если все проверки пройдены, делаем изменение и вставку
    SET balance_after = balance_before + amount_op*op_direction;
	UPDATE users
       SET user_balance = balance_after
     WHERE id_user = id_us;
    
    SET exec_state = 'executed'; 
    SET log_json = JSON_OBJECT(
          					'exec_state',exec_state,
        					'amount_oper', amount_op,
              				'id_type_oper',id_type_op,
      						'balance_before', balance_before,
    						'balance_after', balance_after
            				);
                            
     INSERT INTO user_operations 
     			 (dt,id_user,id_type_oper,direction,amount_oper)
          VALUES (start_time,id_us,id_type_op,op_direction,amount_op);

	INSERT INTO log_users 
    			(dt,idUser,idAction,Params)
         VALUES (start_time,id_us,action_id,log_json);
    COMMIT;
END //

DELIMITER ;