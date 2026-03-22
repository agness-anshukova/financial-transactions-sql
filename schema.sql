CREATE TABLE user_operations (
id_operation BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
id_user INT UNSIGNED NOT NULL,
id_type_oper SMALLINT UNSIGNED NOT NULL,
direction TINYINT NOT NULL,
amount_oper DECIMAL(19, 2) NOT NULL,
PRIMARY KEY (id_operation)
) ENGINE = INNODB;

CREATE TABLE users (
id_user INT UNSIGNED NOT NULL AUTO_INCREMENT,
user_balance DECIMAL(19, 2) DEFAULT 0.00,
id_currency SMALLINT UNSIGNED NOT NULL,
id_country SMALLINT UNSIGNED NOT NULL,
PRIMARY KEY (id_user)
) ENGINE = INNODB;

CREATE TABLE currencies (
id_currency SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
name_currency VARCHAR(255) DEFAULT NULL,
base_rate DECIMAL(15, 5) DEFAULT NULL,
PRIMARY KEY (id_currency)
) ENGINE = INNODB;

CREATE TABLE countries (
id_country SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
name_country VARCHAR(50) DEFAULT NULL,
PRIMARY KEY (id_country)
) ENGINE = INNODB;

CREATE TABLE type_opers (
id_type_oper SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
name_oper VARCHAR(255) DEFAULT NULL COMMENT 'Название операции',
commission DECIMAL(5, 2) DEFAULT 0.00 COMMENT 'Процент комиссии за операцию',
direction TINYINT NOT NULL COMMENT 'направление движения (-1: списание, 1:
начисление)',
PRIMARY KEY (id_type_oper)
) ENGINE = INNODB;


CREATE TABLE log_users (
dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
idUser INT UNSIGNED NOT NULL,
idAction INT UNSIGNED NOT NULL,
Params JSON DEFAULT NULL
) ENGINE = INNODB;

CREATE TABLE operations_aggregates (
id_aggr_oper BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
id_country SMALLINT UNSIGNED NOT NULL,
dt_date DATE NOT NULL,
id_type_oper SMALLINT UNSIGNED NOT NULL,
amount_aggregate DECIMAL(38, 2) NOT NULL,
PRIMARY KEY (id_aggr_oper)
) ENGINE = INNODB;

-- Индексы для ускорения отчетов и процедур
ALTER TABLE user_operations 
ADD INDEX idx_user_dt (id_user, dt);

ALTER TABLE user_operations 
ADD INDEX idx_dt_type (dt, id_type_oper);

ALTER TABLE users 
ADD INDEX idx_country (id_country);

ALTER TABLE users 
ADD INDEX idx_currency (id_currency);