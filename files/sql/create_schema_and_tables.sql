CREATE SCHEMA IF NOT EXISTS ds;
CREATE SCHEMA IF NOT EXISTS logs;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS logs.data_load_logs(
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration INTERVAL NOT NULL,
    table_name TEXT NOT NULL,
    message TEXT
);
	
CREATE TABLE IF NOT EXISTS ds.ft_balance_f(
    on_date DATE NOT NULL,
    account_rk INT NOT NULL,
    currency_rk INT,
    balance_out numeric,
    CONSTRAINT pk_on_date_account_rk PRIMARY KEY(on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f(
    oper_date DATE NOT NULL,
    credit_account_rk INT NOT NULL,
    debet_account_rk INT NOT NULL,
    credit_amount NUMERIC,
    debet_amount NUMERIC
);

CREATE TABLE IF NOT EXISTS ds.md_account_d(
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk INT NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk INT NOT NULL,
    currency_code VARCHAR(3) NOT NULL,
    CONSTRAINT pk_data_actual_date_account_rk PRIMARY KEY(data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d(
    currency_rk INT NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3),
    CONSTRAINT pk_currency_rk_data_actual_date PRIMARY KEY(currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d(
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk INT NOT NULL,
    reduced_cource NUMERIC,
    code_iso_num VARCHAR(3),
    CONSTRAINT pk_data_actual_date_currency_rk PRIMARY KEY(data_actual_date, currency_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s(
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INT,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INT,
    ledger1_account_name VARCHAR(47),
    ledger_account INT NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    is_resident INT,
    is_reserve INT,
    is_reserved INT,
    is_loan INT,
    is_reserved_assets INT,
    is_overdue INT,
    is_interests INT,
    pair_account VARCHAR(5),
    start_date DATE NOT NULL,
    end_date DATE,
    is_rub_only INT,
    min_term VARCHAR(1),
    min_term_measure VARCHAR(1),
    max_term VARCHAR(1),
    max_term_measure VARCHAR(1),
    ledger_acc_full_name_translit VARCHAR(1),
    is_revaluation VARCHAR(1),
    is_correct VARCHAR(1),
    CONSTRAINT pk_ledger_account_start_date PRIMARY KEY(ledger_account, start_date)
);

CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f(
    on_date DATE,
    account_rk INT,
    credit_amount NUMERIC,
    credit_amount_rub NUMERIC,
    debet_amount NUMERIC,
    debet_amount_rub NUMERIC
);

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f(
    from_date DATE,
    to_date DATE,
    chapter CHAR(1),
    ledger_account CHAR(5),
    characteristic CHAR(1),
    balance_in_rub NUMERIC,
    r_balance_in_rub NUMERIC,
    balance_in_val NUMERIC,
    r_balance_in_val NUMERIC,
    balance_in_total NUMERIC,
    r_balance_in_total NUMERIC,
    turn_deb_rub NUMERIC,
    r_turn_deb_rub NUMERIC,
    turn_deb_val NUMERIC,
    r_turn_deb_val NUMERIC,
    turn_deb_total NUMERIC,
    r_turn_deb_total NUMERIC,
    turn_cre_rub NUMERIC,
    r_turn_cre_rub NUMERIC,
    turn_cre_val NUMERIC,
    r_turn_cre_val NUMERIC,
    turn_cre_total NUMERIC,
    r_turn_cre_total NUMERIC,
    balance_out_rub NUMERIC,
    r_balance_out_rub NUMERIC,
    balance_out_val NUMERIC,
    r_balance_out_val NUMERIC,
    balance_out_total NUMERIC,
    r_balance_out_total NUMERIC
);
