CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    WITH Accounts AS (
        SELECT DISTINCT credit_account_rk AS account_rk
          FROM ds.ft_posting_f 
         WHERE oper_date = i_OnDate
         UNION
        SELECT DISTINCT debet_account_rk AS account_rk
          FROM ds.ft_posting_f 
         WHERE oper_date = i_OnDate
    ),
    AccountCource AS (
        SELECT DISTINCT fbf.account_rk, 
               fbf.currency_rk,
               merd.reduced_cource
          FROM ds.ft_balance_f fbf
               LEFT JOIN ds.md_exchange_rate_d merd USING(currency_rk)
         WHERE i_OnDate BETWEEN merd.data_actual_date AND merd.data_actual_end_date
            OR merd.reduced_cource IS NULL
    ),
    TotalCredit AS (
         SELECT credit_account_rk, SUM(credit_amount) AS credit_amount
           FROM ds.ft_posting_f
          WHERE oper_date = i_OnDate
          GROUP BY credit_account_rk
    ),
    TotalDebet AS (
        SELECT debet_account_rk, SUM(debet_amount) AS debet_amount
          FROM ds.ft_posting_f
         WHERE oper_date = i_OnDate
         GROUP BY debet_account_rk
    )
    SELECT i_OnDate AS on_date,
           a.account_rk,
           COALESCE(tc.credit_amount, 0) AS credit_amount,
           COALESCE((tc.credit_amount * COALESCE(ac.reduced_cource, 1)), 0) AS credit_amount_rub,
           COALESCE(td.debet_amount, 0) AS debet_amount,
           COALESCE((td.debet_amount * COALESCE(ac.reduced_cource, 1)), 0) AS debet_amount_rub
      FROM Accounts AS a
           LEFT JOIN TotalCredit AS tc ON a.account_rk = tc.credit_account_rk
           LEFT JOIN TotalDebet AS td ON a.account_rk = td.debet_account_rk
           LEFT JOIN AccountCource AS ac ON a.account_rk = ac.account_rk;

    end_time := CURRENT_TIMESTAMP;

    INSERT INTO logs.data_load_logs (start_time, end_time, duration, table_name, message)
    VALUES (start_time, end_time, end_time - start_time, 'dm_account_turnover_f', 
            'Загрузка данных по оборотам за ' || i_OnDate || ' завершена успешно.');
END;
$$;

DO $$
DECLARE
    var_date DATE := '2018-01-01';
BEGIN
    WHILE var_date <= '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(var_date);
        var_date := var_date + INTERVAL '1 day';
    END LOOP;
END $$;

--_______________________________________________________________________________________
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    IF NOT EXISTS (
        SELECT 1
        FROM ds.md_account_d
        WHERE i_OnDate BETWEEN data_actual_date AND data_actual_end_date
    ) THEN RAISE EXCEPTION 'Дата расчета % находится за пределами дат актуальности счетов.', i_OnDate;
    END IF;

    DELETE FROM dm.dm_account_balance_f
    WHERE on_date = i_OnDate;

    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, currency_rk, balance_out, balance_out_rub)
    SELECT i_OnDate AS on_date,
           mad.account_rk, 
           mad.currency_rk, 
           COALESCE(
                (SELECT dabf.balance_out 
                   FROM dm.dm_account_balance_f dabf 
                  WHERE dabf.account_rk = mad.account_rk 
                    AND dabf.on_date = i_OnDate - INTERVAL '1 DAY'), 0
           ) + COALESCE(datf.debet_amount, 0) - COALESCE(datf.credit_amount, 0) AS balance_out,
           COALESCE(
                (SELECT dabf.balance_out_rub
                   FROM dm.dm_account_balance_f dabf 
                  WHERE dabf.account_rk = mad.account_rk 
                    AND dabf.on_date = i_OnDate - INTERVAL '1 DAY'), 0
           ) + COALESCE(datf.debet_amount_rub, 0) - COALESCE(datf.credit_amount_rub, 0) AS balance_out_rub
      FROM ds.md_account_d mad
           LEFT JOIN dm.dm_account_turnover_f datf
                  ON mad.account_rk = datf.account_rk
                 AND datf.on_date = i_OnDate
     WHERE mad.char_type = 'А'
     UNION
    SELECT i_OnDate AS on_date,
           mad.account_rk, 
           mad.currency_rk, 
           COALESCE(
                (SELECT dabf.balance_out 
                   FROM dm.dm_account_balance_f dabf 
                  WHERE dabf.account_rk = mad.account_rk 
                    AND dabf.on_date = i_OnDate - INTERVAL '1 DAY'), 0
           ) - COALESCE(datf.debet_amount, 0) + COALESCE(datf.credit_amount, 0) AS balance_out,
           COALESCE(
                (SELECT dabf.balance_out_rub
                   FROM dm.dm_account_balance_f dabf 
                  WHERE dabf.account_rk = mad.account_rk 
                    AND dabf.on_date = i_OnDate - INTERVAL '1 DAY'), 0
           ) - COALESCE(datf.debet_amount_rub, 0) + COALESCE(datf.credit_amount_rub, 0) AS balance_out_rub
      FROM ds.md_account_d mad
           LEFT JOIN dm.dm_account_turnover_f datf
                  ON mad.account_rk = datf.account_rk
                 AND datf.on_date = i_OnDate
     WHERE mad.char_type = 'П';

    end_time := CURRENT_TIMESTAMP;

    INSERT INTO logs.data_load_logs (start_time, end_time, duration, table_name, message)
    VALUES (start_time, end_time, end_time - start_time, 'dm_account_balance_f', 
            'Загрузка данных по остаткам за ' || i_OnDate || ' завершена успешно.');
END;
$$;

DO $$
DECLARE
    var_date DATE := '2018-01-01';
BEGIN
    WHILE var_date <= '2018-01-31' LOOP
        CALL ds.fill_account_balance_f(var_date);
        var_date := var_date + INTERVAL '1 day';
    END LOOP;
END $$;

/*SELECT * FROM dm.dm_account_turnover_f
ORDER BY account_rk, on_date;

SELECT * FROM dm.dm_account_balance_f
ORDER BY account_rk, on_date;

SELECT * FROM logs.data_load_logs;