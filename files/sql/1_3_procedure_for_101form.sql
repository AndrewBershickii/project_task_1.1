CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    start_time := CURRENT_TIMESTAMP;

    DELETE FROM dm.dm_f101_round_f
    WHERE from_date = DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH';

    WITH BalanceIn AS (
        SELECT mlas.chapter,
               mlas.ledger_account,
               mad.char_type,
               dabf.on_date,
               SUM(CASE WHEN mad.currency_code IN ('643', '810') THEN dabf.balance_out_rub ELSE 0 END) AS balance_in_rub,
               SUM(CASE WHEN mad.currency_code NOT IN ('643', '810') THEN dabf.balance_out ELSE 0 END) AS balance_in_val,
               SUM(COALESCE(dabf.balance_out_rub, 0)) AS balance_in_total
          FROM ds.md_ledger_account_s mlas
          LEFT JOIN ds.md_account_d mad
                 ON CAST(mlas.ledger_account AS TEXT) = SUBSTRING(mad.account_number FROM 1 FOR 5)
          LEFT JOIN dm.dm_account_balance_f dabf
                 ON mad.account_rk = dabf.account_rk
                AND dabf.on_date = DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH 1 DAY'
         GROUP BY mlas.chapter, mlas.ledger_account, mad.char_type, dabf.on_date
    ),
    TurnDebCre AS (
        SELECT mlas.chapter,
               mlas.ledger_account,
               mad.char_type,
               SUM(CASE WHEN mad.currency_code IN ('643', '810') THEN COALESCE(datf.debet_amount_rub, 0) ELSE 0 END) AS turn_deb_rub,
               SUM(CASE WHEN mad.currency_code NOT IN ('643', '810') THEN COALESCE(datf.debet_amount, 0) ELSE 0 END) AS turn_deb_val,
               SUM(COALESCE(datf.debet_amount_rub, 0)) AS turn_deb_total,
               SUM(CASE WHEN mad.currency_code IN ('643', '810') THEN COALESCE(datf.credit_amount_rub, 0) ELSE 0 END) AS turn_cre_rub,
               SUM(CASE WHEN mad.currency_code NOT IN ('643', '810') THEN COALESCE(datf.credit_amount, 0) ELSE 0 END) AS turn_cre_val,
               SUM(COALESCE(datf.credit_amount_rub, 0)) AS turn_cre_total
          FROM ds.md_ledger_account_s mlas
          LEFT JOIN ds.md_account_d mad
                 ON CAST(mlas.ledger_account AS TEXT) = SUBSTRING(mad.account_number FROM 1 FOR 5)
          LEFT JOIN dm.dm_account_turnover_f datf
                 ON mad.account_rk = datf.account_rk
                AND DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH' = DATE_TRUNC('MONTH', datf.on_date::DATE)
         GROUP BY mlas.chapter, mlas.ledger_account, mad.char_type
    ),
    BalanceOut AS (
        SELECT mlas.chapter,
               mlas.ledger_account,
               mad.char_type,
               SUM(CASE WHEN mad.currency_code IN ('643', '810') THEN dabf.balance_out_rub ELSE 0 END) AS balance_out_rub,
               SUM(CASE WHEN mad.currency_code NOT IN ('643', '810') THEN dabf.balance_out ELSE 0 END) AS balance_out_val,
               SUM(COALESCE(dabf.balance_out_rub, 0)) AS balance_out_total
          FROM ds.md_ledger_account_s mlas
          LEFT JOIN ds.md_account_d mad
                 ON CAST(mlas.ledger_account AS TEXT) = SUBSTRING(mad.account_number FROM 1 FOR 5)
          LEFT JOIN dm.dm_account_balance_f dabf
                 ON mad.account_rk = dabf.account_rk
                AND dabf.on_date = DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 DAY'
         GROUP BY mlas.chapter, mlas.ledger_account, mad.char_type
    )
    INSERT INTO dm.dm_f101_round_f (
									from_date,
									to_date,
									chapter,
									ledger_account,
									characteristic,
									balance_in_rub,
									balance_in_val,
									balance_in_total,
									turn_deb_rub,
									turn_deb_val,
									turn_deb_total,
									turn_cre_rub,
									turn_cre_val,
									turn_cre_total,
									balance_out_rub,
									balance_out_val,
									balance_out_total)
    SELECT DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 MONTH' AS from_date,
           DATE_TRUNC('MONTH', i_OnDate) - INTERVAL '1 DAY' AS to_date,
           bi.chapter,
           bi.ledger_account,
           bi.char_type,
           bi.balance_in_rub,
           bi.balance_in_val,
           bi.balance_in_total,
           tdc.turn_deb_rub,
           tdc.turn_deb_val,
           tdc.turn_deb_total,
           tdc.turn_cre_rub,
           tdc.turn_cre_val,
           tdc.turn_cre_total,
           bo.balance_out_rub,
           bo.balance_out_val,
           bo.balance_out_total
      FROM BalanceIn AS bi
      LEFT JOIN TurnDebCre AS tdc
            ON bi.ledger_account = tdc.ledger_account
      LEFT JOIN BalanceOut AS bo
            ON bi.ledger_account = bo.ledger_account;

    end_time := CURRENT_TIMESTAMP;

    INSERT INTO logs.data_load_logs (start_time, end_time, duration, table_name, message)
    VALUES (start_time, end_time, end_time - start_time, 'dm_f101_round_f', 
            'Расчет данных для витрины F101 за ' || i_OnDate || 'завершен успешно.');
END;
$$;


-- call dm.fill_f101_round_f('2018-02-01');
