CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f
(
    from_date date,
    to_date date,
    chapter char(1),
    ledger_account char(5),
    characteristic char(1),
    balance_in_rub numeric(23,8),
    r_balance_in_rub numeric(23,8),
    balance_in_val numeric(23,8),
    r_balance_in_val numeric(23,8),
    balance_in_total numeric(23,8),
    r_balance_in_total numeric(23,8),
    turn_deb_rub numeric(23,8),
    r_turn_deb_rub numeric(23,8),
    turn_deb_val numeric(23,8),
    r_turn_deb_val numeric(23,8),
    turn_deb_total numeric(23,8),
    r_turn_deb_total numeric(23,8),
    turn_cre_rub numeric(23,8),
    r_turn_cre_rub numeric(23,8),
    turn_cre_val numeric(23,8),
    r_turn_cre_val numeric(23,8),
    turn_cre_total numeric(23,8),
    r_turn_cre_total numeric(23,8),
    balance_out_rub numeric(23,8),
    r_balance_out_rub numeric(23,8),
    balance_out_val numeric(23,8),
    r_balance_out_val numeric(23,8),
    balance_out_total numeric(23,8),
    r_balance_out_total numeric(23,8)
);

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate date)
as $$
declare
    v_from_date date;
    v_to_date date;
begin
    v_from_date := (i_OnDate - interval '1 month')::date;
    v_to_date := (i_OnDate - interval '1 day')::date;
   
   call ds.create_log('Start calculating Data Mart: f101_round_f. For the period: ' || v_from_date || ' to  ' || v_to_date);
   
   delete from dm.dm_f101_round_f where from_date = v_from_date and to_date = v_to_date;

    -- Вставка новых данных
    insert into dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic, 
        balance_in_rub, balance_in_val, balance_in_total, 
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    select 
        v_from_date,
        v_to_date,
        led.chapter,
        left(acc.account_number, 5),
        acc.char_type,
        -- balance_in
        SUM(case when acc.currency_code in ('810', '643') then COALESCE(prev_bal.balance_out_rub, 0) else 0 end) as BALANCE_IN_RUB,
        SUM(case when acc.currency_code not in ('810', '643') then COALESCE(prev_bal.balance_out_rub, 0) else 0 end) as BALANCE_IN_VAL,
        SUM(COALESCE(prev_bal.balance_out_rub, 0)) as BALANCE_IN_TOTAL,
        -- turnover debet
        SUM(case when acc.currency_code in ('810', '643') then COALESCE(turn.debet_amount_rub, 0) else 0 end) as TURN_DEB_RUB,
        SUM(case when acc.currency_code not in ('810', '643') then COALESCE(turn.debet_amount_rub, 0) else 0 end) as TURN_DEB_VAL,
        SUM(COALESCE(turn.debet_amount_rub, 0)) as TURN_DEB_TOTAL,
        -- turnover credit
        SUM(case when acc.currency_code in ('810', '643') then COALESCE(turn.credit_amount_rub, 0) else 0 end) as TURN_CRE_RUB,
        SUM(case when acc.currency_code not in ('810', '643') then COALESCE(turn.credit_amount_rub, 0) else 0 end) as TURN_CRE_VAL,
        SUM(COALESCE(turn.credit_amount_rub, 0)) as TURN_CRE_TOTAL,
        -- balance_out
        SUM(case when acc.currency_code in ('810', '643') then COALESCE(cur_bal.balance_out_rub, 0) else 0 end ) as BALANCE_OUT_RUB,
        SUM(case when acc.currency_code not in ('810', '643') then COALESCE(cur_bal.balance_out_rub, 0) else 0 end) as BALANCE_OUT_VAL,
        SUM(COALESCE(cur_bal.balance_out_rub, 0)) AS BALANCE_OUT_TOTAL
    from 
        ds.md_account_d acc
    left join 
        ds.md_ledger_account_s led on acc.account_rk = led.ledger_account
    left join 
        dm.dm_account_balance_f prev_bal on acc.account_rk = prev_bal.account_rk and prev_bal.on_date = v_from_date - interval '1 day'
    left join 
        (select account_rk, SUM(debet_amount_rub) as debet_amount_rub, SUM(credit_amount_rub) as credit_amount_rub
            from dm.dm_account_turnover_f
            where on_date between v_from_date and v_to_date
            group by account_rk
        ) turn on acc.account_rk = turn.account_rk
    left join 
        dm.dm_account_balance_f cur_bal ON acc.account_rk = cur_bal.account_rk and cur_bal.on_date = v_to_date
    where acc.data_actual_date <= v_to_date and (acc.data_actual_end_date is null or acc.data_actual_end_date >= v_from_date)
    group by led.chapter, left(acc.account_number, 5), acc.char_type;
    
    call ds.create_log('End calculating Data Mart: f101_round_f. For the period: ' || v_from_date || ' to  ' || v_to_date);
end;
$$ language plpgsql;

call dm.fill_f101_round_f('2018-02-01'::date);

--select * from dm.dm_f101_round_f;

--select * from logs.logs l;




