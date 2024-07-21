create schema if not exists dm;

create table if not exists dm.dm_account_turnover_f(
	on_date date,
	account_rk numeric,
	credit_amount numeric(23,8),
	credit_amount_rub numeric(23,8),
	debet_amount numeric(23,8),
	debet_amount_rub numeric(23,8)
	);

create table if not exists dm.dm_account_balance_f(
	on_date date,
	account_rk numeric,
	balance_out float,
	balance_out_rub float
	);
truncate dm.dm_account_balance_f;
insert into dm.dm_account_balance_f (
    on_date,
    account_rk,
    balance_out,
    balance_out_rub
)select
    bf.on_date,
    bf.account_rk,
    bf.balance_out,
    bf.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
from ds.ft_balance_f bf left join ds.md_exchange_rate_d er
on bf.currency_rk = er.currency_rk and bf.on_date between er.data_actual_date and er.data_actual_end_date
where bf.on_date = '2017-12-31'::date;
CREATE OR REPLACE PROCEDURE ds.create_log(info text)
as $$
begin
	insert into logs.logs (log_info, log_date_time) values (info, now());
end;
$$ language plpgsql;

CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_f(i_OnDate date)
as $$
begin
	delete from dm.dm_account_turnover_f where on_date=i_OnDate;
	call ds.create_log('Start calculating account turnover for ' || i_OnDate);
	
    with credit_sum as (
        select "CREDIT_ACCOUNT_RK" as credit_account_rk, sum("CREDIT_AMOUNT") as credit_amount
        from ds.ft_posting_f
        where "OPER_DATE" = i_OnDate 
        group by "CREDIT_ACCOUNT_RK"
    ),
    debet_sum as (
        select "DEBET_ACCOUNT_RK" as debet_account_rk, sum("DEBET_AMOUNT") as debet_amount
        from ds.ft_posting_f
        where "OPER_DATE" = i_OnDate 
        group by "DEBET_ACCOUNT_RK"
    ),
    all_accounts as (
        select credit_account_rk as account_rk
        from credit_sum
        union
        select debet_account_rk as account_rk
        from debet_sum
    )
    insert into dm.dm_account_turnover_f (
        on_date, 
        account_rk, 
        credit_amount, 
        credit_amount_rub, 
        debet_amount, 
        debet_amount_rub
    )
    select 
        i_OnDate as on_date,
        aa.account_rk,
        COALESCE(cs.credit_amount, 0) AS credit_amount,
        COALESCE(cs.credit_amount, 0) * COALESCE(merd.reduced_cource, 1) AS credit_amount_rub,
        COALESCE(ds.debet_amount, 0) AS debet_amount,
        COALESCE(ds.debet_amount, 0) * COALESCE(merd.reduced_cource, 1) AS debet_amount_rub
    from 
        all_accounts aa
    left join 
        credit_sum cs on aa.account_rk = cs.credit_account_rk
    left join 
        debet_sum ds on aa.account_rk = ds.debet_account_rk
    left join 
        ds.md_exchange_rate_d merd 
    on 
        merd.currency_rk = (
            select currency_rk 
            from ds.ft_balance_f bf
            where aa.account_rk=bf.account_rk
        )
    and 
        i_OnDate between merd.data_actual_date and merd.data_actual_end_date;
       
    call ds.create_log('End calculating account turnover for ' || i_OnDate);
end;
$$ language plpgsql;

CREATE OR REPLACE PROCEDURE dm.call_fill_account_turnover(date_start date, date_end date)
as $$
declare 
	i_OnDate date;
	begin
		call ds.create_log('Start calculating Data Mart: account_turnover_f. For the period: ' || date_start || ' to ' || date_end);
		for i_OnDate in (SELECT * FROM generate_series(date_start, date_end, '1 day') s(i_OnDate)) loop
			call dm.fill_account_turnover_f(i_OnDate);
		end loop;
		call ds.create_log('End calculating Data Mart: account_turnover_f. For the period: ' || date_start || ' to  ' || date_end);
	end;
$$ language plpgsql;

CREATE OR REPLACE PROCEDURE dm.call_fill_account_balance(date_start date, date_end date)
as $$
declare 
	i_OnDate date;
	begin
		call ds.create_log('Start calculating Data Mart: account_balance_f. For the period: ' || date_start || ' to  ' || date_end);
		for i_OnDate in (SELECT * FROM generate_series(date_start, date_end, '1 day') s(i_OnDate)) loop
			call dm.fill_account_balance_f(i_OnDate);
		end loop;
		call ds.create_log('End calculating Data Mart: account_balance_f. For the period: ' || date_start || ' to  ' || date_end);
	end;
$$ language plpgsql;

CREATE OR REPLACE PROCEDURE dm.fill_account_balance_f(i_OnDate date)
as $$
	begin
		delete from dm.dm_account_balance_f where on_date=i_OnDate;
		call ds.create_log('Start calculating account balance for ' || i_OnDate);
		with turnover_actual_date as (select
			atf.account_rk,
			atf.credit_amount,
			atf.credit_amount_rub,
			atf.debet_amount,
			atf.debet_amount_rub
			from dm.dm_account_turnover_f atf
			where on_date=i_OnDate
		)
		insert into dm.dm_account_balance_f(
       		on_date,
       		account_rk,
       		balance_out,
       		balance_out_rub
       ) select i_OnDate,
       			acc.account_rk,
       			case 
            		when acc.char_type = 'А' then coalesce(prev_bal.balance_out, 0) + coalesce(tad.debet_amount, 0) - coalesce(tad.credit_amount, 0)
            		when acc.char_type = 'П' then coalesce(prev_bal.balance_out, 0) - coalesce(tad.debet_amount, 0) + coalesce(tad.credit_amount, 0)  
            	end as balance_out,
            	case 
            		when acc.char_type = 'А' then coalesce(prev_bal.balance_out_rub, 0) + coalesce(tad.debet_amount_rub, 0) - coalesce(tad.credit_amount_rub, 0)
            		when acc.char_type = 'П' then coalesce(prev_bal.balance_out_rub, 0) - coalesce(tad.debet_amount_rub, 0) + coalesce(tad.credit_amount_rub, 0)
        		end as balance_out_rub
       	from ds.md_account_d acc 
       	left join turnover_actual_date tad on acc.account_rk=tad.account_rk
       	left join (select dabf.account_rk, dabf.balance_out, dabf.balance_out_rub
        from dm.dm_account_balance_f dabf
        where dabf.on_date = i_OnDate - interval '1 day'
    		) prev_bal on acc.account_rk = prev_bal.account_rk
    	where i_OnDate between acc.data_actual_date and acc.data_actual_end_date;
    call ds.create_log('End calculating account balance for ' || i_OnDate);
	end;
$$ language plpgsql;


--select * from dm.dm_account_turnover_f datf;
call dm.call_fill_account_turnover('2018-01-01'::date, '2018-01-31'::date);

call dm.call_fill_account_balance('2018-01-01'::date, '2018-01-31'::date)

--select * from dm.dm_account_balance_f;

--select * from logs.logs l;










	
