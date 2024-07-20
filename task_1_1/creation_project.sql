create schema if not exists ds;
create schema if not exists logs;

drop table if exists logs.logs;
create table if not exists logs.logs(log_id serial, log_info text, log_date_time timestamp);

CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    "on_date" DATE NOT NULL,
    "account_rk" NUMERIC NOT NULL,
    "currency_rk" NUMERIC,
    "balance_out" FLOAT,
    CONSTRAINT pk_balance PRIMARY KEY ("on_date", "account_rk")
);

drop table if exists ds.ft_posting_f;
CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
    "OPER_DATE" DATE NOT NULL,
    "CREDIT_ACCOUNT_RK" NUMERIC NOT NULL,
    "DEBET_ACCOUNT_RK" NUMERIC NOT NULL,
    "CREDIT_AMOUNT" FLOAT,
    "DEBET_AMOUNT" FLOAT
);


CREATE TABLE IF NOT EXISTS ds.md_account_d (
    "data_actual_date" DATE NOT NULL,
    "data_actual_end_date" DATE NOT NULL,
    "account_rk" NUMERIC NOT NULL,
    "account_number" VARCHAR(20) NOT NULL,
    "char_type" VARCHAR(1) NOT NULL,
    "currency_rk" NUMERIC NOT NULL,
    "currency_code" VARCHAR(3) NOT NULL,
    CONSTRAINT pk_account PRIMARY KEY ("data_actual_date", "account_rk")
);


CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    "currency_rk" NUMERIC NOT NULL,
    "data_actual_date" DATE NOT NULL,
    "data_actual_end_date" DATE,
    "currency_code" FLOAT,
    "code_iso_char" VARCHAR(3),
    CONSTRAINT pk_currency PRIMARY KEY ("currency_rk", "data_actual_date")
);


CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
    "data_actual_date" DATE NOT NULL,
    "data_actual_end_date" DATE,
    "currency_rk" NUMERIC NOT NULL,
    "reduced_cource" FLOAT,
    "code_iso_num" VARCHAR(3),
    CONSTRAINT pk_exchange PRIMARY KEY ("data_actual_date", "currency_rk")
);


CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
    "chapter" CHAR(1),
    "chapter_name" VARCHAR(16),
    "section_number" INT,
    "section_name" VARCHAR(22),
    "subsection_name" VARCHAR(21),
    "ledger1_account" INT,
    "ledger1_account_name" VARCHAR(47),
    "ledger_account" INT NOT NULL,
    "ledger_account_name" VARCHAR(153),
    "characteristic" CHAR(1),
    "is_resident" INT,
    "is_reserve" INT,
    "is_reserved" INT,
    "is_loan" INT,
    "is_reserved_assets" INT,
    "is_overdue" INT,
    "is_interest" INT,
    "pair_account" VARCHAR(5),
    "start_date" DATE NOT NULL,
    "end_date" DATE,
    "is_rub_only" INT,
    "min_term" VARCHAR(1),
    "min_term_measure" VARCHAR(1),
    "max_term" VARCHAR(1),
    "max_term_measure" VARCHAR(1),
    "ledger_acc_full_name_translit" VARCHAR(1),
    "is_revaluation" VARCHAR(1),
    "is_correct" VARCHAR(1),
    CONSTRAINT pk_ledger_account PRIMARY KEY ("ledger1_account", "start_date")
);




