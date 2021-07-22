sqlContext.sql("create database kms")

sqlContext.sql("create table src_account (acct_nbr varchar(20),primary_state varchar(10),zip_code varchar(10)) partitioned by (load_date varchar(12)) stored as parquet")
sqlContext.sql("create table account(acct_nbr varchar(20),account_sk_id bigint,primary_state varchar(10),zip_code varchar(10),eff_start_date varchar(12),eff_end_date varchar(12),load_tm varchar(30),hash_key string,eff_flag varchar(2)) stored as parquet")
sqlContext.sql("create table account_stg (acct_nbr varchar(20),account_sk_id bigint,zip_code varchar(10),primary_state varchar(10),eff_start_date varchar(10),eff_end_date varchar(10),load_tm varchar(30),hash_key string,eff_flag varchar(2),upd_ins_flag varchar(1))stored as parquet")

sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('1','TN','TN10001')");
sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('2','TN','TN10002')");
sqlContext.sql("insert into table src_account PARTITION (load_date='2016-11-08') values ('3','TN','TN10003')");

