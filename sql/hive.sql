SHOW DATABASES;

CREATE DATABASE IF NOT EXISTS szt;

USE szt;

SHOW TABLES;

--1 ODS 原始表， 不做改动， 直接加载
DROP TABLE IF EXISTS ods_szt_data;
CREATE EXTERNAL TABLE ods_szt_data(
deal_date String,
close_date String,
card_no String,
deal_value String,
deal_type String,
company_name String,
car_no String,
station String,
conn_mark String,
deal_money String,
equ_no String) 
PARTITIONED BY(DAY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/ods/ods_szt_data';

LOAD DATA INPATH '/warehouse/szt.db/ods/szt-etl-data_2018-09-01.csv' OVERWRITE INTO TABLE ods_szt_data PARTITION(DAY = '2018-09-01');

SELECT * FROM ods_szt_data WHERE DAY = '2018-09-01' LIMIT 10;

SELECT collect_set(deal_type) FROM ods_szt_data; --["地铁出站", "地铁入站", "巴士"]

--2 DWD 过滤掉 巴士， 过滤掉不在运营时间的地铁出入站数据
-- DROP TABLE IF EXISTS dwd_fact_szt_detail;

DROP TABLE IF EXISTS dwd_fact_szt_in_out_detail;--地铁出入站
CREATE EXTERNAL TABLE dwd_fact_szt_in_out_detail(
deal_date String,
close_date String,
card_no String,
deal_value String,
deal_type String,
company_name String,
car_no String,
station String,
conn_mark String,
deal_money String,
equ_no String) 
PARTITIONED BY(DAY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/dwd/dwd_fact_szt_in_out_detail';

INSERT OVERWRITE TABLE dwd_fact_szt_in_out_detail partition(DAY = '2018-09-01')
SELECT deal_date,
       close_date,
       card_no,
       deal_value,
       deal_type,
       company_name,
       car_no,
       station,
       conn_mark,
       deal_money,
       equ_no
FROM ods_szt_data
WHERE deal_type != '巴士'
  AND unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') > unix_timestamp('2018-09-01 06:14:00', 'yyyy-MM-dd HH:mm:ss')
  AND unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') < unix_timestamp('2018-09-01 23:59:00', 'yyyy-MM-dd HH:mm:ss')
  AND DAY = '2018-09-01'
ORDER BY deal_date;


SELECT count(*) FROM dwd_fact_szt_in_out_detail; --780937


-- -- - dwd_fact_szt_in_detail
DROP TABLE IF EXISTS dwd_fact_szt_in_detail;
CREATE EXTERNAL TABLE dwd_fact_szt_in_detail(
deal_date String,
close_date String,
card_no String,
deal_value String,
deal_type String,
company_name String,
car_no String,
station String,
conn_mark String,
deal_money String,
equ_no String) 
PARTITIONED BY(DAY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/dwd/dwd_fact_szt_in_detail';


INSERT OVERWRITE TABLE dwd_fact_szt_in_detail partition(DAY = '2018-09-01')
SELECT deal_date,
       close_date,
       card_no,
       deal_value,
       deal_type,
       company_name,
       car_no,
       station,
       conn_mark,
       deal_money,
       equ_no
FROM dwd_fact_szt_in_out_detail
WHERE deal_type = '地铁入站'
  AND DAY = '2018-09-01'
ORDER BY deal_date ;

SELECT count(*) FROM dwd_fact_szt_in_detail;
--415386


-- -- - dwd_fact_szt_out_detail
DROP TABLE IF EXISTS dwd_fact_szt_out_detail;
CREATE EXTERNAL TABLE dwd_fact_szt_out_detail(
deal_date String,
close_date String,
card_no String,
deal_value String,
deal_type String,
company_name String,
car_no String,
station String,
conn_mark String,
deal_money String,
equ_no String) 
PARTITIONED BY(DAY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/dwd/dwd_fact_szt_out_detail';


SELECT count(*) FROM dwd_fact_szt_out_detail;

INSERT OVERWRITE TABLE dwd_fact_szt_out_detail partition(DAY = '2018-09-01')
SELECT deal_date,
       close_date,
       card_no,
       deal_value,
       deal_type,
       company_name,
       car_no,
       station,
       conn_mark,
       deal_money,
       equ_no
FROM dwd_fact_szt_in_out_detail
WHERE deal_type = '地铁出站'
  AND DAY = '2018-09-01'
ORDER BY deal_date ;--365551


-- --DWS 宽表

DROP TABLE IF EXISTS dws_card_record_day_wide;
CREATE EXTERNAL TABLE dws_card_record_day_wide(
card_no STRING,
deal_date_arr ARRAY < STRING > , 
deal_value_arr ARRAY < STRING > , 
deal_type_arr ARRAY < STRING > , 
company_name_arr ARRAY < STRING > , 
station_arr ARRAY < STRING > , 
conn_mark_arr ARRAY < STRING > , 
deal_money_arr ARRAY < STRING > , 
equ_no_arr ARRAY < STRING > , 
`count` int) 
PARTITIONED BY(DAY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/dws/dws_card_record_day_wide';

INSERT OVERWRITE TABLE dws_card_record_day_wide PARTITION(DAY = '2018-09-01')
SELECT card_no,
       collect_list(deal_date),
       collect_list(deal_value),
       collect_list(deal_type),
       collect_list(company_name),
       collect_list(station),
       collect_list(conn_mark),
       collect_list(deal_money),
       collect_list(equ_no),
       count(*) c
FROM dwd_fact_szt_in_out_detail
WHERE DAY = '2018-09-01'
GROUP BY card_no
ORDER BY c DESC;

--412082

SELECT * FROM dws_card_record_day_wide LIMIT 100;

--ADS 业务表， 当天的表现

DROP TABLE IF EXISTS ads_in_station_day_top;
CREATE EXTERNAL TABLE ads_in_station_day_top(
station STRING,
deal_date_arr ARRAY < STRING > , 
card_no_arr ARRAY < STRING > , 
company_name_arr ARRAY < STRING > , 
equ_no_arr ARRAY < STRING > , 
`count` int) 
PARTITIONED BY(DAY string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/ads/ads_in_station_day_top';

INSERT OVERWRITE TABLE ads_in_station_day_top PARTITION(DAY = '2018-09-01')
SELECT station,
       collect_list(deal_date),
       collect_list(card_no),
       collect_list(company_name),
       collect_list(equ_no),
       count(*) c
FROM dwd_fact_szt_in_detail
WHERE DAY = '2018-09-01'
GROUP BY station
ORDER BY c DESC;

SELECT * FROM ads_in_station_day_top LIMIT 20;

---
DROP TABLE IF EXISTS ads_out_station_day_top;
CREATE EXTERNAL TABLE ads_out_station_day_top(
station STRING,
deal_date_arr ARRAY < STRING > , 
card_no_arr ARRAY < STRING > , 
deal_value_arr ARRAY < STRING > , 
company_name_arr ARRAY < STRING > , 
conn_mark_arr ARRAY < STRING > , 
deal_money_arr ARRAY < STRING > , 
equ_no_arr ARRAY < STRING > , 
`count` int) 
PARTITIONED BY(DAY string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/ads/ads_out_station_day_top';

INSERT OVERWRITE TABLE ads_out_station_day_top PARTITION(DAY = '2018-09-01')
SELECT station,
       collect_list(deal_date),
       collect_list(card_no),
       collect_list(deal_value),
       collect_list(company_name),
       collect_list(conn_mark),
       collect_list(deal_money),
       collect_list(equ_no),
       count(*) c
FROM dwd_fact_szt_out_detail
WHERE DAY = '2018-09-01'
GROUP BY station
ORDER BY c DESC;

SELECT * FROM ads_out_station_day_top LIMIT 20;
SELECT collect_list(station) FROM ads_out_station_day_top;
SELECT collect_list(station) FROM ads_in_station_day_top;

---
---ads_in_out_station_day_top
DROP TABLE IF EXISTS ads_in_out_station_day_top;
CREATE EXTERNAL TABLE ads_in_out_station_day_top(
station STRING,
deal_date_arr ARRAY < STRING > , 
card_no_arr ARRAY < STRING > , 
deal_value_arr ARRAY < STRING > , 
deal_type_arr ARRAY < STRING > , 
company_name_arr ARRAY < STRING > , 
conn_mark_arr ARRAY < STRING > , 
deal_money_arr ARRAY < STRING > , 
equ_no_arr ARRAY < STRING > , 
`count` int) 
PARTITIONED BY(DAY string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/ads/ads_in_out_station_day_top';

INSERT OVERWRITE TABLE ads_in_out_station_day_top PARTITION(DAY = '2018-09-01')
SELECT station,
       collect_list(deal_date),
       collect_list(card_no),
       collect_list(deal_value),
       collect_list(deal_type),
       collect_list(company_name),
       collect_list(conn_mark),
       collect_list(deal_money),
       collect_list(equ_no),
       count(*) c
FROM dwd_fact_szt_in_out_detail
WHERE DAY = '2018-09-01'
GROUP BY station
ORDER BY c DESC;

SELECT * FROM ads_in_out_station_day_top LIMIT 20;

-------------------------------------------------------------
---卡片单日消费排行榜
DROP TABLE IF EXISTS ads_card_deal_day_top;
CREATE EXTERNAL TABLE ads_card_deal_day_top (
card_no STRING,
deal_date_arr ARRAY<STRING>,
deal_sum DOUBLE,
company_name_arr ARRAY<STRING>,
station_arr ARRAY<STRING>,
conn_mark_arr ARRAY<STRING>,
deal_m_sum DOUBLE,
equ_no_arr ARRAY<STRING>,
`count` INT)
PARTITIONED BY(DAY string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/ads/ads_card_deal_day_top';

---
insert overwrite table ads_card_deal_day_top partition (day='2018-09-01')
SELECT
    t1.card_no,
    t1.deal_date_arr,
    t2.deal_sum,
    t1.company_name_arr,
    t1.station_arr,
    t1.conn_mark_arr,
    t3.deal_m_sum,
    t1.equ_no_arr,
    t1.`count`
from
    dws_card_record_day_wide as t1,
    (SELECT card_no, sum(deal_v) OVER(PARTITION BY card_no) AS deal_sum FROM dws_card_record_day_wide LATERAL VIEW explode(deal_value_arr) tmp as deal_v )t2,
    (SELECT card_no, sum(deal_m) OVER(PARTITION BY card_no) AS deal_m_sum FROM dws_card_record_day_wide LATERAL VIEW explode(deal_money_arr) tmp as deal_m )t3

WHERE t1.day='2018-09-01'  AND
    t1.card_no = t2.card_no AND
    t2.card_no = t3.card_no
ORDER BY t2.deal_sum DESC--ok
;--ok

SELECT * from ads_card_deal_day_top LIMIT 100;
----------
SELECT card_no, sum(deal_v) OVER(PARTITION BY card_no) AS deal_sum
FROM dws_card_record_day_wide LATERAL VIEW explode(deal_value_arr) tmp as deal_v ORDER BY deal_sum DESC;--ok

SELECT sum(c1) FROM
    dws_card_record_day_wide LATERAL VIEW explode(deal_value_arr) tmp as c1
WHERE card_no='HHAAJICJB';--ok

-- 分组
SELECT
    card_no,
    sum(deal_value) OVER(PARTITION BY card_no) as deal_sum
FROM
    dwd_fact_szt_out_detail
ORDER BY deal_sum desc
;--ok
-----------------------------

