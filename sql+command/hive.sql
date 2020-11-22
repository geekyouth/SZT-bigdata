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

SELECT collect_set(company_name) FROM dwd_fact_szt_out_detail; 
--["地铁一号线","地铁四号线","地铁九号线","地铁五号线","地铁十一号线","地铁二号线","地铁三号线","地铁七号线"]

-- ads_line_send_passengers_day_top
DROP TABLE IF EXISTS ads_line_send_passengers_day_top;
CREATE EXTERNAL TABLE ads_line_send_passengers_day_top(
company_name String,
`count` int
)
PARTITIONED BY(DAY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/warehouse/szt.db/ads/ads_line_send_passengers_day_top';

-- 查询 dwd_fact_szt_in_out_detail 进出站详情表 ，
---------------------------------------------------------------------------
-- 进站，线路 +1
-- 出站直达，线路不加，-- 什么也不做
-- 出站联程，线路 +1.
--t1
SELECT company_name,
    deal_type,
    conn_mark,
    count(*) c
FROM dwd_fact_szt_in_out_detail
WHERE DAY = '2018-09-01' and deal_type='地铁入站'
GROUP BY company_name,deal_type,conn_mark
ORDER BY c DESC ;
--LIMIT 10;

--t2
SELECT company_name,
    deal_type,
    conn_mark,
    count(*) c
FROM dwd_fact_szt_in_out_detail
WHERE DAY = '2018-09-01' and deal_type='地铁出站' and conn_mark='1'
GROUP BY company_name,deal_type,conn_mark
ORDER BY c DESC ;

--t3
INSERT OVERWRITE TABLE ads_line_send_passengers_day_top PARTITION(DAY = '2018-09-01')
SELECT t1.company_name,
    t1.c+t2.c AS c
FROM 
    (SELECT company_name,
        deal_type,
        conn_mark,
        count(*) c
    FROM dwd_fact_szt_in_out_detail
    WHERE DAY = '2018-09-01' and deal_type='地铁入站'
    GROUP BY company_name,deal_type,conn_mark) 
    t1,
    
    (SELECT company_name,
        deal_type,
        conn_mark,
        count(*) c
    FROM dwd_fact_szt_in_out_detail
    WHERE DAY = '2018-09-01' and deal_type='地铁出站' and conn_mark='1'
    GROUP BY company_name,deal_type,conn_mark) 
    t2
    
WHERE t1.company_name=t2.company_name 
ORDER BY c DESC ;

select * from ads_line_send_passengers_day_top;


-- 每日运输乘客最多的区间排行榜
----------------------------------------------------------------------------------
drop table if exists dws_in_out_sorted_card_date;
create external table dws_in_out_sorted_card_date (
card_no string,
deal_date string,
deal_type string,
station string
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/dws/dws_in_out_sorted_card_date';

insert overwrite table dws_in_out_sorted_card_date partition (day="2018-09-01")
select 
card_no,
deal_date,
deal_type,
station
from dwd_fact_szt_in_out_detail
where day="2018-09-01"
order by card_no, deal_date;

----------------------------------------------------------------------------------
drop table if exists temp02;
create table temp02 as
select
card_no,
deal_date,
deal_type,
station,
concat_ws('@', deal_type, station) deal_type_station
from dws_in_out_sorted_card_date;

drop table if exists temp03;
create table temp03 as
select
card_no,
deal_date,
deal_type_station,
LEAD(deal_type_station,1) over(partition by card_no order by deal_date) as next_station
from temp02;

drop table if exists temp04;
create table temp04 as
select
card_no,
deal_type_station,
next_station,
concat_ws('>', deal_type_station, next_station) as station2station
from
temp03 where 
substr(deal_type_station,0,4)='地铁入站'
and 
substr(next_station,0,4)='地铁出站'
;

drop table if exists temp05;
create table temp05 as
select 
regexp_replace(station2station,'地铁入站@|地铁出站@','') short_stations
from temp04;

drop table if exists temp06;
create table temp06 as
select 
short_stations,
count(*) as `count`
from temp05
group by short_stations
order by `count` desc;
----------------------------------------------------------------------------------

-- 合并
drop table if exists ads_stations_send_passengers_day_top;
create external table ads_stations_send_passengers_day_top(
short_stations string,
`count` int
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_stations_send_passengers_day_top';

insert overwrite table ads_stations_send_passengers_day_top partition (day="2018-09-01")
select 
short_stations,
count(*) as `count`
from (
    select 
    regexp_replace(station2station,'地铁入站@|地铁出站@','') short_stations
    from (
        select
        card_no,
        deal_type_station,
        next_station,
        concat_ws('>', deal_type_station, next_station) as station2station
        from (
            select
            card_no,
            deal_date,
            deal_type_station,
            LEAD(deal_type_station,1) over(partition by card_no order by deal_date) as next_station
            from (
                select
                card_no,
                deal_date,
                deal_type,
                station,
                concat_ws('@', deal_type, station) deal_type_station
                from dws_in_out_sorted_card_date
                where day='2018-09-01'
            ) temp02
        ) temp03 where 
        substr(deal_type_station,0,4)='地铁入站'
        and 
        substr(next_station,0,4)='地铁出站'
    ) temp04
) temp05
group by short_stations
order by `count` desc;
----------------------------------------------------------------------------------

----------------------------------------------------------------------------------
--- 每条线路单程直达乘客耗时平均值排行榜
--建宽表
drop table if exists dws_in_out_sorted_card_date_wide;
create external table dws_in_out_sorted_card_date_wide (
card_no string,
deal_date string,
ts string,
deal_value string,
deal_type string,
company_name string,
station string,
conn_mark string,
deal_money string,
equ_no string
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/dws/dws_in_out_sorted_card_date_wide';

insert overwrite table dws_in_out_sorted_card_date_wide partition (day="2018-09-01")
select 
card_no,
deal_date,
unix_timestamp(deal_date) ts,
deal_value,
deal_type,
company_name,
station,
conn_mark,
deal_money,
equ_no
from dwd_fact_szt_in_out_detail
where day="2018-09-01"
order by card_no, deal_date;

----------------------------------------------------------------------------------

-- 拼接单程，起始时间
drop table if exists temp02;
create table temp02 COMMENT '临时中间表，拼接单程+起始时间戳'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp02'
as select
card_no,
ts,
deal_type,
company_name,
station,
conn_mark,
concat_ws('@',ts, deal_type, station) ts_deal_type_station
from dws_in_out_sorted_card_date_wide
where day='2018-09-01'
;

-- 寻找下一程
drop table if exists temp03;
create table temp03 COMMENT '临时中间表，开窗寻找下一程'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp03'
as select
card_no,
ts,
company_name,
conn_mark,
ts_deal_type_station,
LEAD(ts_deal_type_station,1) over(partition by card_no order by ts) as ts_next_station
from temp02;


select substr('1535767701@地铁出站@少年宫',0,10);
select substr('1535767701@地铁出站@少年宫',11,1);
select substr('1535765725@地铁入站@向西村',12,4);
select substr('1535767701@地铁出站@少年宫',0,10)-substr('1535765725@地铁入站@向西村',0,10) as time_s;
-- 过滤合法行程
drop table if exists temp04;
create table temp04 COMMENT '临时中间表，过滤合法记录'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp04'
as select
card_no,
company_name,
ts_deal_type_station,
ts_next_station,
-- 求时差
substr(ts_next_station,0,10)-substr(ts_deal_type_station,0,10) as time_s
from
temp03 where 
substr(ts_deal_type_station ,12,4)='地铁入站'
and 
substr(ts_next_station ,12,4)='地铁出站'
and 
conn_mark ='0'
;


-- 分组求平均
drop table if exists ads_line_single_ride_average_time_day_top;
create external table ads_line_single_ride_average_time_day_top(
company_name string,
avg_time_s double
)COMMENT '每条线路单程直达乘客耗时平均值排行榜'
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_line_single_ride_average_time_day_top';

insert overwrite table ads_line_single_ride_average_time_day_top partition (day="2018-09-01")
select 
company_name,
avg(time_s) avg_time_s
from temp04
group by company_name
order by avg_time_s 
;

select * from ads_line_single_ride_average_time_day_top;
----------------------------------------------------------------------------------
-- 合并

drop table if exists ads_line_single_ride_average_time_day_top;
create external table ads_line_single_ride_average_time_day_top(
company_name string,
avg_time_s double
)COMMENT '每条线路单程直达乘客耗时平均值排行榜'
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_line_single_ride_average_time_day_top';

insert overwrite table ads_line_single_ride_average_time_day_top partition (day="2018-09-01")
select 
company_name,
avg(time_s) avg_time_s
from (
  select
  card_no,
  company_name,
  ts_deal_type_station,
  ts_next_station,
  -- 求时差
  substr(ts_next_station,0,10)-substr(ts_deal_type_station,0,10) as time_s
  from (
    select
    card_no,
    ts,
    company_name,
    conn_mark,
    ts_deal_type_station,
    LEAD(ts_deal_type_station,1) over(partition by card_no order by ts) as ts_next_station
    from (
      select
      card_no,
      ts,
      deal_type,
      company_name,
      station,
      conn_mark,
      concat_ws('@',ts, deal_type, station) ts_deal_type_station
      from dws_in_out_sorted_card_date_wide
      where day='2018-09-01'
      ) temp02
    ) temp03 where 
  substr(ts_deal_type_station ,12,4)='地铁入站'
  and 
  substr(ts_next_station ,12,4)='地铁出站'
  and 
  conn_mark ='0'
) temp04
group by company_name
order by avg_time_s 
;


-----------------------------------------------------------------------------------
--所有乘客通勤时间平均值
--ads_all_passengers_single_ride_spend_time_average

-- 拼接单程，起始时间
drop table if exists temp02;
create table temp02 COMMENT '临时中间表，拼接单程+起始时间戳'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp02'
as select
card_no,
ts,
deal_type,
company_name,
station,
conn_mark,
concat_ws('@',ts, deal_type, station) ts_deal_type_station
from dws_in_out_sorted_card_date_wide where day='2018-09-01'
;

-- 寻找下一程
drop table if exists temp03;
create table temp03 COMMENT '临时中间表，开窗寻找下一程'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp03'
as select
card_no,
ts,
company_name,
conn_mark,
ts_deal_type_station,
LEAD(ts_deal_type_station,1) over(partition by card_no order by ts) as ts_next_station
from temp02;


select substr('1535767701@地铁出站@少年宫',0,10);
select substr('1535767701@地铁出站@少年宫',11,1);
select substr('1535765725@地铁入站@向西村',12,4);
select substr('1535767701@地铁出站@少年宫',0,10)-substr('1535765725@地铁入站@向西村',0,10) as time_s;
-- 过滤合法行程，允许多程，添加联程字段
drop table if exists temp04;
create table temp04 COMMENT '临时中间表，过滤合法记录'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp04'
as select
card_no,
company_name,
conn_mark,
ts_deal_type_station,
ts_next_station,
-- 求时差
substr(ts_next_station,0,10)-substr(ts_deal_type_station,0,10) as time_s
from
temp03 where 
substr(ts_deal_type_station ,12,4)='地铁入站'
and 
substr(ts_next_station ,12,4)='地铁出站'
;


-- 全表求平均
drop table if exists ads_all_passengers_single_ride_spend_time_average;
create external table ads_all_passengers_single_ride_spend_time_average (
all_avg_time_s double
)COMMENT '所有乘客通勤时间平均值'
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_all_passengers_single_ride_spend_time_average';

insert overwrite table ads_all_passengers_single_ride_spend_time_average partition (day="2018-09-01")
select 
avg(time_s) all_avg_time_s
from temp04
;

select * from ads_all_passengers_single_ride_spend_time_average;
----------------------------------------------------------------------------------
-- 合并，注意这里有一些可以复用的临时表

drop table if exists ads_all_passengers_single_ride_spend_time_average;
create external table ads_all_passengers_single_ride_spend_time_average (
all_avg_time_s double
)COMMENT '所有乘客通勤时间平均值'
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_all_passengers_single_ride_spend_time_average';

SET hive.load.dynamic.partitions.thread=15;

insert overwrite table ads_all_passengers_single_ride_spend_time_average partition (day="2018-09-01")
select 
avg(time_s) all_avg_time_s
from (
  select
  card_no,
  company_name,
  conn_mark,
  ts_deal_type_station,
  ts_next_station,
  -- 求时差
  substr(ts_next_station,0,10)-substr(ts_deal_type_station,0,10) as time_s
  from (
    select
    card_no,
    ts,
    company_name,
    conn_mark,
    ts_deal_type_station,
    LEAD(ts_deal_type_station,1) over(partition by card_no order by ts) as ts_next_station
    from (
      select
      card_no,
      ts,
      deal_type,
      company_name,
      station,
      conn_mark,
      concat_ws('@',ts, deal_type, station) ts_deal_type_station
      from dws_in_out_sorted_card_date_wide 
      where day='2018-09-01'
      ) temp02
    ) temp03 where 
  substr(ts_deal_type_station ,12,4)='地铁入站'
  and 
  substr(ts_next_station ,12,4)='地铁出站'
) temp04
;

select * from ads_all_passengers_single_ride_spend_time_average;
-----------------------------------------------------------------------------------


--深圳地铁乘客单程通勤时间排行榜 ads_passenger_spend_time_day_top

--合并 过滤合法行程+ ads 建表
drop table if exists ads_passenger_spend_time_day_top;
create external table ads_passenger_spend_time_day_top
(
  card_no string,
  company_name string,
  ts_deal_type_station string,
  ts_next_station string,
  time_s double
)
COMMENT '通勤时间排行榜倒序'
partitioned by (day string) row format delimited fields terminated by ',' 
location '/warehouse/szt.db/ads/ads_passenger_spend_time_day_top';

insert overwrite table ads_passenger_spend_time_day_top partition (day='2018-09-01')
select
card_no,
company_name,
ts_deal_type_station,
ts_next_station,
substr(ts_next_station,0,10)-substr(ts_deal_type_station,0,10) as time_s
from (
  select
  card_no,
  ts,
  company_name,
  conn_mark,
  ts_deal_type_station,
  LEAD(ts_deal_type_station,1) over(partition by card_no order by ts) as ts_next_station
  from 
  (
    select
    card_no,
    ts,
    deal_type,
    company_name,
    station,
    conn_mark,
    concat_ws('@',ts, deal_type, station) ts_deal_type_station
    from dws_in_out_sorted_card_date_wide 
    where day='2018-09-01'
  ) temp02
)
temp03 where 
substr(ts_deal_type_station ,12,4)='地铁入站'
and 
substr(ts_next_station ,12,4)='地铁出站'
order by time_s desc
;

select * from ads_passenger_spend_time_day_top;
-----------------------------------------------------------------

-----------------------------------------------------------------

--内部表 每个站点入站闸机数量  ads_station_in_equ_num_top
--内部表 每个站点出站闸机数量  ads_station_out_equ_num_top

drop table if exists ads_station_in_equ_num_top;
create table ads_station_in_equ_num_top 
row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_station_in_equ_num_top'
as
select company_name,station,
count(distinct equ_no) c
from dwd_fact_szt_in_detail
group by company_name,station
order by c desc
;

select * from ads_station_in_equ_num_top;


drop table if exists ads_station_out_equ_num_top;
create table ads_station_out_equ_num_top 
row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_station_out_equ_num_top'
as
select company_name,station,
count(distinct equ_no) c
from dwd_fact_szt_out_detail
group by company_name,station
order by c desc
;
select * from ads_station_out_equ_num_top;

-- 各线路进站闸机数统计排行榜
drop table if exists ads_line_in_equ_num_top;
create table ads_line_in_equ_num_top 
row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_line_in_equ_num_top'
as
select 
company_name,
count(distinct equ_no) c
from 
dwd_fact_szt_in_detail 
group by company_name
order by c desc
;

select * from ads_line_in_equ_num_top;

-- 各线路出站闸机数统计排行榜
drop table if exists ads_line_out_equ_num_top;
create table ads_line_out_equ_num_top 
row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_line_out_equ_num_top'
as
select 
company_name,
count(distinct equ_no) c
from 
dwd_fact_szt_out_detail 
group by company_name
order by c desc
;

SELECT * from ads_line_out_equ_num_top;

------------------------------------------------------------------------------------
-- 车站收入排行榜 ads_station_deal_top

drop table if exists ads_station_deal_top;
create external table ads_station_deal_top(
company_name string,
station string,
deal_money_sum double,
deal_value_sum double
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_station_deal_top';

insert overwrite table ads_station_deal_top partition (day="2018-09-01")
select 
company_name,
station,
sum(deal_money)/100 deal_money_sum,
sum(deal_value)/100 deal_value_sum
from dwd_fact_szt_out_detail
where day='2018-09-01'
group by company_name, station
order by deal_money_sum desc
;
select * from ads_station_deal_top;

-- 线路收入排行榜 ads_line_deal_top

drop table if exists ads_line_deal_top;
create external table ads_line_deal_top(
company_name string,
deal_money_sum double,
deal_value_sum double
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_line_deal_top';

insert overwrite table ads_line_deal_top partition (day="2018-09-01") 
select 
company_name,
sum(deal_money)/100 deal_money_sum,
sum(deal_value)/100 deal_value_sum
from dwd_fact_szt_out_detail
where day='2018-09-01'
group by company_name
order by deal_money_sum desc
;
select * from ads_line_deal_top;
------------------------------------------------------------------------------------
-- 各线路换乘出站乘客百分比排行榜

-- 各线路出站乘客统计
SELECT 
company_name,
count(*) c1
from dwd_fact_szt_out_detail
where day='2018-09-01'
GROUP BY company_name
;--t1

-- 各线路出站直达乘客统计
SELECT 
company_name,
count(*) c2
from dwd_fact_szt_out_detail
where day='2018-09-01' and conn_mark='1'
GROUP BY company_name
;--t2


-- 求比例，排序
drop table if exists ads_conn_ratio_day_top;
CREATE EXTERNAL TABLE ads_conn_ratio_day_top(
company_name STRING,
ratio DOUBLE
)
COMMENT '联程百分比'
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_conn_ratio_day_top';

insert overwrite table ads_conn_ratio_day_top partition (day="2018-09-01")
SELECT
t1.company_name,
t2.c2/t1.c1*100 rate
from 
(
    SELECT 
    company_name,
    count(*) c1
    from dwd_fact_szt_out_detail
    where day='2018-09-01'
    GROUP BY company_name
) 
t1 FULL join 
(
    SELECT 
    company_name,
    count(*) c2
    from dwd_fact_szt_out_detail
    where day='2018-09-01' and conn_mark='1'
    GROUP BY company_name
)
t2 
on t1.company_name=t2.company_name
ORDER BY rate desc
;

SELECT * from ads_conn_ratio_day_top;
------------------------------------------------------------------------------------

---各线路车费优惠人次百分比

-- 各线路出站乘客统计
SELECT 
company_name,
count(*) c1
from dwd_fact_szt_out_detail
where day='2018-09-01'
GROUP BY company_name
;--t1

-- 各线路直达出站并且享受优惠的人数
SELECT 
company_name,
count( if(deal_value - deal_money=0,null,1) ) sale_count
from dwd_fact_szt_out_detail
where day='2018-09-01' and conn_mark='0'
GROUP BY company_name
;--t2

-------------------------
drop table if exists ads_line_sale_ratio_top;
CREATE EXTERNAL TABLE ads_line_sale_ratio_top(
company_name STRING,
ratio DOUBLE
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_line_sale_ratio_top';

insert overwrite table ads_line_sale_ratio_top partition (day="2018-09-01")
SELECT t1.company_name,
t2.sale_count/t1.c1*100 ratio
from 
(
    SELECT 
    company_name,
    count(*) c1
    from dwd_fact_szt_out_detail
    where day='2018-09-01'
    GROUP BY company_name
)
t1,
(
    SELECT 
    company_name,
    count( if(deal_value - deal_money=0,null,1) ) sale_count
    from dwd_fact_szt_out_detail
    where day='2018-09-01' and conn_mark='0'
    GROUP BY company_name
)
t2
where t1.company_name=t2.company_name
order by ratio desc
;
SELECT * from ads_line_sale_ratio_top;

------------------------------------------------------------------------------------

-------------------------------------------------------------
-- 换乘时间最久的乘客排行榜

-- 拼接单程，起始时间
drop table if exists temp022;
create table temp022 COMMENT '临时中间表，拼接单程:前缀：联程标记+# 起始时间戳+行程'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp022'
as select
card_no,
ts,
deal_type,
company_name,
station,
conn_mark,
concat_ws('@',conn_mark, ts, deal_type, station) conn_ts_deal_type_station
from dws_in_out_sorted_card_date_wide where day='2018-09-01'
;

-- 寻找下一程
drop table if exists temp033;
create table temp033 COMMENT '临时中间表，开窗寻找下一程'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp033'
as select
card_no,
ts,
company_name,
conn_mark,
conn_ts_deal_type_station,
LEAD(conn_ts_deal_type_station,1) over(partition by card_no order by ts) as conn_ts_next_station
from temp022;

select substr('0@1535760350@地铁入站@益田',3,10);

-- 过滤合法行程，允许多程，添加联程字段
drop table if exists temp044;
create table temp044 COMMENT '临时中间表，过滤合法记录'
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp044'
as select
card_no,
company_name,
conn_mark,
conn_ts_deal_type_station,
conn_ts_next_station,
substr(conn_ts_next_station,3,10)-substr(conn_ts_deal_type_station,3,10) as time_s
from
temp033 where 
substr(conn_ts_deal_type_station ,14,4)='地铁入站'
and 
substr(conn_ts_next_station ,14,4)='地铁出站'
;

-- 创建行程表
create external table dws_travel_info(
card_no string,
company_name string,
conn_mark string,
conn_ts_deal_type_station string,
conn_ts_next_station string,
time_s double
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/dws/dws_travel_info';

insert overwrite table dws_travel_info partition (day="2018-09-01")
select
card_no,
company_name,
conn_mark,
conn_ts_deal_type_station,
conn_ts_next_station,
substr(conn_ts_next_station,3,10)-substr(conn_ts_deal_type_station,3,10) as time_s
from
(
    select
    card_no,
    ts,
    company_name,
    conn_mark,
    conn_ts_deal_type_station,
    LEAD(conn_ts_deal_type_station,1) over(partition by card_no order by ts) as conn_ts_next_station
    from 
    (
        select
        card_no,
        ts,
        deal_type,
        company_name,
        station,
        conn_mark,
        concat_ws('@',conn_mark, ts, deal_type, station) conn_ts_deal_type_station
        from dws_in_out_sorted_card_date_wide where day='2018-09-01'
    )
    temp022
)
temp033 where 
substr(conn_ts_deal_type_station ,14,4)='地铁入站'
and 
substr(conn_ts_next_station ,14,4)='地铁出站'
;

drop table if exists temp055;
create table temp055
row format delimited fields terminated by ',' location '/warehouse/szt.db/temp/temp055'
as select 
card_no,
company_name,
substr(conn_ts_deal_type_station,19) pre_station,
substr(conn_ts_next_station,19) last_station,
time_s
from temp044
where 
substr(conn_ts_next_station,1,1)='1' 
order by time_s desc
;


select 
card_no,
company_name,
pre_station,
last_station,
time_s
from temp055
where pre_station != last_station
order by time_s desc
;


------------------------合并
drop table if exists ads_conn_spend_time_top;
CREATE EXTERNAL TABLE ads_conn_spend_time_top(
card_no string,
company_name STRING,
pre_station string,
last_station string,
time_s double
)
partitioned by(day string) row format delimited fields terminated by ',' location '/warehouse/szt.db/ads/ads_conn_spend_time_top';

insert overwrite table ads_conn_spend_time_top partition (day="2018-09-01")
select 
card_no,
company_name,
pre_station,
last_station,
time_s
from 
(
    select 
    card_no,
    company_name,
    substr(conn_ts_deal_type_station,19) pre_station,
    substr(conn_ts_next_station,19) last_station,
    time_s
    from 
    dws_travel_info where day='2018-09-01'
    where 
    substr(conn_ts_next_station,1,1)='1' 
    order by time_s desc
)
temp055
where pre_station != last_station
order by time_s desc
;

select * from ads_conn_spend_time_top;
------------------------------------------------------------------------------------

--------------------------------------- 孖岭
drop table if exists ods_line_station;
create table ods_line_station
(
    line_no         string comment '线路编号',
    line_name       string comment '线路名称',
    station_no      string comment '车站编号',
    station_name    string comment '车站名称',
    line_station_id string comment '线站唯一名称'
) comment '线站详情表|孖岭|szmc.net-metro.csv' row format delimited fields terminated by ',' location '/warehouse/szt.db/ods/ods_line_station';
load data inpath '/warehouse/szt.db/ods/szmc.net-metro.csv' overwrite into table ods_line_station;

select * from ods_line_station;

----------------------------------------------------------------------------
-- 线路规划表，指定必要字段
drop table if exists ods_travel_plan;
create table ods_travel_plan
(
    start_station_id string comment '始发站 id|qidiancode',
    end_station_id   string comment '终到站 id|zhondiancode',
    switch_counts    string comment '换乘次数|times',
    tralev_all       string comment '所有行程'
) comment '线路规划、换乘方案，来自深圳地铁官网，45932 条|MiniTime.jsons -->45932 csv' 
    row format delimited fields terminated by '\t' location '/warehouse/szt.db/ods/ods_travel_plan';

load data inpath '/warehouse/szt.db/ods/travel_plan.csv' overwrite into table ods_travel_plan;
select * from ods_travel_plan;

select get_json_object(tralev_all, "$.[0,1,2].next_sid") 
from ods_travel_plan limit 1000,1050;

select start_station_id, 
       end_station_id, 
       switch_counts,
       get_json_object( tralev_all, "$.[0,1,2,3,4,5].travel_time") travel_time,
       get_json_object( tralev_all, "$.[0,1,2,3,4,5].switch_time") switch_time,
       get_json_object( tralev_all, "$.[0:5:1].next_sid")          next_sid,
       get_json_object( tralev_all, "$.[0].next_sid")              next_sid1,
       get_json_object( tralev_all, "$.[0::1].next_sid")           next_sid2
from ods_travel_plan limit 1000,1100;

------------------------------------------------------------------------------------------------------
-- 合并 ods_line_station + ods_travel_plan ---> dwd_travel_plan_detail
drop table if exists dwd_travel_plan_detail;

drop table if exists dwd_travel_plan_detail;
create table dwd_travel_plan_detail row format delimited fields terminated by '\t' location '/warehouse/szt.db/dwd/dwd_travel_plan_detail' as
select start_station_id,
       line1.station_name as start_station_name,
       end_station_id,
       line2.station_name as end_station_name,
       switch_counts,
       tralev_all
from ods_line_station as line1,
     ods_travel_plan as plan,
     ods_line_station as line2
where line1.line_station_id =
      plan.start_station_id
  and plan.end_station_id =
      line2.line_station_id;
-- 所有换乘方案
select * from dwd_travel_plan_detail;
