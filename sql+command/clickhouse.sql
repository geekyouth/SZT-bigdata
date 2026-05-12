CREATE DATABASE IF NOT EXISTS szt;

CREATE TABLE IF NOT EXISTS szt.szt_data
(
    deal_date   String,
    close_date  String,
    card_no     String,
    deal_value  String,
    deal_type   String,
    company_name String,
    car_no      String,
    station     String,
    conn_mark   String,
    deal_money  String,
    equ_no      String
) ENGINE = MergeTree()
ORDER BY (deal_date, card_no);

SELECT * FROM szt.szt_data LIMIT 100;
