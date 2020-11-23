CREATE DATABASE IF NOT EXISTS szt;
drop table if exists szt.szt_data;
show tables ;
CREATE TABLE szt.szt_data (string_value String) ENGINE = Log();

SELECT * FROM system.clusters;
SELECT * FROM szt.szt_data;
