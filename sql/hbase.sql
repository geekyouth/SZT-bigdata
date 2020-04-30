-- 全表扫描，并且格式化字符串
scan 'szt:data', {FORMATTER => 'toString'}

-- 查看历史版本
scan 'szt:data', {FORMATTER => 'toString',VERSIONS=>10}

-- 查看指定的 rowkey 
get 'szt:data','10drac',{COLUMN=>'card:station',VERSIONS=>10, FORMATTER => 'toString'}
