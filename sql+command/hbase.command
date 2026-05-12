# HBase Shell Commands for SZT-bigdata v2.0
# Compatible with HBase 2.6+

# Create namespace
create_namespace 'szt'

# Create table with column family and versions
create 'szt:data', {NAME => 'card', VERSIONS => 10}

# Scan table
scan 'szt:data', {FORMATTER => 'toString', VERSIONS => 10}

# Get by row key
get 'szt:data', 'JHAGBEIFF', {FORMATTER => 'toString', VERSIONS => 10}

# Describe table
describe 'szt:data'

# Count rows
count 'szt:data'

# Disable and drop table
# disable 'szt:data'
# drop 'szt:data'
