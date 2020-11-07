/opt/couchbase/bin/cbimport json -c 127.0.0.1:8091 -u Administrator -p xxxx -b TPCC -d file://./dataset/items.json -f lines -g key::#UUID# -t 4
/opt/couchbase/bin/cbimport json -c 127.0.0.1:8091 -u Administrator -p xxxx -b TPCC -d file://./dataset/warehouse_1.json -f lines -g key::#UUID# -t 4
