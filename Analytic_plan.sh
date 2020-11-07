python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 1 --n_clients 1 > Analytic/th_n1.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 2 --n_clients 2 > Analytic/th_n2.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 4 --n_clients 4 > Analytic/th_n4.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 8 --n_clients 8 > Analytic/th_n8.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 16 --n_clients 16 > Analytic/th_n16.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 32 --n_clients 32 > Analytic/th_n32.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 64 --n_clients 64 > Analytic/th_n64.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 1 --a_clients 1 > Analytic/th_a1.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 2 --a_clients 2 > Analytic/th_a2.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 4 --a_clients 4 > Analytic/th_a4.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 8 --a_clients 8 > Analytic/th_a8.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 10 --a_clients 10 > Analytic/th_a10.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 2 --a_clients 1 --n_clients 1 > Analytic/th_a1_n1.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 3 --a_clients 1 --n_clients 2 > Analytic/th_a1_n2.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 5 --a_clients 1 --n_clients 4 > Analytic/th_a1_n4.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 9 --a_clients 1 --n_clients 8 > Analytic/th_a1_n8.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 17 --a_clients 1 --n_clients 16 > Analytic/th_a1_n16.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 33 --a_clients 1 --n_clients 32 > Analytic/th_a1_n32.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 65 --a_clients 1 --n_clients 64 > Analytic/th_a1_n64.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 3 --a_clients 2 --n_clients 1 > Analytic/th_a2_n1.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 4 --a_clients 2 --n_clients 2 > Analytic/th_a2_n2.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 6 --a_clients 2 --n_clients 4 > Analytic/th_a2_n4.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 10 --a_clients 2 --n_clients 8 > Analytic/th_a2_n8.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 18 --a_clients 2 --n_clients 16 > Analytic/th_a2_n16.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 34 --a_clients 2 --n_clients 32 > Analytic/th_a2_n32.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 66 --a_clients 2 --n_clients 64 > Analytic/th_a2_n64.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 5 --a_clients 4 --n_clients 1 > Analytic/th_a4_n1.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 6 --a_clients 4 --n_clients 2 > Analytic/th_a4_n2.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 8 --a_clients 4 --n_clients 4 > Analytic/th_a4_n4.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 12 --a_clients 4 --n_clients 8 > Analytic/th_a4_n8.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 20 --a_clients 4 --n_clients 16 > Analytic/th_a4_n16.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 36 --a_clients 4 --n_clients 32 > Analytic/th_a4_n32.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 68 --a_clients 4 --n_clients 64 > Analytic/th_a4_n64.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 9 --a_clients 8 --n_clients 1 > Analytic/th_a8_n1.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 10 --a_clients 8 --n_clients 2 > Analytic/th_a8_n2.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 12 --a_clients 8 --n_clients 4 > Analytic/th_a8_n4.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 16 --a_clients 8 --n_clients 8 > Analytic/th_a8_n8.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 24 --a_clients 8 --n_clients 16 > Analytic/th_a8_n16.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 40 --a_clients 8 --n_clients 32 > Analytic/th_a8_n32.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 72 --a_clients 8 --n_clients 64 > Analytic/th_a8_n64.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 11 --a_clients 10 --n_clients 1 > Analytic/th_a10_n1.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 12 --a_clients 10 --n_clients 2 > Analytic/th_a10_n2.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 14 --a_clients 10 --n_clients 4 > Analytic/th_a10_n4.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 18 --a_clients 10 --n_clients 8 > Analytic/th_a10_n8.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 26 --a_clients 10 --n_clients 16 > Analytic/th_a10_n16.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 42 --a_clients 10 --n_clients 32 > Analytic/th_a10_n32.txt
python reset.py
sleep 300

python ./tpcc.py --no-load --config=couchbase.config couchbase --clients 74 --a_clients 10 --n_clients 64 > Analytic/th_a10_n64.txt
python reset.py
sleep 300

