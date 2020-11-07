1. Run the program to get the data
python ./tpcc.py --no-execute couchbase

2. Setup the server. Run ansible
ansible-playbook -i example_hosts -u yifant -K  cluster_AQA_setup.yml
ansible-playbook -i example_hosts -u yifant -K  cluster_AQQ_setup.yml

3. Upload the data to server

4. Run the ansible to setup the index
ansible-playbook -i example_hosts -u yifant -K  index_setup.yml
ansible-playbook -i example_hosts -u yifant -K  analytic_setup.yml

5. Run the worload program
python ./tpcc.py --no-load couchbase --clients 4 --n_clients 4
