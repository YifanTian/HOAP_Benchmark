import numpy as np
import pandas as pd
from tabulate import tabulate


def process(output_dir, n_num, a_num, service):
    if n_num == 0:
        file_name = '{}/th_a{}.txt'.format(output_dir,a_num)
    elif a_num == 0:
        file_name = '{}/th_n{}.txt'.format(output_dir,n_num)
    else:
        file_name = '{}/th_a{}_n{}.txt'.format(output_dir,a_num,n_num)

    if service == 'n1ql':
        line_name = 'NEW_ORDER [' 
    else:
        line_name = 'ANALYTIC ['
    with open(file_name) as f:
        lines = f.readlines()
        if len(lines) == 0:
            return 0.0
        clients_results = []
        count_list = []
        error_count = 0
        success_count = 0
        for line in lines:
            if 'errors' in line:
                error_count += 1
            if 'success' in line:
                success_count += 1
            if line.startswith(line_name):
                time_line = line.strip().split()[-1][5:-1]
                count = line.strip().split()[1]
                clients_results.append(int(time_line))
                count_list.append(int(line.strip().split()[1][5:-1]))
        total_time_list = [clients_results[0]]+[clients_results[i+1]-clients_results[i]  for i in range(len(clients_results)-1)]
        count_list = [count_list[0]]+[count_list[i+1]-count_list[i]  for i in range(len(count_list)-1)]
 
        throughput = sum(count_list)/max(total_time_list)
    return throughput

  
output_dir = './N1QL'

results = []
for i in range(-1,7):
    if i == -1:
        temp = ['N0']
        n_num = 0
    else:
        temp = ['N{}'.format(2**i)]
        n_num = 2**i
    for a_num in [0,1,2,4,8,10]:
        if n_num == 0:
            temp.append(0.0)
            continue
        if a_num == 0 and n_num == 0:
            throughput = 0.0
        else:
            try:
                throughput = process(output_dir, n_num, a_num, 'n1ql')
            except:
                throughput = 0.0
        temp.append(throughput)
    results.append(temp)

print(tabulate(results, headers=['ALL', 'A0', 'A1','A2','A4','A8','A10'], tablefmt='orgtbl'))
print()

df2 = pd.DataFrame(results, columns=['ALL', 'A0', 'A1','A2','A4','A8','A10'])
df2.to_csv('./AQQ_8nodes_round_n1ql_throughput.csv')

output_dir = './Analytic'

results = []
for i in range(-1,7):
    if i == -1:
        temp = ['N0']
        n_num = 0
    else:
        temp = ['N{}'.format(2**i)]
        n_num = 2**i
    for a_num in [0,1,2,4,8,10]:
        if a_num == 0:
            temp.append(0.0)
            continue
        if a_num == 0 and n_num == 0:
            throughput = 0.0
        else:
            try:
                throughput = process(output_dir, n_num, a_num, 'analytic')*60
            except:
                throughput = 0.0
        temp.append(throughput)
    results.append(temp)
        
print(tabulate(results, headers=['ALL', 'A0', 'A1', 'A2', 'A4', 'A8', 'A10'], tablefmt='orgtbl'))

df2 = pd.DataFrame(results, columns=['ALL', 'A0', 'A1', 'A2', 'A4', 'A8', 'A10'])
df2.to_csv('./AQQ_8nodes_round_analytic_throughput.csv')
