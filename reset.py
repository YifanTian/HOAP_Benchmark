import requests
import json
import logging

print(requests.__version__)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    #
                    filename='results.log')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter(
    '%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s'))
logging.getLogger('').addHandler(console)



def N1QL_query(query):
    url = "http://128.195.52.212:8093/query/service"
    payload = query
    data = [
        ('statement', payload),
    ]
    response = requests.post(url, data=data, headers={'Connection':'close'}, auth=('Administrator', 'xxxx'))
    return json.loads(response.text)


if __name__ == '__main__':
    q1 = 'DELETE FROM TPCC p WHERE p.Category = "NEW_ORDER" AND NO_O_ID > 3000 AND NO_D_ID >= 0 AND NO_W_ID >= 0;'
    q2 = 'DELETE FROM TPCC p WHERE p.Category = "ORDERS" AND O_ID > 3000 AND O_D_ID >= 0 AND O_W_ID >= 0;'
    q3 = 'UPDATE TPCC p SET p.D_NEXT_O_ID = 3001 WHERE p.Category = "DISTRICT" AND D_ID >= 0 AND D_W_ID >= 0;'
    # q4 = 'UPDATE TPCC p SET p.S_ORDER_CNT = 0, p.S_REMOTE_CNT = 0, p.S_YTD = 0 WHERE p.Category = "STOCK" AND S_I_ID >= 0 AND S_W_ID >= 0;'
    # N1QL_query(q1)
    # N1QL_query(q2)
    # N1QL_query(q3)
    # N1QL_query(q4)
    for i in range(801,1201):
        query = 'SELECT * FROM TPCC p WHERE p.Category = "WAREHOUSE" AND W_ID = {};'.format(i)
        res = N1QL_query(query)
        # print(res)
        # if res['results'] == 0:
            # print('')
        try:
            print(res['results'][0]['p']['W_ID'])
            # if res['status'] != 'success':
                # print(i,'not success')
            # if res['status'] == 'success':
                # print(i,'success')
        except Exception as e:
            print(e)
