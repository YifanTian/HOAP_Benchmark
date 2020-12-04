# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from __future__ import with_statement

import sys
import json
import logging
import urllib
from pprint import pformat
from time import sleep
import requests
import random

from util import rand
import uuid 
import constants
from drivers.abstractdriver import AbstractDriver

TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "I_ID", # INTEGER
        "I_IM_ID", # INTEGER
        "I_NAME", # VARCHAR
        "I_PRICE", # FLOAT
        "I_DATA", # VARCHAR
        "I_W_ID", # INTEGER
    ],
    constants.TABLENAME_WAREHOUSE: [
        "W_ID", # SMALLINT
        "W_NAME", # VARCHAR
        "W_STREET_1", # VARCHAR
        "W_STREET_2", # VARCHAR
        "W_CITY", # VARCHAR
        "W_STATE", # VARCHAR
        "W_ZIP", # VARCHAR
        "W_TAX", # FLOAT
        "W_YTD", # FLOAT
    ],
    constants.TABLENAME_DISTRICT: [
        "D_ID", # TINYINT
        "D_W_ID", # SMALLINT
        "D_NAME", # VARCHAR
        "D_STREET_1", # VARCHAR
        "D_STREET_2", # VARCHAR
        "D_CITY", # VARCHAR
        "D_STATE", # VARCHAR
        "D_ZIP", # VARCHAR
        "D_TAX", # FLOAT
        "D_YTD", # FLOAT
        "D_NEXT_O_ID", # INT
    ],
    constants.TABLENAME_CUSTOMER:   [
        "C_ID", # INTEGER
        "C_D_ID", # TINYINT
        "C_W_ID", # SMALLINT
        "C_FIRST", # VARCHAR
        "C_MIDDLE", # VARCHAR
        "C_LAST", # VARCHAR
        "C_STREET_1", # VARCHAR
        "C_STREET_2", # VARCHAR
        "C_CITY", # VARCHAR
        "C_STATE", # VARCHAR
        "C_ZIP", # VARCHAR
        "C_PHONE", # VARCHAR
        "C_SINCE", # TIMESTAMP
        "C_CREDIT", # VARCHAR
        "C_CREDIT_LIM", # FLOAT
        "C_DISCOUNT", # FLOAT
        "C_BALANCE", # FLOAT
        "C_YTD_PAYMENT", # FLOAT
        "C_PAYMENT_CNT", # INTEGER
        "C_DELIVERY_CNT", # INTEGER
        "C_DATA", # VARCHAR
    ],
    constants.TABLENAME_STOCK:      [
        "S_I_ID", # INTEGER
        "S_W_ID", # SMALLINT
        "S_QUANTITY", # INTEGER
        "S_DIST_01", # VARCHAR
        "S_DIST_02", # VARCHAR
        "S_DIST_03", # VARCHAR
        "S_DIST_04", # VARCHAR
        "S_DIST_05", # VARCHAR
        "S_DIST_06", # VARCHAR
        "S_DIST_07", # VARCHAR
        "S_DIST_08", # VARCHAR
        "S_DIST_09", # VARCHAR
        "S_DIST_10", # VARCHAR
        "S_YTD", # INTEGER
        "S_ORDER_CNT", # INTEGER
        "S_REMOTE_CNT", # INTEGER
        "S_DATA", # VARCHAR
    ],
    constants.TABLENAME_ORDERS:     [
        "O_ID", # INTEGER
        "O_C_ID", # INTEGER
        "O_D_ID", # TINYINT
        "O_W_ID", # SMALLINT
        "O_ENTRY_D", # TIMESTAMP
        "O_CARRIER_ID", # INTEGER
        "O_OL_CNT", # INTEGER
        "O_ALL_LOCAL", # INTEGER
    ],
    constants.TABLENAME_NEW_ORDER:  [
        "NO_O_ID", # INTEGER
        "NO_D_ID", # TINYINT
        "NO_W_ID", # SMALLINT
    ],
    constants.TABLENAME_ORDER_LINE: [
        "OL_O_ID", # INTEGER
        "OL_D_ID", # TINYINT
        "OL_W_ID", # SMALLINT
        "OL_NUMBER", # INTEGER
        "OL_I_ID", # INTEGER
        "OL_SUPPLY_W_ID", # SMALLINT
        "OL_DELIVERY_D", # TIMESTAMP
        "OL_QUANTITY", # INTEGER
        "OL_AMOUNT", # FLOAT
        "OL_DIST_INFO", # VARCHAR
    ],
    constants.TABLENAME_HISTORY:    [
        "H_C_ID", # INTEGER
        "H_C_D_ID", # TINYINT
        "H_C_W_ID", # SMALLINT
        "H_D_ID", # TINYINT
        "H_W_ID", # SMALLINT
        "H_DATE", # TIMESTAMP
        "H_AMOUNT", # FLOAT
        "H_DATA", # VARCHAR
    ],
}

INDEX_QUERIES = {
    'ITEM' : 'CREATE INDEX idx_ITEM ON `TPCC`(I_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "ITEM";',
    'WAREHOUSE' : 'CREATE INDEX idx_WAREHOUSE ON `TPCC`(W_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "WAREHOUSE";',
    'DISTRICT' : 'CREATE INDEX idx_DISTRICT ON `TPCC`(D_W_ID, D_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "DISTRICT";',
    'CUSTOMER' : 'CREATE INDEX idx_CUSTOMER ON `TPCC`(C_W_ID, C_D_ID, C_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "CUSTOMER";',
    'STOCK' : 'CREATE INDEX idx_STOCK ON `TPCC`(S_W_ID, S_I_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "STOCK";',
    'NEW_ORDER' : 'CREATE INDEX idx_NEW_ORDER ON `TPCC`(NO_W_ID, NO_D_ID, NO_O_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "NEW_ORDER";',
    'ORDERS' : 'CREATE INDEX idx_ORDER ON `TPCC`(O_W_ID, O_C_ID, O_D_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "ORDERS";',
    'ORDERS_primary' : 'CREATE INDEX idx_Primary_ORDER ON `TPCC`(O_ID, O_W_ID, O_D_ID)  PARTITION BY HASH(META().id) WHERE `Category` = "ORDERS";',
    'C_STATE_idx' : 'CREATE INDEX `C_STATE_idx` ON `TPCC`(`C_STATE`) PARTITION BY HASH(META().id) WHERE (`Category` = "CUSTOMER");'
}

TXN_QUERIES = {
    "NEW_ORDER": {
        "District_find_one" : 'SELECT * FROM TPCC p WHERE p.Category = "DISTRICT" AND D_ID = {D_ID} AND D_W_ID = {D_W_ID};',
        "District_find_one_and_update" : 'UPDATE TPCC p SET p.D_NEXT_O_ID = {D_NEXT_O_ID} WHERE p.Category = "DISTRICT" AND D_ID = {D_ID} AND D_W_ID = {D_W_ID};',
        "Item_find" : 'SELECT * FROM TPCC p WHERE p.Category = "ITEM" AND I_ID IN {I_ID} AND I_W_ID = {I_W_ID};',
        "Warehouse_find_one" : 'SELECT * FROM TPCC p WHERE p.Category = "WAREHOUSE" AND W_ID = {W_ID}',
        "Customer_find_one" : 'SELECT * FROM TPCC p WHERE p.Category = "CUSTOMER" AND C_W_ID = {C_W_ID} AND C_D_ID = {C_D_ID} AND C_ID = {C_ID}',
        "createNewOrder": 'UPSERT INTO `TPCC` (KEY,VALUE) VALUES("{key}", {content}) RETURNING *;',
        "Stock_find" : 'SELECT * FROM TPCC p WHERE p.Category = "STOCK" AND p.S_I_ID in {S_I_ID} AND p.S_W_ID = {S_W_ID};',
        "Stock_find_one" : 'SELECT * FROM TPCC p WHERE p.Category = "STOCK" AND p.S_I_ID = {S_I_ID} AND p.S_W_ID = {S_W_ID};',
        "UpdateStock": 'UPDATE TPCC p SET S_QUANTITY = {S_QUANTITY}, S_YTD = {S_YTD}, S_ORDER_CNT = {S_ORDER_CNT}, S_REMOTE_CNT = {S_REMOTE_CNT} WHERE S_I_ID = {S_I_ID} AND S_W_ID = {S_W_ID} AND Category = "STOCK";', # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "Orders_insert_one" : 'UPSERT INTO `TPCC` (KEY,VALUE) VALUES("{key}", {content}) RETURNING *;',        
    }
}

n1ql_url_dict = {
    0:'http://128.195.52.212:8093/query/service',
    1:'http://titanium.ics.uci.edu:8093/query/service',
    2:'http://vanadium.ics.uci.edu:8093/query/service',
    3:'http://lawrencium.ics.uci.edu:8093/query/service',
    4:'http://dubnium.ics.uci.edu:8093/query/service',
    5:'http://fermium.ics.uci.edu:8093/query/service',
    6:'http://rutherfordium.ics.uci.edu:8093/query/service',
    7:'http://seaborgium.ics.uci.edu:8093/query/service',
}

analytic_url_dict = {
    0:'http://dubnium.ics.uci.edu:8093/query/service',
    1:'http://fermium.ics.uci.edu:8093/query/service',
    2:'http://rutherfordium.ics.uci.edu:8093/query/service',
    3:'http://seaborgium.ics.uci.edu:8093/query/service',
}

def Analytic_query(query, thread_id=None):
    if thread_id == None:
        url_idx = random.choice([_ for _ in range(len(analytic_url_dict))])
        url = analytic_url_dict[url_idx]
    else:
        url = analytic_url_dict[int(thread_id.split('_')[-1])%len(analytic_url_dict)]
        print('url index: ',int(thread_id.split('_')[-1])%len(analytic_url_dict),' thread_id: ',thread_id,' url: ',url)

    headers = {
        'content-type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
    }
    payload = query
    data = [
        ('statement', payload),
    ]
    response = requests.request("POST", url, data=data, headers=headers,auth=('Administrator', 'xxxx'))    
    return json.loads(response.text)

def N1QL_query(query, thread_id=None):
    if thread_id == None:
        url_idx = random.choice([_ for _ in range(len(n1ql_url_dict))])
        url = n1ql_url_dict[url_idx]
    else:
        url = n1ql_url_dict[int(thread_id.split('_')[-1])%len(n1ql_url_dict)]
        print('url index: ',int(thread_id.split('_')[-1])%len(n1ql_url_dict),' thread_id: ',thread_id,' url: ',url)

    payload = query
    data = [
        ('statement', payload),
	    ('timeout', '1800s')
    ]
    response = requests.post(url, data=data, headers={'Connection':'close'}, auth=('Administrator', 'xxxx'))
    return json.loads(response.text)

class db_table:
    def __init__(self, name):
        self.name = name

    def find_one(self, params):
        query = TXN_QUERIES[self.name].format(params)
        self.N1QL_query(query)

    def create_index(self, table, unique):
        N1QL_query(INDEX_QUERIES[self.name])

    def execute(self, table_name, query_name, d):
        query = TXN_QUERIES[table_name][query_name].format(**d)
        res = N1QL_query(query)
        return res

def get_database(table_name):
    return db_table(table_name)

## ==============================================
## CouchbaseDriver
## ==============================================
class CouchbaseDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "uri":              ("The mongodb connection string or URI", "mongodb://localhost:27017"),
        "name":             ("Database name", "TPCC"),
        "denormalize":      ("If true, data will be denormalized using MongoDB schema design best practices", True),
        "notransactions":   ("If true, transactions will not be used (benchmarking only)", False),
        "findandmodify":    ("If true, all things to update will be fetched via findAndModify", True),
        "secondary_reads":  ("If true, we will allow secondary reads", True),
        "retry_writes":     ("If true, we will enable retryable writes", True),
        "causal_consistency":  ("If true, we will perform causal reads ", True),
        "shards":          ("If >1 then sharded", "1")
    }
    DENORMALIZED_TABLES = [
        constants.TABLENAME_ORDERS,
        constants.TABLENAME_ORDER_LINE
    ]

    def __init__(self, ddl):
        super(CouchbaseDriver, self).__init__("mongodb", ddl)
        self.no_transactions = False
        self.find_and_modify = True
        self.read_preference = "primary"
        self.database = None
        self.client = None
        self.executed = False
        self.w_orders = {}
        # things that are not better can't be set in config
        self.batch_writes = False
        self.agg = False
        self.all_in_one_txn = True
        # initialize
        self.causal_consistency = False
        self.secondary_reads = False
        self.retry_writes = True
        self.read_concern = "majority"
        self.denormalize = True
        self.output = open('results.json','a')
        self.result_doc = {}
        self.warehouses = 0
        self.shards = 1

        ## Create member mapping to collections
        for name in constants.ALL_TABLES:
            self.__dict__[name.lower()] = None

    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return CouchbaseDriver.DEFAULT_CONFIG

    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        default_uri = 'uri' not in config
        for key in CouchbaseDriver.DEFAULT_CONFIG:
            # rather than forcing every value which has a default to be specified
            # we should pluck out the keys from default that are missing in config
            # and set them there to their default values
            if not key in config:
                logging.debug("'%s' not in %s conf, set to %s",
                              key, self.name, str(CouchbaseDriver.DEFAULT_CONFIG[key][1]))
                config[key] = str(CouchbaseDriver.DEFAULT_CONFIG[key][1])

        logging.debug("Default plus our config %s", pformat(config))
        self.denormalize = config['denormalize'] == 'True'
        self.no_transactions = config['notransactions'] == 'True'
        self.shards = int(config['shards'])
        self.warehouses = config['warehouses']
        print(config['findandmodify'])
        self.find_and_modify = config['findandmodify'] == 'True'
        self.causal_consistency = config['causal_consistency'] == 'True'
        self.retry_writes = config['retry_writes'] == 'True'
        self.secondary_reads = config['secondary_reads'] == 'True'
        if self.secondary_reads:
            self.read_preference = "nearest"

        # handle building connection string
        userpassword = ""
        usersecret = ""
        uri = config['uri']
        # only use host/port if they didn't provide URI
        if default_uri and 'host' in config:
            host = config['host']
            if 'port' in config:
                host = host+':'+config['port']
            uri = "mongodb://" + host
        if 'user' in config:
            user = config['user']
            if not 'passwd' in config:
                logging.error("must specify password if user is specified")
                sys.exit(1)
            userpassword = urllib.quote_plus(user)+':'+urllib.quote_plus(config['passwd'])+"@"
            usersecret = urllib.quote_plus(user)+':'+ '*'*len(config['passwd']) + "@"

        pindex = 10  # "mongodb://"
        if uri[0:14] == "mongodb+srv://":
            pindex = 14
        real_uri = uri[0:pindex]+userpassword+uri[pindex:]
        display_uri = uri[0:pindex]+usersecret+uri[pindex:]
        self.database = dict()

        if self.denormalize:
            logging.debug("Using denormalized data model")

        try:
            if config["reset"]:
                logging.info("Deleting database '%s'", self.database.name)
                for name in constants.ALL_TABLES:
                    self.database[name].drop()
                    logging.debug("Dropped collection %s", name)
                ## FOR
            ## IF

            ## whether should check for indexes
            load_indexes = ('execute' in config and not config['execute']) and \
                           ('load' in config and not config['load'])
            for name in constants.ALL_TABLES:
                if self.denormalize and name == "ORDER_LINE":
                    continue

                self.database[name]  = get_database(name) 
                self.__dict__[name.lower()] = self.database[name]

                if load_indexes and name in INDEX_QUERIES:
                    uniq = True
                    for index in INDEX_QUERIES[name]:
                        uniq = False
                ## IF
            ## FOR
        except Exception as e:
            print(e)
        

    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples, f):        
        if not tuples:
            return
        logging.debug("Loading %d tuples for tableName %s", len(tuples), tableName)

        assert tableName in TABLE_COLUMNS, "Table %s not found in TABLE_COLUMNS" % tableName
        columns = TABLE_COLUMNS[tableName]
        num_columns = range(len(columns))

        tuple_dicts = []

        ## We want to combine all of a CUSTOMER's ORDERS, and ORDER_LINE records
        ## into a single document
        if self.denormalize and tableName in CouchbaseDriver.DENORMALIZED_TABLES:
            ## If this is the ORDERS table, then we'll just store the record locally for now
            if tableName == constants.TABLENAME_ORDERS:
                for t in tuples:
                    key = tuple(t[:1]+t[2:4]) # O_ID, O_C_ID, O_D_ID, O_W_ID
                    self.w_orders[key] = dict([(columns[i], t[i]) for i in num_columns])
                ## FOR
            ## IF

            ## If this is an ORDER_LINE record, then we need to stick it inside of the
            ## right ORDERS record
            elif tableName == constants.TABLENAME_ORDER_LINE:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    assert o_key in self.w_orders, "Order Key: %s\nAll Keys:\n%s" % (str(o_key), "\n".join(map(str, sorted(self.w_orders.keys()))))
                    o = self.w_orders[o_key]
                    if not tableName in o:
                        o[tableName] = []
                    o[tableName].append(dict([(columns[i], t[i]) for i in num_columns[4:]]))
                ## FOR

            ## Otherwise nothing
            else: assert False, "Only Orders and order lines are denormalized! Got %s." % tableName
        ## Otherwise just shove the tuples straight to the target collection
        else:
            if tableName == constants.TABLENAME_ITEM:
                tuples3 = []
                if self.shards > 1:
                    ww = range(1,self.warehouses+1)
                else:
                    ww = [0]
                for t in tuples:
                    for w in ww:
                       t2 = list(t)
                       t2.append(w)
                       tuples3.append(t2)
                tuples = tuples3
            for t in tuples:
                tuple_dicts.append(dict([(columns[i], t[i]) for i in num_columns]))
                
                doc_dict = dict(map(lambda i: (columns[i], t[i]), num_columns))
                doc_dict['Category'] = tableName
                f.write(json.dumps(doc_dict)+'\n')

        return

    def loadFinishDistrict(self, w_id, d_id, f):
        print('inside loadFinishDistrict')
        if self.denormalize:
            logging.debug("Pushing %d denormalized ORDERS records for WAREHOUSE %d DISTRICT %d into TPCC", len(self.w_orders), w_id, d_id)

            for val in self.w_orders.values():
                val['Category'] = 'ORDERS'
                f.write(json.dumps(val)+'\n')

            self.w_orders.clear()
        ## IF

    def executeStart(self):
        """Optional callback before the execution for each client starts"""
        return None

    def executeFinish(self):
        """Callback after the execution for each client finishes"""
        return None


    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        s_dist_col = "S_DIST_%02d" % d_id

        comment = "NEW_ORDER"

        assert i_ids, "No matching i_ids found for new order"
        assert len(i_ids) == len(i_w_ids), "different number of i_ids and i_w_ids"
        assert len(i_ids) == len(i_qtys), "different number of i_ids and i_qtys"

        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------

        # getDistrict
        district_project = {"_id":0, "D_ID":1, "D_W_ID":1, "D_TAX": 1, "D_NEXT_O_ID": 1}

        if self.find_and_modify:
            d = self.district.execute("NEW_ORDER", "District_find_one", {"D_ID": d_id, "D_W_ID": w_id})
            if len(d['results']) > 0:
                d = d['results'][0]['p']
                d_update = self.district.execute("NEW_ORDER", "District_find_one_and_update", {"D_ID": d_id, "D_W_ID": w_id, "D_NEXT_O_ID": d['D_NEXT_O_ID']+1})
            
            assert d, "Couldn't find district in new order w_id %d d_id %d" % (w_id, d_id)
        else:
            d = self.district.find_one({"D_ID": d_id, "D_W_ID": w_id, "$comment": comment},
                                       district_project, session=s)
            assert d, "Couldn't find district in new order w_id %d d_id %d" % (w_id, d_id)
            # incrementNextOrderId
            d["$comment"] = comment
            self.district.update_one(d, {"$inc": {"D_NEXT_O_ID": 1}}, session=s)

        ## IF
        d_tax = d["D_TAX"]
        d_next_o_id = d["D_NEXT_O_ID"]

        # fetch matching items and see if they are all valid
        if self.shards > 1: i_w_id = w_id
        else: i_w_id = 0

        items = self.item.execute("NEW_ORDER", "Item_find", {"I_ID": i_ids, "I_W_ID": i_w_id})
        items = list(res['p'] for res in items['results'])

        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        if len(items) != len(i_ids):
            logging.debug("1% Abort transaction: " +  constants.INVALID_ITEM_MESSAGE)
            return None
        ## IF
        items = sorted(items, key=lambda x: i_ids.index(x['I_ID']))

        # getWarehouseTaxRate
        w = self.warehouse.execute("NEW_ORDER", "Warehouse_find_one",{"W_ID": w_id})
        w = w['results'][0]['p']
        
        assert w, "Couldn't find warehouse in new order w_id %d" % (w_id)
        w_tax = w["W_TAX"]

        # getCustomer
        c = self.customer.execute("NEW_ORDER", "Customer_find_one",{"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id})
        c = c['results'][0]['p']   

        assert c, "Couldn't find customer in new order"
        c_discount = c["C_DISCOUNT"]
        
        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = 0

        # createNewOrder
        self.new_order.execute("NEW_ORDER", "createNewOrder", {"key": uuid.uuid1(), "content": {"Category" : "NEW_ORDER", "NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id}} )

        all_local = 1 if ([w_id] * len(i_w_ids)) == i_w_ids else 0
        o = {"O_ID": d_next_o_id, "O_ENTRY_D": o_entry_d,
             "O_CARRIER_ID": o_carrier_id, "O_OL_CNT": ol_cnt, "O_ALL_LOCAL": all_local}

        if self.denormalize:
            o[constants.TABLENAME_ORDER_LINE] = []

        o["O_D_ID"] = d_id
        o["O_W_ID"] = w_id
        o["O_C_ID"] = c_id

        ## ----------------
        ## OPTIMIZATION:
        ## If all of the items are at the same warehouse, then we'll issue a single
        ## request to get their information, otherwise we'll still issue a single request
        ## ----------------
        item_w_list = zip(i_ids, i_w_ids)
        stock_project = {"_id":0, "S_I_ID": 1, "S_W_ID": 1,
                         "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1,
                         "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1}

        if all_local:
            all_stocks = self.stock.execute("NEW_ORDER", "Stock_find", {"S_I_ID": i_ids, "S_W_ID": w_id})
            all_stocks = list(res['p'] for res in all_stocks['results'])

        else:
            field_list = ["S_I_ID", "S_W_ID"]
            search_list = [dict(zip(field_list, ze)) for ze in item_w_list]

            all_stocks = []
            for search in search_list:
                stock = self.stock.execute("NEW_ORDER", "Stock_find_one", {"S_I_ID": search['S_I_ID'], "S_W_ID": search['S_W_ID']})
                stock = stock['results'][0]['p'] 
                all_stocks.append(stock)

        ## IF
        assert len(all_stocks) == ol_cnt, "all_stocks len %d != ol_cnt %d" % (len(all_stocks), ol_cnt)
        all_stocks = sorted(all_stocks, key=lambda x: item_w_list.index((x['S_I_ID'], x["S_W_ID"])))

        ## ----------------
        ## Insert Order Line, Stock Item Information
        ## ----------------
        item_data = []
        total = 0
        # we already fetched all items so we should never need to go to self.item again
        # iterate over every line item
        # if self.batch_writes is set then write once per collection
        if self.batch_writes:
            stock_writes = []
            order_line_writes = []
        ## IF

        for i in range(ol_cnt):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            item_info = items[i]
            i_name = item_info["I_NAME"]
            i_data = item_info["I_DATA"]
            i_price = item_info["I_PRICE"]

            si = all_stocks[i]

            assert si, "stock item not found"

            s_quantity = si["S_QUANTITY"]
            s_ytd = si["S_YTD"]
            s_order_cnt = si["S_ORDER_CNT"]
            s_remote_cnt = si["S_REMOTE_CNT"]
            s_data = si["S_DATA"]
            s_dist_xx = si[s_dist_col] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            ## IF

            s_order_cnt += 1
            if ol_supply_w_id != w_id:
                s_remote_cnt += 1

            if s_ytd > 1000:
                s_ytd = 0
            if s_order_cnt > 100:
                s_order_cnt = 0
            if s_remote_cnt > 100:
                s_remote_cnt = 0
            
            # updateStock
            stock_write_update = {"$set": {"S_QUANTITY": s_quantity,
                                           "S_YTD": s_ytd,
                                           "S_ORDER_CNT": s_order_cnt,
                                           "S_REMOTE_CNT": s_remote_cnt}}
            if self.batch_writes:
                si["$comment"] = comment
                stock_writes.append({"S_QUANTITY": s_quantity,
                                           "S_YTD": s_ytd,
                                           "S_ORDER_CNT": s_order_cnt,
                                           "S_REMOTE_CNT": s_remote_cnt})
            else:
                si["$comment"] = comment

                self.stock.execute("NEW_ORDER","UpdateStock",{"S_QUANTITY":s_quantity,"S_YTD":s_ytd,"S_ORDER_CNT":s_order_cnt,"S_REMOTE_CNT":s_remote_cnt,"S_W_ID":si["S_W_ID"],"S_I_ID":si["S_I_ID"]})

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            ## IF

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            ol = {"OL_O_ID": d_next_o_id, "OL_NUMBER": ol_number, "OL_I_ID": ol_i_id,
                  "OL_SUPPLY_W_ID": ol_supply_w_id, "OL_DELIVERY_D": o_entry_d,
                  "OL_QUANTITY": ol_quantity, "OL_AMOUNT": ol_amount, "OL_DIST_INFO": str(s_dist_xx)}

            if self.denormalize:
                # createOrderLine
                o[constants.TABLENAME_ORDER_LINE].append(ol)
            else:
                ol["OL_D_ID"] = d_id
                ol["OL_W_ID"] = w_id

                # createOrderLine
                if self.batch_writes:
                    order_line_writes.append(ol)
                else:
                    print('stock: insert_one')
                    self.order_line.insert_one(ol, session=s)

            ## Add the info to be returned
            item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))
        ## FOR
        
        ## Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        if self.batch_writes:
            if not self.denormalize:
                print('order_line insert_many')
                self.order_line.insert_many(order_line_writes, session=s)

        # createOrder
        o['Category'] = 'ORDERS'
        self.orders.execute("NEW_ORDER", "Orders_insert_one", {'key':uuid.uuid1(), 'content': o})

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [(w_tax, d_tax, d_next_o_id, total)]        
        return [c, misc, item_data], 0


    def TPCC_analytic(self, params, thread_id):
        analytic_node = False 
        c_state = rand.astring(constants.STATE, constants.STATE)

        if analytic_node:      
            query = 'SELECT c.C_ID, COUNT(*) AS OC_COUNT \
                FROM \
                    tpcc_customer c, tpcc_order o \
                WHERE \
                    to_bigint(c.C_ID)  /*+ indexnl */ = o.O_C_ID \
                    AND to_bigint(c.C_D_ID) /*+ indexnl */ = o.O_D_ID \
                    AND to_bigint(c.C_W_ID) /*+ indexnl */ = o.O_W_ID \
                AND c.C_STATE = "{}"\
                GROUP BY c.C_ID \
                ORDER BY OC_COUNT DESC, c.C_ID ASC \
                LIMIT 10;'.format(c_state)

            res = Analytic_query(query, thread_id)
        else:
            query = "SELECT \
                c.C_ID, COUNT(*) AS OC_COUNT\
                FROM \
                TPCC o JOIN TPCC c  \
            USE HASH (BUILD) \
                ON\
                o.O_C_ID = c.C_ID \
                AND o.O_D_ID = c.C_D_ID \
                AND o.O_W_ID = c.C_W_ID \
                WHERE c.Category = 'CUSTOMER' AND o.Category = 'ORDERS' AND c.C_STATE = '{}'\
                GROUP BY c.C_ID \
                ORDER BY OC_COUNT DESC, c.C_ID ASC \
                LIMIT 10;".format(c_state)

            res = N1QL_query(query, thread_id)
        print(res)
        return res['results'], 0


    def run_transaction(self, txn_callback, session, name, params):
        if self.no_transactions:
            return (True, txn_callback(session, params))
        try:
            # this implicitly commits on success
            with session.start_transaction():
                return (True, txn_callback(session, params))
        except pymongo.errors.OperationFailure as exc:
            # exc.code in (24, 112, 244):  LockTimeout, WriteConflict, TransactionAborted
            if exc.has_error_label("TransientTransactionError"):
                logging.debug("OperationFailure with error code: %d (%s) during operation: %s",
                              exc.code, exc.details, name)
                return (False, None)
            logging.error("Failed with unknown OperationFailure: %d", exc.code)
            print("Failed with unknown OperationFailure: %d" % exc.code)
            print(exc.details)
            raise
        except pymongo.errors.ConnectionFailure:
            print("ConnectionFailure during %s: " % name)
            return (False, None)
        ## TRY

    # Should we retry txns within the same session or start a new one?
    def run_transaction_with_retries(self, txn_callback, name, params):
        txn_retry_counter = 0
        to = pymongo.client_session.TransactionOptions(
            read_concern=None,
            #read_concern=pymongo.read_concern.ReadConcern("snapshot"),
            write_concern=self.write_concern,
            read_preference=pymongo.read_preferences.Primary())
        with self.client.start_session(default_transaction_options=to,
                                       causal_consistency=self.causal_consistency) as s:
            while True:
                (ok, value) = self.run_transaction(txn_callback, s, name, params)
                if ok:
                    if txn_retry_counter > 0:
                        logging.debug("Committed operation %s after %d retries",
                                      name,
                                      txn_retry_counter)
                    return (value, txn_retry_counter)
                ## IF

                # backoff a little bit before retry
                txn_retry_counter += 1
                sleep(txn_retry_counter * .1)
                logging.debug("txn retry number for %s: %d", name, txn_retry_counter)
            ## WHILE

    def get_server_status(self):
        return 'normal'

    def save_result(self, result_doc):
        self.result_doc.update(result_doc)
        self.result_doc['after']=self.get_server_status()
        # self.client.test.results.save(self.result_doc)

## CLASS

