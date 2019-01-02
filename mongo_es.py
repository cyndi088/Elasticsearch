# -*- coding: utf-8 -*-
import time
from datetime import datetime
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dateutil import parser


class ElasticObj:
    def __init__(self, mongo_url, mongo_db, collection, index_name, index_type, es_ip):
        '''

        :param index_name: 索引名称
        :param index_type: 索引类型
        '''
        self.client = MongoClient(mongo_url)
        self.db_mongo = self.client[mongo_db]
        self.db_coll = self.db_mongo[collection]
        self.index_name = index_name
        self.index_type = index_type
        # 无用户名密码状态
        self.es = Elasticsearch(es_ip)
        #用户名密码状态
        # self.es = Elasticsearch([ip],http_auth=('elastic', 'password'),port=9200)

    def create_index(self):
        '''
        创建索引,创建索引名称为zhejiang，类型为sheng的索引
        :param ex: Elasticsearch对象
        :return:
        '''
        #创建映射
        _index_mappings = {
            "mappings": {
                self.index_type: {
                    "properties": {
                        "stampDateTime": {
                            "type": "date",
                        },
                        "commodityName": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "corpNameBy": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "address_by": {
                            "type": "text",
                            # "analyzer": "ik_max_word",
                            # "search_analyzer": "ik_max_word"
                        },
                        "addressByRegionId": {
                            "type": "integer"
                        },
                        "corpName": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "address": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "addressRegionId": {
                            "type": "integer"
                        },
                        "createDate": {
                            "type": "text",
                        },
                        # "fl": {
                        #     "type": "text",
                        #     "analyzer": "ik_max_word",
                        #     "search_analyzer": "ik_max_word"
                        # },
                        "flId": {
                            "type": "integer"
                        },
                        "ggh": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "ggrq": {
                            "type": "date",
                        },
                        # "rwly": {
                        #     "type": "text",
                        #     "analyzer": "ik_max_word",
                        #     "search_analyzer": "ik_max_word"
                        # },
                        "rwly_id": {
                            "type": "integer"
                        },
                        # "id": {
                        #     "type": "text",
                        # },
                        # "inspectionUnit": {
                        #     "type": "text",
                        #     "analyzer": "ik_max_word",
                        #     "search_analyzer": "ik_max_word"
                        # },
                        "model": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        # "newsDetailType": {
                        #     "type": "integer",
                        # },
                        "newsDetailTypeId": {
                            "type": "integer"
                        },
                        "note": {
                            "type": "text",
                        },
                        "productionDate": {
                            "type": "text",
                        },
                        # "sampleOrderNumber": {
                        #     "type": "text",
                        # },
                        # "status": {
                        #     "type": "integer",
                        # },
                        # "statusEnumValue": {
                        #     "type": "text",
                        # },
                        "trademark": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        # "transId": {
                        #     "type": "text",
                        # },
                        "unqualifiedItem": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "checkResult": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        "standardValue": {
                            "type": "text",
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word"
                        },
                        # "approvalNumber": {
                        #     "type": "text",
                        # },
                        "batchNumber": {
                            "type": "text",
                        }
                    }
                }

            }
        }
        if self.es.indices.exists(index=self.index_name) is not True:
            res = self.es.indices.create(index=self.index_name, body=_index_mappings)
            print(res)

    def bulk_Index_Data(self):
        '''
        用bulk将批量数据存储到es
        :return:
        '''

        ACTIONS = []
        i = 1
        self.mongoRecordRes = self.db_coll.find()
        for record in self.mongoRecordRes:
            print(i)
            print(record)
            if "addressBy" in record:
                action = {
                    "_index": self.index_name,
                    "_type": self.index_type,
                    "_id": str(record.pop('_id')),
                    "_source": {
                        "stampDateTime": record["stampDateTime"],
                        "commodityName": record["commodityName"],
                        "corpNameBy": record["corpNameBy"],
                        "address_by": record["addressBy"],
                        "addressByRegionId": record["addressByRegionId"],
                        "corpName": record["corpName"],
                        "address": record["address"],
                        "addressRegionId": record["addressRegionId"],
                        "createDate": record["createDate"],
                        # "fl": record["fl"],
                        "flId": self.fl(record["flId"]),
                        "ggh": record["ggh"],
                        "ggrq": record["ggrq"],
                        "rwly_id": record["rwly_id"],
                        # "rwly_id": self.rwly(record["rwly_id"]),
                        # "id": record["id"],
                        # "inspectionUnit": record["inspectionUnit"],
                        "model": record["model"],
                        # "newsDetailType": record["newsDetailType"],
                        "newsDetailTypeId": record["newsDetailTypeId"],
                        "note": record["note"],
                        "productionDate": record["productionDate"],
                        # "sampleOrderNumber": record["sampleOrderNumber"],
                        # "status": record["status"],
                        # "statusEnumValue": record["statusEnumValue"],
                        "trademark": record["trademark"],
                        # "transId": record["transId"],
                        "unqualifiedItem": record["unqualifiedItem"],
                        "checkResult": record["checkResult"],
                        "standardValue": record["standardValue"],
                        # "approvalNumber": record["approvalNumber"],
                        "batchNumber": record["batchNumber"]}
                }
                i += 1
                ACTIONS.append(action)
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=False)
        print('Performed %d actions' % success)

    @staticmethod
    def time_process(str):
        try:
            if "/" in str:
                return datetime.now()
        except:
            pass

    @staticmethod
    def address_by(str):
        try:
            return str
        except KeyError as e:
            pass

    @staticmethod
    def str_time(str):
        if '.' in str:
            stp = time.strptime(str, '%Y.%m.%d')
            ss2 = parser.parse(time.strftime("%Y-%m-%d", stp))
            return ss2
        else:
            return str

    @staticmethod
    def func(str):
        if not str:
            return '/'
        else:
            return str

    @staticmethod
    def rwly(num):
        if num == 1:
            return 520
        elif num == 2:
            return 521
        elif num == 3:
            return 522
        elif num == 4:
            return 523
        else:
            return 524

    @staticmethod
    def fl(num):
        if num == 8:
            return 80
        elif num >=75 and num <=82:
            return num
        else:
            return 82


obj = ElasticObj("106.14.176.62:27017", "zhejiang", "sheng", "zhejiang", "sheng", "http://47.98.210.22:9200")
obj.create_index()
obj.bulk_Index_Data()

