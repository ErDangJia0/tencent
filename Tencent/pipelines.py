# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import pymysql
from Tencent.settings import *


class TencentPipeline(object):
    def process_item(self, item, spider):
        return item


# 存入Mongo
class TencentMongoPipeline(object):
    def __init__(self):
        self.conn = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        self.db = self.conn[MONGO_DB]
        self.myset = self.db[MONGO_SET]

    def process_item(self, item, spider):
        # 把item对象转为字典
        tencent = dict(item)
        self.myset.insert_one(tencent)
        return item


# 存入Mysql数据库
class TencentMysqlPipeline(object):
    def __init__(self):
        self.db = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PWD, MYSQL_DB, charset='utf8')
        self.cursor = self.db.cursor()

    def process_item(self, item, spider):
        # 存入MYSQL数据库
        ins='insert into info values(%s,%s,%s,%s,%s,%s)'
        L=[item['zhName'],item['zhType'],item['zhNum'],item['zhAddress'],item['zhTime'],item['zhLink']]
        # 执行插入语句，列表传参补位
        self.cursor.execute(ins, L)
        # 提交到数据库执行
        self.db.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.db.close()
        print('爬虫关闭')
