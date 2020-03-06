# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb

class DoubanPipeline(object):
    # 构造函数用于初始化mysql的各项参数
    def __init__(self):
        host = sql_host
        dbname = sql_db_name
        ruser = sql_user
        rpassword = sql_password
        self.sheetname = sql_sheetname
        # 连接数据库
        self.conn = pymysql.connect(host=host, user=ruser, password=rpassword, db=dbname, charset='utf8')
        # 创建一个游标
        self.cursor = self.conn.cursor()

    # 防止连接出现错误
    def open_spider(self, spider):
        try:
            host = sql_host
            dbname = sql_db_name
            ruser = sql_user
            rpassword = sql_password
            self.sheetname = sql_sheetname
            # 连接数据库
            self.conn = pymysql.connect(host=host, user=ruser, password=rpassword, db=dbname, charset='utf8')
            # 创建一个游标
            self.cursor = self.conn.cursor()
        except:
            self.open_spider()
        else:
            spider.logger.info('MySQL: connected')
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            spider.cursor = self.cursor

    def process_item(self, item, spider):
        # item是从douban_spider里传出的数据
        # 先将数据转换为字典形式
        data = dict(item)
        # mysql插入数据
        keys = ','.join(data.keys())
        values = ','.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES({values})'.format(table=self.sheetname, keys=keys, values=values)
        try:
            self.cursor.execute(sql, tuple(data.values()))
            self.conn.commit()
        except:
            self.conn.rollback()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
