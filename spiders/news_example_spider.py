import scrapy
import pymysql
from dotenv import load_dotenv
import os
import json
load_dotenv("../.env")


class NewsExampleSpider(scrapy.Spider):
    name = "news-example-spider"
    allowed_domains = ["news-example.com"]
    start_urls = ["https://news-example.com"]

    def parse(self, response):
        pass

    def __init__(self):
        # loading database credentials into spider
        self.host = os.environ.get("dbServer_ip")
        self.user = os.environ.get("dbUser")
        self.password = os.environ.get("dbpass")
        self.db = os.environ.get("dbName")
        self.tableLog = os.environ.get("dbTableLog")
        self.charset = os.environ.get('dbCharSet')

    def closed(self, reason):
        # connecting to database
        self.dbconnection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.db,
            charset=self.charset,
        )
        self.cursor = self.dbconnection.cursor()

        # supplementing scrapy stats
        self.crawler.stats.set_value('spider_name', self.name)

        # checks if table exists. if it doesnt, creates it
        self.cursor.execute("SHOW TABLES LIKE '{table}'".format(table=self.tableLog))
        if not self.cursor.fetchone():
            self.cursor.execute('''
                CREATE TABLE {table} (
                    id SERIAL,
                    log_stats JSON
                )'''.format(table=self.tableLog))
            self.dbconnection.commit()

        # converting log stats dict into json
        self.stt = json.dumps(self.crawler.stats.get_stats(), sort_keys=True, default=str)
        try:
            # pushing log stats as json to database
            self.cursor.execute("INSERT INTO {table} (log_stats) VALUES (%s)".format(table=self.tableLog),self.stt)
            self.dbconnection.commit() 
        except pymysql.Error as e:
            print(e)         
        # closing connection to database
        self.dbconnection.close()
