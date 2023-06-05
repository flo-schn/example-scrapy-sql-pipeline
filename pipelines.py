import os
from dotenv import load_dotenv
import pymysql
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

# loading the database credentials
load_dotenv(".env")

class SQLpipe:

    def __init__(self):
        # list needed to collect additional info for logging spider behaviour
        self.sql_errors = []
        # geting database credentials into pipeline
        self.host = os.environ.get("dbServer_ip")
        self.user = os.environ.get("dbUser")
        self.password = os.environ.get("dbpass")
        self.db = os.environ.get("dbName")
        self.table = os.environ.get("dbTable")
        self.charset = os.environ.get('dbCharSet')

    # establishes connection once spider starts crawling
    def open_spider(self, spider):
        self.dbconnection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.db,
            charset=self.charset,
        )
        # create conncetion to database
        self.cursor = self.dbconnection.cursor()

        # checks if table exists, if it doesnt, creates it
        self.cursor.execute("select table_name from information_schema.tables where table_name = '{table}'".format(table=self.table))
        if not self.cursor.fetchone():
            self.cursor.execute('''
                CREATE TABLE news_paper (
                    id SERIAL,
                    title varchar(255),
                    link varchar(255),
                    subject varchar(255),
                    date DATETIME,
                    article_id varchar(255),
                    Category varchar(255),
                    Type varchar(255),
                    scrape_date DATETIME
                )
            ''')
            self.dbconnection.commit()

    def process_item(self, item, spider):
        try:
            # checking if item is already in database
            self.cursor.execute('SELECT article_id FROM {table} WHERE article_id = %s'.format(table=self.table), (item['article_id']))
            if not self.cursor.fetchone():
                # if item is not alraedy in database then dumping item into database
                self.cursor.execute('''
                    INSERT INTO {table}
                    (title, 
                    link, 
                    subject, 
                    date, 
                    article_id, 
                    Category, 
                    Type,
                    scrape_date) 
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, NOW())'''.format(table=self.table), 
                    [item['title'], item['link'], item['subject'], item['date'], item['article_id'], item['Category'], item['Type']])
                self.dbconnection.commit()
                # supplementing scrapy stats
                spider.crawler.stats.inc_value('item_scraped_count/new_item')


            # if item is already in database then drop item
            else:
                # supplementing scrapy stats and droping item
                spider.crawler.stats.inc_value('item_dropped_count/already_in_db') 
                raise DropItem(f"SQLPipe: Duplicate item found: {item!r}")
      
        # catch any potential sql errors
        except pymysql.Error as e:
            # supplementing scrapy stats and droping item
            spider.crawler.stats.inc_value('item_dropped_count/sql_insertion_error')
            self.sql_errors.append(f"Error inserting item: {e}")
            raise DropItem(f'Error inserting item: {e}')

    def close_spider(self, spider):
        # supplementing scrapy stats
        spider.crawler.stats.set_value('sql_errors', ' - '.join(self.sql_errors))
        # closing connection to db
        self.dbconnection.close()