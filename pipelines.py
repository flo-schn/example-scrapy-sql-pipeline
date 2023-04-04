import os
from dotenv import load_dotenv
import pymysql
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

# loading the database credentials
load_dotenv(".env")

class SQLpipe:

    def __init__(self):
        # geting database credentials into pipeline
        self.host = os.environ.get("dbserver_ip")
        self.user = os.environ.get("dbuser")
        self.password = os.environ.get("dbpass")
        self.db = os.environ.get("dbname")
        self.table = os.environ.get("dbtable")
        self.charset = 'utf8mb4'

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
            if len(self.cursor.fetchall()) == 0:
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

            # if item is already in database then drop item
            else:
                raise DropItem(f"SQLPipe: Duplicate item found: {item!r}")
      
        # catch any potential sql errors
        except pymysql.Error as e:
            raise DropItem(f'Error inserting item: {e}')

    # close connection to database once spider finishes crawling
    def close_spider(self, spider):
        self.dbconnection.close()
