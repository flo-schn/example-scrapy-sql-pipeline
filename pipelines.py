import os
from dotenv import load_dotenv
import pymysql
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

# loading the database credentials
load_dotenv(".env")

class SQLpipe:

    def __init__(self):
        self.host = os.environ.get("dbserver_ip")
        self.user = os.environ.get("dbuser")
        self.password = os.environ.get("dbpass")
        self.db = os.environ.get("dbname")
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
        # create two conncetions to database, 1 to check if table is already in it and 2 to check if item is aleady in table of database
        self.cursor = self.dbconnection.cursor()
        self.cursor2 = self.dbconnection.cursor()

        # checks if table exists, if it doesnt, creates it
        self.cursor.execute("SHOW TABLES LIKE 'news_paper'")
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
            self.cursor2.execute('''
                SELECT article_id FROM news_paper WHERE article_id = %s 
            ''', (item['article_id']))
            if len(self.cursor2.fetchall()) == 0:
                # if item is not alraedy in database then dumping item into database
                self.cursor.execute('''
                    INSERT INTO news_paper
                    (title, 
                    link, 
                    subject, 
                    date, 
                    article_id, 
                    Category, 
                    Type,
                    scrape_date) 
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, NOW())
                ''', [item['title'], item['link'], item['subject'], item['date'], item['article_id'], item['Category'], item['Type']])

                self.dbconnection.commit()
                return item
            # if item is already in database then drop item
            else:
                raise DropItem(f"SQLPipe: Duplicate item found: {item!r}")
      
        # catch any potential sql errors
        except pymysql.Error as e:
            raise DropItem(f'Error inserting item: {e}')

    # close connection to database once spider finishes crawling
    def close_spider(self, spider):
        self.dbconnection.close()
