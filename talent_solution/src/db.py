import sqlite3
import logging
import sys
import os

#General logging config
log_level = os.environ.get('LOG_LEVEL','INFO')
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger_format = logging.Formatter('%(asctime)s %(filename)s:%(lineno)s %(levelname)-8s %(message)s','%Y-%m-%d %H:%M',)

#Console Logging
console_logger = logging.StreamHandler()
console_logger.setLevel(log_level)
console_logger.setFormatter(logger_format)
logger.addHandler(console_logger)

class cts_db():

    def __init__(self):
        try:
            __connection = sqlite3.connect("./res/cts.db")
            self.__cursor = __connection.cursor()
            logger.info("DB Connection created.")
            self.setup_cts_schema(self.__cursor)
        except ConnectionError as e:
            logger.exception("Error when opening DB connection. Message:{}".format(e))
        finally:
            __connection.close


    def setup_cts_schema(self,cursor):
        try:
            sql_create_table = {}
            sql_create_table['tenant'] = "CREATE TABLE IF NOT EXISTS tenant (  \
                                        external_id TEXT PRIMARY KEY,   \
                                        tenant_name TEXT,   \
                                        suspended INTEGER,  \
                                        create_time INTEGER)"

            sql_create_table['company'] = "CREATE TABLE IF NOT EXISTS company (  \
                                        external_id TEXT PRIMARY KEY,   \
                                        company_name TEXT,   \
                                        suspended INTEGER,  \
                                        create_time INTEGER)"

            for table in sql_create_table:
                cursor.execute(sql_create_table[table])
                logger.info("Table {} created.".format(table))
        except Exception as e:
            logger.exception("Error when creating table {}. Message: {}".format(table,e))


    def connection(self):
        return self.__cursor




