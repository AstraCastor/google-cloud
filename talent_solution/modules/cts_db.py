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

class DB():

    def __init__(self):
        try:
            __connection = sqlite3.connect("./res/cts.db",isolation_level=None)
            self.__cursor = __connection.cursor()
            logger.debug("CTS Database connected.")
            self.setup_cts_schema(self.__cursor)
        except ConnectionError as e:
            logger.exception("Error when opening DB connection. Message:{}".format(e))
        finally:
            __connection.close

    def connection(self):
        return self.__cursor

    def setup_cts_schema(self,cursor):
        try:
            sql_create_table = {}
            sql_create_table['metadata'] = "CREATE TABLE IF NOT EXISTS metadata (   \
                                            table_nm TEXT PRIMARY KEY, \
                                            sync_time INTEGER)"

            sql_create_table['tenant'] = "CREATE TABLE IF NOT EXISTS tenant (  \
                                        tenant_key TEXT PRIMARY KEY,     \
                                        tenant_name TEXT NOT NULL,   \
                                        external_id TEXT,   \
                                        project_id TEXT NOT NULL,    \
                                        suspended INTEGER,  \
                                        create_time INTEGER \
                                        )"

            sql_create_table['company'] = "CREATE TABLE IF NOT EXISTS company (  \
                                        company_key TEXT PRIMARY KEY, \
                                        external_id TEXT NOT NULL,   \
                                        company_name TEXT,   \
                                        tenant_name TEXT, \
                                        project_id TEXT NOT NULL,    \
                                        suspended INTEGER,  \
                                        create_time INTEGER    \
                                        )"

            for table in sql_create_table:
                cursor.execute(sql_create_table[table])
                logger.debug("Table {} is available.".format(table))
        except Exception as e:
            logger.exception("Error when creating table {}. Message: {}".format(table,e))

