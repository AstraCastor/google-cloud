import sqlite3
import logging
import sys
import os
from res import config as config
from modules.cts_errors import CTSSchemaError

#General logging config
logger = logging.getLogger()


class DB():

    def __init__(self):
        try:
            # __connection = sqlite3.connect("./res/cts.db",isolation_level=None)
            __connection = sqlite3.connect(config.DATABASE['file'],isolation_level=None)
            self.__cursor = __connection.cursor()
            logger.debug("CTS Database connected.")
            self.check_cts_schema(self.__cursor)
        except ConnectionError as e:
            logger.error("Error when opening DB connection. Message:{}".format(e),exc_info=config.LOGGING['traceback'])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self.__cursor

    def cts_schema(self):
        sql_cts_table = {}

        sql_cts_table['metadata'] = "CREATE TABLE IF NOT EXISTS metadata (   \
                                        table_nm TEXT PRIMARY KEY, \
                                        sync_time INTEGER)"

        sql_cts_table['tenant'] = "CREATE TABLE IF NOT EXISTS tenant (  \
                                    tenant_key TEXT PRIMARY KEY,     \
                                    tenant_name TEXT NOT NULL,   \
                                    external_id TEXT,   \
                                    project_id TEXT NOT NULL,    \
                                    suspended INTEGER,  \
                                    create_time INTEGER \
                                    )"

        sql_cts_table['company'] = "CREATE TABLE IF NOT EXISTS company (  \
                                    company_key TEXT PRIMARY KEY, \
                                    external_id TEXT NOT NULL,   \
                                    company_name TEXT,   \
                                    tenant_name TEXT, \
                                    project_id TEXT NOT NULL,    \
                                    suspended INTEGER,  \
                                    create_time INTEGER    \
                                    )"

        sql_cts_table['job'] = "CREATE TABLE IF NOT EXISTS job (  \
                                    job_key TEXT PRIMARY KEY, \
                                    external_id TEXT NOT NULL,   \
                                    language_code TEXT NOT NULL, \
                                    job_name TEXT, \
                                    company_name TEXT NOT NULL,   \
                                    tenant_name TEXT, \
                                    project_id TEXT NOT NULL,    \
                                    suspended INTEGER,  \
                                    create_time INTEGER    \
                                    )"
        return sql_cts_table


    def check_cts_schema(self,cursor):
        try:
            schema = self.cts_schema()
            errors = []
            for table in schema:
                try:
                    cursor.execute("select True from {}".format(table))
                except sqlite3.OperationalError as e:
                    logger.error(e)
                    logger.warn("DB Check: {} is missing and will be recreated.".format(table)) 
                    self.create_cts_table(cursor,table)
                except Exception as e:
                    errors.append(e)
                else:
                    logger.debug("DB check: {} OK".format(table))
            if errors:
                #TODO: Create custom MultipleErrors
                raise CTSSchemaError(errors)
        except Exception as e:
            logger.error("Schema check failed. Message: {}".format(e),exc_info=config.LOGGING['traceback'])


    def create_cts_table(self,cursor,table=None):
        try:
            schema = self.cts_schema()
            if table is None:
                for t in schema:
                    cursor.execute(schema[t])
                    logger.debug("Table {} is created.".format(t))
            else:
                cursor.execute(schema[table])
                logger.debug("Table {} is created.".format(table))

        except Exception as e:
            logger.error("Error when creating table {}. Message: {}".format(table,e),exc_info=config.LOGGING['traceback'])

