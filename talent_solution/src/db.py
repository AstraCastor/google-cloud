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
            __cursor = __connection.cursor()
            logger.info("DB Connection created.")
        except ConnectionError as e:
            logger.exception("Error when opening DB connection. Message:{}".format(e))
        finally:
            __connection.close

    def get_db_connection(self):
        return self.__cursor




