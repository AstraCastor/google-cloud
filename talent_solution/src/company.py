from google.cloud import talent_v4beta1
import os
import sys
import logging
import importlib
import argparse
import sqlite3
from sqlite3 import Error

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

class Company:
    def __init__(self):
        pass

    def create_company(self,object=None,mode='online',file=None):
        pass

    def delete_company(self,external_id=None,project_id=None,mode=online,file=None,all=False):
        pass

    def update_company(self,object=None,mode='online',file=None):
        pass
    
    def get_company(self,external_id=None,project_id=None,all=False):
        pass

