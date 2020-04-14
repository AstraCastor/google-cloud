from google.cloud import talent_v4beta1
import os
import sys
import logging
import argparse
from datetime import datetime
from . import cts_db,cts_tenant

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
        try:
            main_dir = os.path.dirname(__file__)
            credential_file = os.path.join(main_dir,'../res/secrets/pe-cts-poc-0bbb0b044fea.json')
            logger.debug("credentials: {}".format(credential_file))
            self._company_client = talent_v4beta1.CompanyServiceClient.from_service_account_file(credential_file)
            logger.debug("Company client created: {}".format(self._company_client))
            self._db_connection = cts_db.DB().connection()
            logger.debug("Company db connection obtained: {}".format(self._db_connection))
        except Exception as e:
            logging.exception("Error instantiating Company. Message: {}".format(e))

    def create_company(self,object=None,mode='online',file=None):
        pass

    def delete_company(self,external_id=None,project_id=None,mode='online',file=None,all=False):
        pass

    def update_company(self,object=None,mode='online',file=None):
        pass

    def get_company(self,external_id=None,project_id=None,all=False):
        pass

if __name__ == '__main__':
    Company()

