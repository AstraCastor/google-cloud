from google.cloud import talent_v4beta1
import os,sys,logging,argparse, inspect
from datetime import datetime
from modules import cts_db,cts_tenant,cts_company

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

#TODO:Add file logging

class Job:
    def __init__(self,credentials=None):
        try:
            main_dir = os.path.dirname(__file__)
            credential_file = os.path.join(main_dir,'../res/secrets/pe-cts-poc-0bbb0b044fea.json')
            logger.debug("credentials: {}".format(credential_file))
            self._job_client = talent_v4beta1.JobServiceClient.from_service_account_file(credential_file)
            logger.debug("Job client created: {}".format(self._job_client))
            self._db_connection = cts_db.DB().connection()
            logger.debug("Job db connection obtained: {}".format(self._db_connection))
        except Exception as e:
            logging.exception("Error instantiating Job. Message: {}".format(e))
    

    def client(self):
        return self._job_client
    
    def create_job(self,project_id,tenant_id=None,job_object=None,file=None):
        print ("create job") if file is None else print ("batch create job")

    def update_job(self,tenant_id,project_id=None,job=None,file=None):
        print ("update job") if file is None else print ("batch update job")
    
    def delete_job(self,project_id,tenant_id=None,external_id=None,all=False,forced=False):
        print ("delete job") if all is False else print ("batch create job")

    def get_job(self,project_id,tenant_id=None,external_id=None,all=False):
        print ("get job") if all is False else print ("list job")

