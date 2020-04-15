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

    def create_company(self,tenant_id=None,project_id=None,company=None,file=None):
        pass

    def delete_company(self,tenant_id=None,project_id=None,external_id=None,all=False,forced=False):
        pass

    def update_company(self,tenant_id,project_id=None,company=None,file=None):
        pass

    def get_company(self,project_id,tenant_id=None,external_id=None,all=False):
        """ Get CTS company by name or get all CTS companies by project.
        Args:
            project_id: Project where the company will be created - string
            external_id: Unique ID of the company - string
            all: List all companies. Mutually exclusive with external_id - Boolean.
        Returns:
            an instance of company or None if company was not found.
        """
        logger.debug("Get company called.")
        db = self._db_connection
        client = self._company_client
        try:
            if external_id is not None:
                if all:
                    logging.exception("Conflicting arguments: --external_id and --all are mutually exclusive.")
                    raise ValueError
                company_key = project_id+"-"+tenant_id+"-"+external_id if project_id is not None and tenant_id is not None \
                else project_id+"-"+external_id
                db.execute("SELECT company_name FROM company where company_key = '{}'".format(company_key))
                rows = db.fetchall()
                if rows == []:
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    company = client.get_company(rows[0][1])
            elif all:
                if tenant_id is not None:
                    # tenant = cts_tenant.Tenant()
                    # tenant_obj = tenant.get_tenant(project_id,tenant_id)
                    # logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
                    # if tenant_obj is None:
                    #     logging.error("Unknown Tenant: {}".format(tenant_id))
                    #     exit(1)
                    # parent = tenant_obj.name
                    db.execute("SELECT distinct tenant_name FROM company where company_key like '{}%'".format(project_id+"-"+tenant_id+"-"))
                    rows = db.fetchall()
                    if rows == []:
                        return []
                    else:
                        parent = rows[0][0]
                    logger.debug("Parent path: {}".format(parent))
                else:
                    parent = client.project_path(project_id)
                company = [t for t in client.list_companies(parent)]
                return company
            else:
                logger.exception("Invalid arguments.")
                raise AttributeError
        except Exception as e:
            logger.error("Error getting company by name {}. Message: {}".format(external_id,e))
            raise 

if __name__ == '__main__':
    Company()