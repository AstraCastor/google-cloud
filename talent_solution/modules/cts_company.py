from google.cloud import talent_v4beta1,storage
import os,sys,logging,argparse, inspect
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
            logging.debug("Company instantiated.")
        except Exception as e:
            logging.exception("Error instantiating Company. Message: {}".format(e))

    def create_company(self,project_id,tenant_id=None,company_object=None,file=None):
        logger.debug("CALLED: create_company({},{},{},{} by {})".format(project_id,tenant_id,company_object,file,inspect.currentframe().f_back.f_code.co_name,))
        db = self._db_connection
        client = self._company_client
        try:
            logger.debug("Company input: Type {}\n{}".format(type(company_object),company_object))
            if isinstance(company_object,dict):
                external_id = company_object['external_id']
                # Check if it is an existing company
                logger.debug("{}:Calling get_company({},{})".format(inspect.currentframe().f_code.co_name,project_id,external_id))
                existing_company = self.get_company(project_id=project_id,tenant_id=tenant_id,external_id=external_id)
                if existing_company is None:
                    if tenant_id is not None:
                        # To set the parent of the company to be created to the tenant_name 
                        # for the given tenant_id(tenant external_id)
                        tenant = cts_tenant.Tenant()
                        tenant_obj = tenant.get_tenant(project_id,tenant_id)
                        logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
                        if tenant_obj is None:
                            logging.error("Unknown Tenant: {}".format(tenant_id))
                            exit(1)
                        parent = tenant_obj.name
                        tenant_name = tenant_obj.name
                    else:
                        tenant_name = None
                        tenant_id = ""
                        parent = client.project_path(project_id)
                    logger.debug("{}:Parent path set to: {}".format(inspect.currentframe().f_code.co_name,parent))
                    new_company = client.create_company(parent,company_object)
                    query = "INSERT INTO company (company_key,external_id,company_name,tenant_name,project_id,suspended,create_time)    \
                        VALUES ('{}','{}','{}','{}','{}','{:d}','{}')".format(project_id+"-"+tenant_id+"-"+external_id,new_company.external_id,    \
                            new_company.name,tenant_name,project_id,0,datetime.now())
                    logger.debug("QUERY: {}".format(query))
                    db.execute(query)
                    logger.info("Company {} created.\n{}".format(external_id,new_company))
                    print("Company {} created.\n{}".format(external_id,new_company))
                    return new_company
                else:
                    logger.error("{}: Company {} already exists.\n{}".format(inspect.currentframe().f_code.co_name,external_id,existing_company))
                    return None
            else:
                logger.error("{}:Invalid or missing company argument. Should be a valid company object.\n {}"\
                    .format(inspect.currentframe().f_code.co_name,company_object))
                raise ValueError
        except Exception as e:
            print("{}:Error creating company:\n{}\n{}".format(inspect.currentframe().f_code.co_name,company_object,e))
            self.delete_company(project_id,tenant_id,company_object['external_id'],forced=True)
            raise

    def delete_company(self,project_id,tenant_id=None,external_id=None,all=False,forced=False):
        """ Delete a CTS company by external name.
        Args:
            project_id: project where the company will be created - string
            external_id: unique ID of the company - string
        Returns:
            None - If company is not found.
        """
        logger.debug("CALLED: delete_company({},{},{},{},{} by {})".format(project_id,tenant_id,external_id,all,forced,inspect.currentframe().f_back.f_code.co_name))
        db = self._db_connection
        client = self._company_client
        try:
            if forced:
                all_companies = self.get_company(project_id=project_id,tenant_id=tenant_id,all=True)
                logger.debug("Total companies retrieved: {}".format(len(all_companies)))
                if len(all_companies) != 0:
                    for company in all_companies:
                        if company.external_id == external_id:
                            existing_company = company
                            break
                        else:
                            existing_company = None
                else:
                    existing_company = None
            else:
                logger.debug("{}:Calling get_company({},{},{})".format(inspect.currentframe().f_code.co_name,project_id,tenant_id,external_id))
                existing_company = self.get_company(project_id=project_id,tenant_id=tenant_id,external_id=external_id)
                logger.debug("{}:Existing company? {}".format(inspect.currentframe().f_code.co_name,existing_company))
            if existing_company is not None:
                logger.info("Deleting company id: {}".format(existing_company.external_id))
                client.delete_company(existing_company.name)
                db.execute("DELETE FROM company where company_name = '{}'".format(existing_company.name))
                logger.info("Company {} deleted.".format(external_id))
                print("Company {} deleted.".format(external_id))

            else:
                logger.error("{}: Company {} does not exist.".format(inspect.currentframe().f_code.co_name,external_id))
                print("Company {} does not exist.".format(external_id))
                return None
        except Exception as e:
            print("{}:Error deleting company {}: {}".format(inspect.currentframe().f_code.co_name,external_id,e))
            raise

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
        logger.debug("CALLED: get_company({},{},{},{} by {})".format(project_id,tenant_id,external_id,all,inspect.currentframe().f_back.f_code.co_name))
        db = self._db_connection
        client = self._company_client
        try:
            if external_id is not None:
                if all:
                    logging.exception("{}:Conflicting arguments: --external_id and --all are mutually exclusive.".format(inspect.currentframe().f_code.co_name))
                    raise ValueError
                company_key = project_id+"-"+tenant_id+"-"+external_id if project_id is not None and tenant_id is not None \
                else project_id+"-"+external_id
                logger.debug("{}:Searching for company: {}".format(inspect.currentframe().f_code.co_name,company_key))
                db.execute("SELECT company_name FROM company where company_key = '{}'".format(company_key))
                rows = db.fetchall()
                if rows == []:
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    company = client.get_company(rows[0][0])
            elif all:
                if tenant_id is not None:
                    tenant = cts_tenant.Tenant()
                    tenant_obj = tenant.get_tenant(project_id,tenant_id)
                    logger.debug("{}:Tenant retrieved:\n{}".format(inspect.currentframe().f_code.co_name,tenant_obj))
                    if tenant_obj is None:
                        logging.error("{}:Unknown Tenant: {}".format(inspect.currentframe().f_code.co_name,tenant_id))
                        exit(1)
                    parent = tenant_obj.name
                else:
                    parent = client.project_path(project_id)
                logger.debug("{}:Parent path: {}".format(inspect.currentframe().f_code.co_name,parent))
                company = [t for t in client.list_companies(parent)]
            else:
                logger.exception("Invalid arguments.")
                raise AttributeError
            return company
        except Exception as e:
            logger.error("{}:Error getting company by name {}. Message: {}".format(inspect.currentframe().f_code.co_name,external_id,e))
            raise 

if __name__ == '__main__':
    Company()