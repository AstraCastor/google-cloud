from google.cloud import talent_v4beta1
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError, RetryError
from google.cloud.talent_v4beta1.types import Company as CTS_Company

import os
import sys
import logging
import inspect
import re
import json
import concurrent.futures

from modules import cts_db,cts_tenant,cts_helper
from modules.cts_errors import UnknownCompanyError
from modules.cts_helper import get_parent
from conf import config as config

#Get the root logger
logger = logging.getLogger()

class Company:
    
    def client(self):
        try:
            if 'secret_key' in config.APP:
                if os.path.exists(config.APP['secret_key']):
                    _company_client = talent_v4beta1.CompanyServiceClient\
                        .from_service_account_file(config.APP['secret_key'])
                    logger.debug("credentials: {}".format(config.APP['secret_key']))
                else:
                    raise Exception("Missing credential file.")
            else:
                _company_client = talent_v4beta1.CompanyServiceClient()
            logger.debug("Company client created: {}".format(_company_client))
            return _company_client

        except Exception as e:
            logging.error("Error instantiating Company client. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
            exit(1)

    def create_company(self,project_id,tenant_id=None,company=None,file=None):
        logger.debug("CALLED: create_company({},{},{},{} by {})".format(project_id,tenant_id,company,file,inspect.currentframe().f_back.f_code.co_name))
        try:
            def task_create_company(line,company_object):
                try:
                    external_id = company_object['external_id']
                    # Check if it is an existing company
                    logger.debug("Calling get_company({},{})".format(project_id,external_id))
                    existing_company = self.get_company(project_id=project_id,tenant_id=tenant_id,external_id=external_id,scope='limited')
                    logger.debug("get_company returned:{}".format(existing_company))
                    if existing_company is None:
                        if tenant_id is not None:
                            # To set the parent of the company to be created to the tenant_name 
                            # for the given tenant_id(tenant external_id)
                            tenant = cts_tenant.Tenant()
                            tenant_obj = tenant.get_tenant(project_id,tenant_id,scope='limited')
                            logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
                            if tenant_obj is None:
                                logging.error("Unknown Tenant: {}".format(tenant_id))
                                exit(1)
                            parent = tenant_obj.name
                            tenant_name = tenant_obj.name
                        else:
                            tenant_name = None
                            parent = client.project_path(project_id)
                        logger.debug("Parent path set to: {}".format(parent))
                        new_company = client.create_company(parent,company_object)
                        if cts_db.persist_to_db(new_company,project_id=project_id,tenant_id=tenant_id):
                            new_companies.append(new_company)
                            logger.info("Line {}: company {} created.{}".format(line,external_id,new_company))
                            if file:
                                print("Line {}: company {} created.".format(line,external_id))
                                return True
                            else:
                                return new_company
                        else:
                            raise("Line {}: Error when persisting company {} to DB.".format(line,new_company.external_id))
                    else:
                        logger.warning("Line {}: company {} already exists.\n{}".format(line,external_id,existing_company))
                        print("Line {}: company {} already exists.\n{}".format(line,external_id,existing_company))
                except AlreadyExists as e:                    
                    logger.warning("Line{}: company {} exists in server. Creating local record..".format(line,company_object))
                    print("Line{}: company {} exists in server. Creating local record..".format(line,company_object))
                    # Sync with DB if it doesn't exist in DB
                    logger.warning("Local DB out of sync. Syncing local db..")
                    print("Local DB out of sync. Syncing local db..")
                    sync_company = CTS_Company()
                    sync_company.name = re.search("^Company (.*) already exists.*$",e.message).group(1)
                    sync_company.external_id = company_object['external_id']
                    if cts_db.persist_to_db(sync_company,project_id=project_id,tenant_id=tenant_id):
                        logger.warning("Line {}: company {} record synced to DB.".format(line,external_id))                        
                        print("Line {}: company {} record synced to DB.".format(line,external_id))   
                        return True                     
                    else:
                        raise Exception("Line {}: Error when syncing company {} to DB.".format(line,sync_company.external_id))
                        return False

            client = self.client()
            if not file:
                company_batches = [{1:{1:company}}]
            else:
                # company_batches = cts_helper.generate_file_batch(file=file,rows=1)
                company_batches = cts_helper.generate_file_batch(file=file,rows=config.BATCH_PROCESS['batch_size'],concurrent_batches=config.BATCH_PROCESS['concurrent_batches'])
            company_errors = []
            new_companies = []
            create_company_tasks = {}
            for c_batches in company_batches:
                for batch_id,batch in c_batches.items():
                    try:
                        with concurrent.futures.ThreadPoolExecutor(max_workers = config.BATCH_PROCESS['concurrent_batches']) as executor:
                            # batch_id,batch = c_batch.popitem()
                            for line,company_object in batch.items():
                                if isinstance(company_object,str):
                                    company_object = json.loads(company_object)
                                logger.info("Creating company from line {}...".format(line))
                                create_company_tasks.update({executor.submit(task_create_company,line,company_object):{"external_id":company_object['external_id'],"line":line}})
                            for task in concurrent.futures.as_completed(create_company_tasks):
                                task_item = create_company_tasks.pop(task)
                                result = task.result()
                                if not result:
                                    logger.debug("Line {}: company {} not created.".format(task_item['line'],task_item['external_id']))
                                    print("Line {}: company {} not created.".format(task_item['line'],task_item['external_id']))
                                elif isinstance(result,CTS_Company):
                                        return result
                    except Exception as e:
                        logger.error("Company creation failed due to {}.".format(e),config.LOGGING['traceback'])
                        company_errors.append(e)
            if company_errors:
                raise Exception(company_errors)
        except ValueError:
            logger.error("Invalid Input Parameters.")
            raise
        except TypeError:
            logger.error("One or multiple input fields have wrong or invalid data type.")
            raise
        except GoogleAPICallError as e:
            logger.error("API error when creating job. Message: {}".format(e))
            raise
        except Exception as e:
            logger.error("Error creating company:\n{}\n{}".format(company_object or "NA",e),\
                exc_info=config.LOGGING['traceback'])
            raise
        finally:
            logger.debug("Total companies created: {}".format(len(new_companies)))
            if file:
                print("Total companies created: {}".format(len(new_companies)))
            return new_companies if new_companies else None

    def delete_company(self,project_id,tenant_id=None,external_id=None,all=False,force=False):
        """ Delete a CTS company by external name.
        Args:
            project_id: project where the company will be created - string
            external_id: unique ID of the company - string
        Returns:
            None - If company is not found.
        """
        logger.debug("CALLED: delete_company({},{},{},{},{} by {})".format(project_id,tenant_id,external_id,all,force,\
            inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()  
            if force:
                # Get all the companies from the server directly - get_company(all,scope=full) gets everything from the server directly
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
                logger.debug("Calling get_company({},{},{})".format(project_id,tenant_id,external_id))
                existing_companies = self.get_company(project_id=project_id,tenant_id=tenant_id,external_id=external_id,all=all)
                # logger.debug("Existing company? {}".format(existing_company))
            if existing_companies:
                for existing_company in existing_companies:
                    try:
                        logger.info("Deleting company id: {}".format(existing_company.external_id))
                        client.delete_company(existing_company.name)
                        db.execute("DELETE FROM company where company_name = ?",(existing_company.name,))
                        logger.info("Company {} deleted.".format(existing_company.external_id))
                        print("Company {} deleted.".format(existing_company.external_id))
                    except GoogleAPICallError as e:
                        logger.error("API error when deleting job. Message: {}".format(e))
                        raise
                    except Exception as e:
                        logger.error("Error deleting company:\n{}\n{}".format(existing_company,e),\
                            exc_info=config.LOGGING['traceback'])
                exit(0)              
            else:
                logger.warn("Company {} does not exist.".format(external_id))
                print("Company {} does not exist.".format(external_id))
                return None
        except Exception as e:
            logger.error("Error deleting company {}: {}".format(external_id,e),\
                exc_info=config.LOGGING['traceback'])
            raise

    def update_company(self,tenant_id,project_id=None,company=None,file=None):
        pass

    def get_company(self,project_id,tenant_id=None,external_id=None,all=False,scope='full'):
        """ Get CTS company by name or get all CTS companies by project.
        Args:
            project_id: Project where the company will be created - string
            external_id: Unique ID of the company - string
            all: List all companies. Mutually exclusive with external_id - Boolean.
        Returns:
            an instance of company or None if company was not found.
        """
        logger.debug("CALLED: get_company({},{},{},{} by function {})".format(project_id,tenant_id,external_id,all,\
            inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()  
            if external_id is not None:
                if all:
                    logging.error("Conflicting arguments: --external_id and --all are mutually exclusive."\
                        ,exc_info=config.LOGGING['traceback'])
                    raise ValueError
                # List of company external_ids provided as args
                company_ids = external_id.split(",")
                # Build the DB lookup key for the company table
                company_keys=[project_id+"-"+tenant_id+"-"+company_id if project_id is not None and tenant_id is not None \
                else project_id+"-"+company_id for company_id in company_ids]
                logger.debug("Searching for company: {}".format(company_keys))
                # Lookup companies in the DB
                db.execute("SELECT distinct external_id,company_name FROM company where company_key in ({})"\
                    .format(",".join("?"*len(company_keys))),company_keys)
                rows = db.fetchall()
                if rows == []:
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    lookedup_companies = []
                    if scope == 'limited':
                        #Return limited data looked up from the DB
                        for row in rows:
                            limited_company = CTS_Company()
                            limited_company.name = row[1]
                            limited_company.external_id = row[0]
                            lookedup_companies.append(limited_company)
                        logger.debug("Company show operation - scope limited: {}".format(lookedup_companies))
                    else:
                        #Return limited data looked up from the server
                        lookedup_companies = [client.get_company(row[1]) for row in rows]
                        logger.debug("Company show operation - scope full: {}".format(lookedup_companies))
                if len(company_ids)!=len(lookedup_companies):
                    lookedup_ids = [lc.external_id for lc in lookedup_companies]
                    logger.warning("Missing or unknown company ID(s): {}".format(set(company_ids) - set(lookedup_ids)))
                    print("Missing or unknown company ID(s): {}".format(set(company_ids) - set(lookedup_ids)))

            # It's a list all operation
            elif all:
                if scope == 'full':
                    # Listing directly from the server
                    if tenant_id is not None:
                        tenant = cts_tenant.Tenant()
                        tenant_obj = tenant.get_tenant(project_id,tenant_id,scope='limited')
                        logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
                        if tenant_obj is None:
                            logger.error("Unknown Tenant: {}".format(tenant_id))
                            exit(1)
                        parent = tenant_obj.name
                    else:
                        parent = client.project_path(project_id)
                    logger.debug("Parent path: {}".format(parent))
                    print("Getting list of companies for {} tenant. This could take a while...".format(tenant_id or 'DEFAULT'))
                    lookedup_companies = [t for t in client.list_companies(parent)]
                    logger.debug("Company list operation: {} companies returned".format(len(lookedup_companies)))
                else:
                    # Lookup companies in the DB and return a limited version of Company objects with just the IDs
                    db.execute("SELECT distinct external_id,company_name FROM company where company_key like (?)",\
                        (project_id+"-"+(tenant_id or "")+"%",))
                    rows = db.fetchall()
                    if rows == []:
                        return None
                    else:
                        logger.debug("Companies in db:{}".format(len(rows)))
                        lookedup_companies = []
                        if scope == 'limited':
                            #Return limited data looked up from the DB
                            for row in rows:
                                limited_company = CTS_Company()
                                limited_company.name = row[1]
                                limited_company.external_id = row[0]
                                lookedup_companies.append(limited_company)
                            logger.debug("Company show operation - scope limited: {}".format(lookedup_companies))                 
            else:
                logger.error("Invalid arguments.",exc_info=config.LOGGING['traceback'])
                raise AttributeError
            return lookedup_companies
        except Exception as e:
            logger.error("Error getting company by name {}. Message: {}".format(external_id,e),exc_info=config.LOGGING['traceback'])
            raise 

    def sync_company(self,project_id,tenant_id=None,external_id=None):
        """ 
        Sync a company or all companies from the server to the client.
        """
        logger.debug("CALLED: sync_job({},{},{} by {})".format(project_id,tenant_id,external_id,\
            inspect.currentframe().f_back.f_code.co_name))
        try:
            client = self.client()    

            if project_id is not None:
                job_key = project_id
            else:
                logging.error("Missing arguments: project_id.",exc_info=config.LOGGING['traceback'])
                raise ValueError

            # Look up parent resource path for listing
            if tenant_id:
                tenant = cts_tenant.Tenant()
                tenant_obj = tenant.get_tenant(project_id,tenant_id,scope='limited')
                logger.debug("Tenant set to:\n {}".format(tenant_obj))
                if tenant_obj is None:
                    logger.error("Unknown tenant: {}".format(tenant_id))
                    exit(1)
                parent = tenant_obj.name
            else:
                parent = client.project_path(project_id)
            logger.debug("Parent path: {}".format(parent))

            all_companies = self.get_company(project_id=project_id,tenant_id=tenant_id,all=True,scope='full')
            if all_companies:
                # Look up company resource path for filtering
                if external_id:
                    logger.info("Syncing {} in the {} tenant to local...".format(external_id, tenant_id or "default"))
                    print("Syncing {} in the {} tenant to local...".format(external_id, tenant_id or "default"))
                    all_companies = [company for company in all_companies if external_id == company.external_id]
                else:
                    # this will be looping over all companies now
                    logger.info("Syncing all companies in the {} tenant to local...".format(tenant_id or "default"))
                    print("Sync all companies in the {} tenant to local...".format(tenant_id or "default"))

                if len(all_companies) < 10:
                    logger.debug("Companies retrieved:\n {}".format(all_companies))
                    print([company.external_id for company in all_companies])
                else:
                    logger.debug("Companies retrieved:\n {}".format(len(all_companies)))
                    print(len(all_companies))

                existing_companies = self.get_company(project_id=project_id,tenant_id=tenant_id,all=True,scope='limited')
                existing_company_names = [company.name for company in existing_companies] if existing_companies else []
                companies_to_sync = [company for company in all_companies if company.name not in existing_company_names] \
                    if existing_companies else all_companies
                for company in companies_to_sync:
                    if cts_db.persist_to_db(object=company,project_id=project_id,tenant_id=tenant_id):
                        logger.info("Synced company {} to local.".format(company.external_id))
                        print("Synced company {} to local.".format(company.external_id))
                    else:
                        logger.warning("Synced company {} to local failed.".format(company.external_id))
                        print("Synced company {} to local failed.".format(company.external_id))
                if len(companies_to_sync) > 0:
                    logger.info("Total synced companies: {}".format(len(companies_to_sync)))
                    print("Total synced companies: {}.".format(len(companies_to_sync)))
                else:
                    logger.info("Nothing to sync.")
                    print("Nothing to sync.")                    
                
                return True
            else:
                logger.warning("No companies found in the {} tenant.".format(tenant_id))
                print("No companies found in the {} tenant.".format(tenant_id))

        except Exception as e: 
            logger.error("Error syncing {}{}. Message: {}".format("company" if external_id else "",\
                " " + external_id or "all companies.",e),exc_info=config.LOGGING['traceback'])
            raise    

if __name__ == '__main__':
    Company()