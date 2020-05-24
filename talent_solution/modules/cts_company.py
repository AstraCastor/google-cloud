from google.cloud import talent_v4beta1
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError, RetryError
from google.cloud.talent_v4beta1.types import Company as CTS_Company

import os
import sys
import logging
import argparse
import inspect
import re
import json
from datetime import datetime
from modules import cts_db,cts_tenant,cts_helper
from modules.cts_errors import UnknownCompanyError
from modules.cts_helper import get_parent
from conf import config as config

#Get the root logger
logger = logging.getLogger()

class Company:
    
    def client(self):
        credential_file = config.APP['secret_key']
        logger.debug("credentials: {}".format(credential_file))
        _company_client = talent_v4beta1.CompanyServiceClient.from_service_account_file(credential_file)
        logger.debug("Company client created: {}".format(_company_client))
        return _company_client
    

    def create_company(self,project_id,tenant_id=None,company=None,file=None):
        logger.debug("CALLED: create_company({},{},{},{} by {})".format(project_id,tenant_id,company,file,inspect.currentframe().f_back.f_code.co_name))
        try:
            client = self.client()
            if not file:
                companies = [company]
            else:
                companies = cts_helper.generate_file_batch(file=file,rows=1)
            company_count = 0
            company_errors = []
            for c in companies:
                try:
                    if file:
                        line,company_batch_item = c[0].popitem()
                        company_object = company_batch_item.pop()
                    else:
                        company_object = c
                    if isinstance(company_object,str):
                        company_object = json.loads(company_object)

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
                                logging.error("Unknown Tenant: {}".format(tenant_id),exc_info=config.LOGGING['traceback'])
                                exit(1)
                            parent = tenant_obj.name
                            tenant_name = tenant_obj.name
                        else:
                            tenant_name = None
                            parent = client.project_path(project_id)
                        logger.debug("Parent path set to: {}".format(parent))
                        new_company = client.create_company(parent,company_object)
                        if cts_db.persist_to_db(new_company,project_id=project_id,tenant_id=tenant_id):
                            logger.info("Company {} created.\n{}".format(external_id,new_company))
                            print("Company {} created.\n{}".format(external_id,new_company))
                            # return new_company
                        else:
                            raise("Error when persisting company {} to DB.".format(new_company.external_id))
                        company_count += 1
                    else:
                        logger.warning("Company {} already exists.\n{}".format(external_id,existing_company))
                        # return None

                except AlreadyExists as e:                    
                    logger.warning("Company {} exists in server. Creating local record..".format(company_object))
                    # Sync with DB if it doesn't exist in DB
                    logger.warning("Local DB out of sync. Syncing local db..")
                    sync_company = CTS_Company()
                    sync_company.name = re.search("^Company (.*) already exists.*$",e.message).group(1)
                    sync_company.external_id = company_object['external_id']
                    if cts_db.persist_to_db(sync_company,project_id=project_id,tenant_id=tenant_id):
                        logger.warning("Company {} record synced to DB.".format(external_id))                        
                    else:
                        raise Exception("Error when syncing company {} to DB.".format(sync_company.external_id))
                except Exception as e:
                     company_errors.append(e)
            logger.debug("Total companies created: {}".format(company_count))
            print("Total companies created: {}".format(company_count))
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
            logger.error("{}:Error creating company:\n{}\n{}".format(inspect.currentframe().f_code.co_name,company_object,e),\
                exc_info=config.LOGGING['traceback'])
            self.delete_company(project_id,tenant_id,company_object['external_id'],force=True)
            raise

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
                logger.debug("{}:Calling get_company({},{},{})".format(inspect.currentframe().f_code.co_name,project_id,\
                    tenant_id,external_id))
                existing_company = self.get_company(project_id=project_id,tenant_id=tenant_id,external_id=external_id)
                logger.debug("{}:Existing company? {}".format(inspect.currentframe().f_code.co_name,existing_company))
            if existing_company is not None:
                logger.info("Deleting company id: {}".format(existing_company[0].external_id))
                client.delete_company(existing_company[0].name)
                db.execute("DELETE FROM company where company_name = ?",(existing_company[0].name,))
                logger.info("Company {} deleted.".format(external_id))
                print("Company {} deleted.".format(external_id))

            else:
                logger.error("{}: Company {} does not exist.".format(inspect.currentframe().f_code.co_name,external_id),\
                    exc_info=config.LOGGING['traceback'])
                print("Company {} does not exist.".format(external_id))
                return None
        except Exception as e:
            logger.error("{}:Error deleting company {}: {}".format(inspect.currentframe().f_code.co_name,external_id,e),\
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
                    logging.error("{}:Conflicting arguments: --external_id and --all are mutually exclusive."\
                        .format(inspect.currentframe().f_code.co_name),exc_info=config.LOGGING['traceback'])
                    raise ValueError
                # List of company external_ids provided as args
                company_ids = external_id.split(",")
                # Build the DB lookup key for the company table
                company_keys=[project_id+"-"+tenant_id+"-"+company_id if project_id is not None and tenant_id is not None \
                else project_id+"-"+company_id for company_id in company_ids]
                logger.debug("{}:Searching for company: {}".format(inspect.currentframe().f_code.co_name,company_keys))
                # Lookup companies in the DB
                db.execute("SELECT distinct external_id,company_name FROM company where company_key in ({})"\
                    .format(",".join("?"*len(company_keys))),company_keys)
                rows = db.fetchall()
                if rows == []:
                    print("Company ID(s) not found: {}".format(external_id))
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
                    raise UnknownCompanyError("Missing or unknown company ID(s): {}".format(set(company_ids) - set(lookedup_ids)))

            # It's a list all operation
            elif all:
                if tenant_id is not None:
                    tenant = cts_tenant.Tenant()
                    tenant_obj = tenant.get_tenant(project_id,tenant_id,scope='limited')
                    logger.debug("{}:Tenant retrieved:\n{}".format(inspect.currentframe().f_code.co_name,tenant_obj))
                    if tenant_obj is None:
                        logger.error("{}:Unknown Tenant: {}".format(inspect.currentframe().f_code.co_name,tenant_id),\
                            exc_info=config.LOGGING['traceback'])
                        exit(1)
                    parent = tenant_obj.name
                else:
                    parent = client.project_path(project_id)
                logger.debug("{}:Parent path: {}".format(inspect.currentframe().f_code.co_name,parent))
                lookedup_companies = [t for t in client.list_companies(parent)]
                logger.debug("{}:Company list operation: {} companies returned".format(inspect.currentframe().f_code.co_name,\
                    len(lookedup_companies)))
            else:
                logger.error("Invalid arguments.",exc_info=config.LOGGING['traceback'])
                raise AttributeError
            return lookedup_companies
        except Exception as e:
            logger.error("Error getting company by name {}. Message: {}".format(external_id,e),exc_info=config.LOGGING['traceback'])
            raise 

if __name__ == '__main__':
    Company()