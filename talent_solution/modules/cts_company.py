from google.cloud import talent_v4beta1
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError, RetryError

import os
import sys
import logging
import argparse
import inspect
import re
import json
from datetime import datetime
from modules import cts_db,cts_tenant
from modules.cts_errors import UnknownCompanyError
from modules.cts_helper import get_parent
from res import config as config

#Get the root logger
logger = logging.getLogger()

class Company:
    def __init__(self,external_id=None,name=None):
        try:
            self.external_id = external_id
            self.name = name
            logging.debug("Company instantiated.")
        except Exception as e:
            logging.error("Error instantiating Company. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
    
    def client(self):
        credential_file = config.APP['secret_key']
        logger.debug("credentials: {}".format(credential_file))
        _company_client = talent_v4beta1.CompanyServiceClient.from_service_account_file(credential_file)
        logger.debug("Company client created: {}".format(_company_client))
        return _company_client
    

    def create_company(self,project_id,tenant_id=None,company_object=None,file=None):
        logger.debug("CALLED: create_company({},{},{},{} by {})".format(project_id,tenant_id,company_object,file,inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()       
            logger.debug("Company input: Type {}\n{}".format(type(company_object),company_object))
            if isinstance(company_object,dict):
                external_id = company_object['external_id']
                # Check if it is an existing company
                logger.debug("{}:Calling get_company({},{})".format(inspect.currentframe().f_code.co_name,project_id,external_id))
                existing_company = self.get_company(project_id=project_id,tenant_id=tenant_id,external_id=external_id)
                logger.debug("{}:get_company returned:{}".format(inspect.currentframe().f_code.co_name,existing_company))
                if existing_company is None:
                    if tenant_id is not None:
                        # To set the parent of the company to be created to the tenant_name 
                        # for the given tenant_id(tenant external_id)
                        tenant = cts_tenant.Tenant()
                        tenant_obj = tenant.get_tenant(project_id,tenant_id)
                        logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
                        if tenant_obj is None:
                            logging.error("Unknown Tenant: {}".format(tenant_id),exc_info=config.LOGGING['traceback'])
                            exit(1)
                        parent = tenant_obj.name
                        tenant_name = tenant_obj.name
                    else:
                        tenant_name = None
                        # tenant_id = ""
                        parent = client.project_path(project_id)
                    logger.debug("{}:Parent path set to: {}".format(inspect.currentframe().f_code.co_name,parent))
                    try:
                        new_company ={}
                        new_company = client.create_company(parent,company_object)
                    except AlreadyExists as e:                    
                        logger.debug("Company {} exists in server. (TODO)Creating local record..".format(company_object))
                        #TODO: Better error handling
                        # print("Error Message:{}".format(e.message))
                        # company_name_re = re.compile('^Company (.*?) (:?.*)')
                        # regx_search = company_name_re.findall(e.message)
                        # print("REGEX OUT {}".format(regx_search))
                        # print ("REGEX TYPE: {}".format(type(regx_search)))
                        # company_name = regx_search
                        # print("REGEX[0][0]".format(str(regx_search[0][0])))
                        # print ("Company Name:".format(regx_search[0][0]))
                        # print ("Company Name:".format(regx_search[0][1]))
                        # print ("Company Name:".format(regx_search[0]))

                        # print ("Company Name : {}".format(company_name))
                        # print ("Company Name : {}".format(company_name[0]))
                        # print ("Company Name : {}".format(company_name[0][0]))
                        # print ("Company Name : {}".format(company_name[0][1]))

                        # new_company['external_id']=company_object['external_id']
                        # new_company['company_name'] = company_name[0][0]
                        # print ("Type of New company {}".format(type(new_company)))
                        # print ("New company length {}".format(len(new_company)))
                        # for k,v in new_company.items():
                        #        print ("New Company items: {} {}".format(k,v)) 
                        # print("New company after error:".format(new_company))
                        pass
                    except Exception as e:
                        raise e

                    company_key = project_id+"-"+tenant_id+"-"+external_id if tenant_id is not None else project_id+"-"+external_id
                    logger.debug("Inserting record for company key:{}".format(company_key))
                    logger.debug("Query: INSERT INTO company (company_key,external_id,company_name,tenant_name,project_id,suspended,create_time)    \
                        VALUES ('{}','{}','{}','{}','{}','{:d}','{}')".format(company_key,new_company.external_id,    \
                            new_company.name,tenant_name,project_id,0,datetime.now()))
                    db.execute("INSERT INTO company (company_key,external_id,company_name,tenant_name,project_id,suspended,create_time) \
                        VALUES (?,?,?,?,?,?,?)",\
                        (company_key,new_company.external_id,new_company.name,tenant_name,project_id,0,datetime.now()))
                    logger.info("Company {} created.\n{}".format(external_id,new_company))
                    print("Company {} created.\n{}".format(external_id,new_company))
                    return new_company
                else:
                    logger.error("{}: Company {} already exists.\n{}".format(inspect.currentframe().f_code.co_name,external_id,\
                        existing_company),exc_info=config.LOGGING['traceback'])
                    return None
            else:
                logger.error("{}:Invalid or missing company argument. Should be a valid company object.\n {}"\
                    .format(inspect.currentframe().f_code.co_name,company_object),exc_info=config.LOGGING['traceback'])
                raise ValueError
        except Exception as e:
            logger.error("{}:Error creating company:\n{}\n{}".format(inspect.currentframe().f_code.co_name,company_object,e),\
                exc_info=config.LOGGING['traceback'])
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
        logger.debug("CALLED: delete_company({},{},{},{},{} by {})".format(project_id,tenant_id,external_id,all,forced,\
            inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()  
            if forced:
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
                    print("Unknown company ID(s): {}".format(external_id))
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    if scope == 'limited':
                        #Return limited data looked up from the DB
                        lookedup_companies = [Company(row[0],row[1]) for row in rows]
                        logger.debug("Company show operation - scope limited: {}".format(lookedup_companies))
                    else:
                        #Return limited data looked up from the server
                        lookedup_companies = [client.get_company(row[1]) for row in rows]
                        logger.debug("Company show operation - scope full: {}".format(lookedup_companies))
                if len(company_ids)!=len(lookedup_companies):
                    lookedup_ids = [lc['external_id'] for lc in lookedup_companies]
                    raise UnknownCompanyError("Missing or unknown company ID(s): {}".format(company_ids - lookedup_ids))

            # It's a list all operation
            elif all:
                if tenant_id is not None:
                    tenant = cts_tenant.Tenant()
                    tenant_obj = tenant.get_tenant(project_id,tenant_id)
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