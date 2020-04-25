from google.cloud import talent_v4beta1
import os
import sys
import logging
import argparse
import inspect
import re
import json
from datetime import datetime
from modules import cts_db,cts_tenant,cts_company
from res import config as config


#Get the root logger
logger = logging.getLogger()

class Job:
    def __init__(self,name=None,company_name=None,requisition_id=None,language=None):
        try:
            self.name = name
            self.company=company_name
            self.requisition_id=requisition_id
            self.language=language
            logging.debug("Job instantiated.")
        except Exception as e:
            logging.error("Error instantiating Job. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
    
    def client(self):
        credential_file = config.APP['secret_key']
        logger.debug("credentials: {}".format(credential_file))
        _job_client = talent_v4beta1.JobServiceClient.from_service_account_file(credential_file)
        logger.debug("Job client created: {}".format(_job_client))
        return _job_client
    
    def create_job(self,project_id,tenant_id=None,job_object=None,file=None):
        logger.debug("logger:CALLED: create_job({},{},{},{})".format(project_id,tenant_id,job_object,file))
        try:
            db = cts_db.DB().connection
            client = self.client()    
            if file is None:
                print ("create job")
            else:
                if os.path.exists(os.path.dirname(file)):
                    logger.debug("{}: Reading input file from {}".format(inspect.currentframe().f_code.co_name,file))
                    batch_size = 5
                    batch_id=1
                    with open(file,'r') as f:
                        job_batch=[]
                        for line_no,line in enumerate(f,1):
                            print ("Reading line {} - current batch: {} - Mod(line_no,batch_size) {}".format(line_no,len(job_batch),line_no%batch_size))
                            if line_no%batch_size==0:
                                try:
                                    # logger.debug("Posting Jobs {}".format("\n\n".join(job_batch)))
                                    # Convert list to set to list to get the unique list
                                    company_ids=list(set([job['company'] for job in job_batch]))
                                    logger.debug("Looking up companies for the current job batch:\n{}".format(company_ids))
                                    if company_ids is not None:
                                        company_client = cts_company.Company()
                                        companies = company_client.get_company(project_id=project_id,tenant_id=tenant_id,external_id=(",").join(company_ids),scope='limited')
                                        logger.debug("{}:get_company returned: {}".format(inspect.currentframe().f_code.co_name,[c.external_id for c in companies]))
                                    else:
                                        raise KeyError
                                    # for job in job_batch:
                                    #     job['company']=[company.name for company in companies if job['company']==company.external_id]
                                    job_batch=[]
                                except KeyError:
                                    logger.error("Missing company field in the job object: Line {}: {}".format(line_no,line),\
                                        exc_info=config.LOGGING['traceback'])
                                except Exception as e:
                                    logger.debug("Error when creating jobs. Message: {}".format(e))
                            else:
                                # logger.debug ("Read line:\n {}".format(line))
                                # logger.debug("Read line type: {}".format(type(line)))
                                job_batch.append(json.loads(line))
        except Exception as e:
            logger.error("{}:Error creating job:\n{}\n{}".format(inspect.currentframe().f_code.co_name,company_object,e),\
                exc_info=config.LOGGING['traceback'])
            self.delete_job(project_id,tenant_id,job_object['external_id'],forced=True)
            raise

                            

    def update_job(self,tenant_id,project_id=None,job=None,file=None):
        print ("update job") if file is None else print ("batch update job")
    
    def delete_job(self,project_id,tenant_id=None,external_id=None,all=False,forced=False):
        print ("delete job") if all is False else print ("batch create job")

    def get_job(self,project_id,company_id,tenant_id=None,external_id=None,languages='en',status='OPEN',all=False):
        """ Get CTS job by name or get all CTS jobs by project.
        Args:
            project_id: Project where the job will be created - string
            external_id: Unique ID of the job - string
            all: List all jobs. Mutually exclusive with external_id - Boolean.
        Returns:
            an instance of job or None if job was not found.
        """
        logger.debug("CALLED: get_job({},{},{},{},{},{} by {})".format(project_id,tenant_id,external_id,languages,status,all,\
            inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()    

            if project_id is not None:
                job_key = project_id
            else:
                logging.error("{}:Missing arguments: project_id.".format(inspect.currentframe().f_code.co_name),\
                    exc_info=config.LOGGING['traceback'])
                raise ValueError

            if tenant_id is not None:
                # Calculate the tenant_id part of the job_key to look up from the DB
                logger.debug("Tenant: {}\n".format(tenant_id))
                job_key = job_key + "-" + tenant_id
            
            if company_id is not None:
                # Calculate the company_id part of the job_key to look up from the DB
                job_key = job_key + "-" +company_id
            else:
                logging.error("{}:Missing arguments: company_id.".format(inspect.currentframe().f_code.co_name),\
                    exc_info=config.LOGGING['traceback'])
                raise ValueError                

            if external_id is not None:
                # Check if it is a SHOW operation but was pass a conflicting arg
                logger.debug("Req ID: {}\n".format(external_id))
                if all:
                    logging.error("{}:Conflicting arguments: --external_id and --all are mutually exclusive."\
                        .format(inspect.currentframe().f_code.co_name),exc_info=config.LOGGING['traceback'])
                    raise ValueError
                #Return english listing by default
                lang_list = [str(l) for l in languages.split(',')]
                job_keys = [job_key+"-"+external_id+"-"+language for language in lang_list]
                logger.debug("{}:Searching for job: {}".format(inspect.currentframe().f_code.co_name,job_key))
                db.execute("SELECT distinct job_name FROM job where job_key in ({})".format(','.join('?'*len(job_keys))),job_keys)
                rows = db.fetchall()
                logger.debug("Job names retrieved: {}".format(rows))
                if rows == []:
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    jobs = [client.get_job(row) for row in rows]
                    return jobs
            else:
                # LIST operation
                # Calculate the parent path : Get tenant name if tenant ID was provided, if not default tenant under the 
                # given project.
                if tenant_id is not None:
                    logger.debug("Tenant: {}\n".format(tenant_id))
                    tenant = cts_tenant.Tenant()
                    tenant_obj = tenant.get_tenant(project_id,tenant_id)
                    logger.debug("{}:Tenant retrieved:\n{}".format(inspect.currentframe().f_code.co_name,tenant_obj))
                    if tenant_obj is None:
                        logging.error("{}:Unknown Tenant: {}".format(inspect.currentframe().f_code.co_name,tenant_id),\
                            exc_info=config.LOGGING['traceback'])
                        exit(1)
                    parent = tenant_obj.name
                else:
                    parent = client.project_path(project_id)
                logger.debug("{}:Parent path: {}".format(inspect.currentframe().f_code.co_name,parent))
                # Look up company resource path for filtering
                company = cts_company.Company().get_company(project_id=project_id,tenant_id=tenant_id,external_id=company_id)
                logger.debug("{}:Company retrieved:\n{}".format(inspect.currentframe().f_code.co_name,company))
                filter_ = {}
                if company is None:
                    logging.error("{}:Unknown company: {}. Company is a mandatory attribute for a job listing, create the company \
                        before creating or looking up job listings.".format(inspect.currentframe().f_code.co_name,company_id))
                    exit(1)
                else:
                    filter_['companyName'] = company.name
                # Add status to the filter object
                filter_['status'] = status
                
                logger.debug ("Listing all jobs for {}, filtered by: \n{}".format(parent,filter_))
                jobs = [t for t in client.list_jobs(parent,json.dumps(filter_).encode('utf-8'))]
                return jobs

            # #Show operation
            # if not all:
            #     # Calculate the job_key to look up from the DB
            #     if company_id is not None:
            #         # Check if company name was passed in the attribute as well which would conflict with the command line option
            #         if filter_obj is not None: 
            #             if filter_obj['companyName'] is not None:
            #                 logging.exception("{}:Conflicting arguments: --company_id and companyName filter are mutually exclusive.".format(inspect.currentframe().f_code.co_name))
            #                 raise ValueError
            #         else: 
            #             filter_obj = {}
                
            #         # Check if the tenant is default tenant or a specific tenant
            #         if tenant_id is not None:
            #             # Check if it is list operation or a show operation
            #             logger.debug("Tenant: {}\n".format(tenant_id))
            #             if external_id is not None:
            #                 # Check if it is a show operation but was pass a conflicting arg
            #                 logger.debug("Req ID: {}\n".format(external_id))
            #                 if all:
            #                     logging.exception("{}:Conflicting arguments: --external_id and --all are mutually exclusive.".format(inspect.currentframe().f_code.co_name))
            #                     raise ValueError
            #                 job_key = project_id+"-"+tenant_id+"-"+company_id+"-"+external_id 
            #             else:
            #                 # No requisition ID was passed
            #                 job_key = project_id+"-"+tenant_id+"-"+company_id
            #         else:
            #             # No tenant ID was passed - default tenant
            #             job_key = project_id+"-"+company_id
                    
            #         logger.debug("{}:Searching for job: {}".format(inspect.currentframe().f_code.co_name,job_key))
            #         db.execute("SELECT job_name FROM job where job_key like '{}%'".format(job_key))
            #         rows = db.fetchall()
            #         logger.debug("Job name retrieved: {}".format(rows[0][0]))
            #         if rows == []:
            #             return None
            #         else:
            #             logger.debug("db lookup:{}".format(rows))
            #             job = client.get_job(rows[0][0])
            # #List operation (where all is True)
            # else:
            #     logger.debug("Listing jobs for:\nCompany: {}\n".format(company_id))
            #     #   if tenant_id is not None:
            #     if tenant_id is not None:
            #         logger.debug("Tenant: {}\n".format(tenant_id))
            #         tenant = cts_tenant.Tenant()
            #         tenant_obj = tenant.get_tenant(project_id,tenant_id)
            #         logger.debug("{}:Tenant retrieved:\n{}".format(inspect.currentframe().f_code.co_name,tenant_obj))
            #         if tenant_obj is None:
            #             logging.error("{}:Unknown Tenant: {}".format(inspect.currentframe().f_code.co_name,tenant_id))
            #             exit(1)
            #         parent = tenant_obj.name
            #     else:
            #         parent = client.project_path(project_id)
            #     logger.debug("{}:Parent path: {}".format(inspect.currentframe().f_code.co_name,parent))
            #     if company_id is not None:
            #         # Check if company name was passed in the attribute as well which would conflict with the command line option
            #         if filter_obj is not None: 
            #             if filter_obj['companyName'] is not None:
            #                 logging.exception("{}:Conflicting arguments: --company_id and companyName filter are mutually exclusive.".format(inspect.currentframe().f_code.co_name))
            #                 raise ValueError
            #         else: 
            #             filter_obj = {}
            #         company = cts_company.Company().get_company(project_id=project_id,tenant_id=tenant_id,external_id=company_id)
            #         logger.debug("{}:Company retrieved:\n{}".format(inspect.currentframe().f_code.co_name,company))
            #         if company is None:
            #             logging.error("{}:Unknown company: {}. Company is a mandatory attribute for a job listing, create the company \
            #                 before creating or looking up a job listing.".format(inspect.currentframe().f_code.co_name,company_id))
            #             exit(1)
            #         else:
            #             filter_obj['companyName'] = company.name
                
            #     logger.debug ("Listing all jobs for {}, filtered by: \n{}".format(parent,filter_obj))
            #     filter_ = json.dumps(filter_obj).encode('utf-8')
            #     job = [t for t in client.list_jobs(parent,filter_)]
 
            # return job
        except Exception as e:
            if external_id is not None:
                logger.error("{}:Error getting job by name {}. Message: {}".format(inspect.currentframe().f_code.co_name,\
                    external_id,e),exc_info=config.LOGGING['traceback'])
            else:
                logger.error("{}:Error getting job for project {} company {}. Message: {}".format(inspect.currentframe().f_code.co_name,
                project_id,company_id,e),exc_info=config.LOGGING['traceback'])
            raise 
