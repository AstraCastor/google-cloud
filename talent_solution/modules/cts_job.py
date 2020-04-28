from google.cloud import talent_v4beta1
import os
import sys
import logging
import argparse
import inspect
import re
import json
from datetime import datetime
from modules import cts_db,cts_tenant,cts_company,cts_helper
from res import config as config
#from google.cloud.talent_v4beta1.types import Job as talent_job
from google.cloud.talent_v4beta1.proto.common_pb2 import CustomAttribute,RequestMetadata
from google.protobuf.timestamp_pb2 import Timestamp


#Get the root logger
logger = logging.getLogger()

class Job():
    def __init__(self,name=None,company_name=None,requisition_id=None,language=None):
        try:
            self.name = name
            self.company=company_name
            self.requisition_id=requisition_id
            self.language_code=language
            logging.debug("Job instantiated.")
        except Exception as e:
            logging.error("Error instantiating Job. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
    
    def client(self):
        logger.setLevel(logging.DEBUG)
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
                
                job_parent = client.tenant_path(project_id,tenant_id)
                if os.path.exists(os.path.dirname(file)):
                    logger.debug("{}: Reading input file from {}".format(inspect.currentframe().f_code.co_name,file))
                    batch_size = 5
                    batch_id=1
                    # job_req_metadata = {"userId":"test2","sessionId":"test2"}
                    job_req_metadata = RequestMetadata()
                    job_req_metadata.user_id='test2'
                    job_req_metadata.session_id='test2'
                    logger.debug("Request Metadata: {}".format(job_req_metadata))

                    def batch_result(operation):
                        result = operation.result()
                        print ("Batch {} done. Results: \n{}".format(batch_id,result))
                        return result

                    with open(file,'r') as f:
                        job_batch=[]
                        for line_no,line in enumerate(f,1):
                            print ("Reading line {} - current batch {} size: {} - Mod(line_no,batch_size) {}".format(line_no,batch_id, len(job_batch),line_no%batch_size))
                            # if line_no%batch_size==0:
                            print("Line {} is: \n{}".format(line_no, line))
                            # check if the job exists already

                            job_batch.append(json.loads(line))
                            #TODO:delete prints
                            print ("Outside Batch size: {}".format(len(job_batch)))
                            if (len(job_batch) == batch_size):
                                try:
                                    # job_batch.append(json.loads(line))
                                    print ("\n---------------\Batch {}--------------".format(batch_id))
                                    print ("Inside Batch size: {}".format(len(job_batch)))
                                    # logger.debug("Posting Jobs {}".format("\n\n".join(job_batch)))
                                    # Convert list to set to list to get the unique list
                                    company_ids=list(set([job['company'] for job in job_batch]))
                                    logger.debug("Looking up companies for the current job batch:\n{}".format(company_ids))
                                    if company_ids is not None and "" not in company_ids:
                                        company_client = cts_company.Company()
                                        companies = company_client.get_company(project_id=project_id,tenant_id=tenant_id,external_id=(",").join(company_ids),scope='limited')
                                        logger.debug("{}:get_company returned: {}".format(inspect.currentframe().f_code.co_name,[c.external_id for c in companies]))
                                    else:
                                        logger.error("Missing company ID in the input file between lines {} and {}.".format(line_no-batch_size,line_no))
                                        raise KeyError
                                    for job in job_batch:
                                        # print ("Job[company]:{}".format(job['company']))
                                        # print ("Company Name: {}".format([company.name for company in companies if job['company']==company.external_id][0]))
                                        job['company']=[company.name for company in companies if job['company']==company.external_id].pop()
                                        job['promotion_value']=int(job['promotion_value'])
                                        job['job_start_time']=Timestamp(seconds=int(job['job_start_time']))
                                        job['job_end_time']=Timestamp(seconds=int(job['job_end_time']))
                                        job['posting_publish_time']=Timestamp(seconds=int(job['posting_publish_time']))
                                        job['posting_expire_time']=Timestamp(seconds=int(job['posting_expire_time']))
                                        for key in job['custom_attributes']:
                                            job['custom_attributes'][key]=CustomAttribute(string_values=[job['custom_attributes'][key]['string_values'][0]],filterable=True)
                                            # print("Custom attribs are: \n{}".format(job['custom_attributes']))

                                    #TODO: delete this print
                                    # for job in job_batch:
                                        # print ("\n---------------\nJob:\n{}".format(job))
                                    parent = cts_helper.get_parent(project_id,tenant_id)
                                    logger.debug("Parent is set to {}".format(parent))
                                    logger.debug("Posting batch {}: Lines {} to {}".format(batch_id,line_no - batch_size+1,\
                                        line_no))
                                    # batch_req = client.batch_create_jobs(parent,job_batch,metadata=[job_req_metadata])
                                    # metadata ERROR: Error when creating jobs. Message: 'RequestMetadata' object is not iterable
                                    batch_req = client.batch_create_jobs(parent,job_batch)
                                    # logger.debug("Batch ID {} request: {}".format(batch_req.name))
                                    # logger.debug("Batch ID {} request metadata: {}".format(batch_req.metadata))
                                    while not batch_req.done:
                                        sleep(100)
                                    batch_resp = batch_req.add_done_callback(batch_result)                                        
                                    print ("Batch ID {} response: {}".format(batch_id,batch_resp))
                                    # print (result for result in results)

                                    batch_id += 1
                                    job_batch=[]
                                except KeyError:
                                    logger.error("Missing company field in the job object: Line {}: {}".format(line_no,line),\
                                        exc_info=config.LOGGING['traceback'])
                                    exit(1) 
                                except Exception as e:
                                    # logger.debug("Error when creating jobs. Request: {} \nMetadata {} \nMessage: {}".format(batch_req.name,batch_req.metadata,e))
                                    #AttributeError: 'Operation' object has no attribute 'name'
                                    logger.debug("Error caught is: {} {}".format(type(e),e))
                                    logger.debug("Error when creating jobs. Request Metadata {} \nMessage: {}".format(batch_req.metadata,e))
                                    exit(1)
                            #else:
                                # logger.debug ("Read line:\n {}".format(line))
                                # logger.debug("Read line type: {}".format(type(line)))
                                # logger.debug ("Read line JSON:\n {}".format(json.loads(line)))
                                # logger.debug("Read line JSON type: {}".format(type(json.loads(line))))
                                # read_job = talent_job().ParseFromString(json.loads(line))
                                # print ("Read Job: {}".format(read_job.name))
                                # job['custom_attributes']



        except Exception as e:
            if job_object:
                logger.error("{}:Error creating job:\n{}\nMessage:{}".format(inspect.currentframe().f_code.co_name,job_object,e),\
                    exc_info=config.LOGGING['traceback'])
            else:
                logger.error("{}:Error creating job from file: {}. Message: {}".format(inspect.currentframe().f_code.co_name,file,e),\
                    exc_info=config.LOGGING['traceback'])
            # self.delete_job(project_id,tenant_id,job_object.external_id,forced=True)
            raise
                   

    def update_job(self,tenant_id,project_id=None,job=None,file=None):
        print ("update job") if file is None else print ("batch update job")
    
    def delete_job(self,project_id,tenant_id=None,external_id=None,all=False,forced=False):
        print ("delete job") if all is False else print ("batch create job")

    def get_job(self,project_id,company_id,tenant_id=None,external_id=None,languages='en',status='OPEN',all=False,scope='full'):
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
                db.execute("SELECT distinct job_name,external_id,language_code,company_name FROM job where job_key in ({})".format(','.join('?'*len(job_keys))),job_keys)
                rows = db.fetchall()
                logger.debug("Job names retrieved: {}".format(rows))
                if rows == []:
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    if scope=='limited':
                        jobs = [Job(name=row[0],requisition_id=row[1],language_code=row[2],company=row[4]) for row in rows]
                        logger.debug("Returning Jobs count: {}".format(len(jobs)))
                    else:
                        jobs = [client.get_job(row[0]) for row in rows]
                        logger.debug("Returning Jobs count: {}".format(len(jobs)))
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
                    filter_['companyName'] = company[0].name
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
