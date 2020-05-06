from google.cloud import talent_v4beta1
import os
import sys
import logging
import argparse
import inspect
import re
import json
from datetime import datetime
import time
from modules import cts_db,cts_tenant,cts_company,cts_helper
from res import config as config
#from google.cloud.talent_v4beta1.types import Job as talent_job
from google.cloud.talent_v4beta1.proto.common_pb2 import RequestMetadata
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError, RetryError
from modules.cts_errors import UnparseableJobError

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

    def create_job(self,project_id,tenant_id=None,input_job=None,file=None):
        logger.debug("logger:CALLED: create_job({},{},{},{})".format(project_id,tenant_id,input_job,file))
        try:
            db = cts_db.DB().connection
            client = self.client()
            #Prepare Request Metadata
            # TODO:Replace with config 
            job_req_metadata = {}
            job_req_metadata['user_id']='test2'
            job_req_metadata['session_id']='test2'            
            logger.debug("Setting request Metadata to {}".format(job_req_metadata))

            # Create a single job. If no file arg is provided a job string is required as input.
            if file is None:
                try:
                    logger.debug("create_job: Parsing job string: {}".format(input_job))
                    # job_json = json.loads(input_job)
                    # company_id = job_json['company']
                    company_id = input_job['company']
                    parsed_job = cts_helper.parse_job(project_id=project_id,tenant_id=tenant_id,jobs=[input_job])
                    if parsed_job is None:
                        raise UnparseableJobError
                    parent = cts_helper.get_parent(project_id,tenant_id)
                    logger.debug("create_job: Parent is set to {}".format(parent))

                except UnparseableJobError as e:
                    logger.error("Unable to parse the input job string.")
                else:
                    try:
                        logger.debug("Posting parsed job {}".format(parsed_job))
                        new_job = client.create_job(parent,parsed_job[0],metadata=[job_req_metadata])
                        if new_job is not None:
                            if cts_helper.persist_to_db(object=new_job,project_id=project_id,tenant_id=tenant_id,company_id=company_id):
                                # print ("Created job requisition ID {} for company {}.".format(job_json.requisition_id,company_id))
                                print ("Created job requisition ID {} for company {}.".format(input_job.requisition_id,company_id))
                        else:
                            raise
                    except AlreadyExists as e:
                        #TODO: Sync with DB if it doesn't exist in DB
                        logger.error("Job already exists. Message: {}".format(e))
                    except ValueError:
                        logger.error("Invalid Parameters")
                    except GoogleAPICallError as e:
                        logger.error("API error when creating job. Message: {}".format(e))
                    except Exception as e:
                        logger.error("Error when creating job. Message: {}".format(e))

            # Create a batch of jobs. Job strings are provided in a new line delimited JSON file as input.
            else:                
                job_parent = client.tenant_path(project_id,tenant_id)
                if os.path.exists(file):
                    logger.debug("Reading input file from {}".format(file))
                    #TODO: Replace with a config param
                    batch_size = 5
                    # batch_id=1
                    logger.debug("Batching the file {} to be posted...".format(file))

                    def operation_complete(operation_future):
                        try:
                            # print("Size of operation after callback: {}".format(sys.getsizeof(operation)))
                            if operation_future in batch_ops.values(): 
                                print ("operation_future in batch_ops.values() is True")
                                print ("Metadata is {}".format(operation_future.metadata))
                                print ("State is {}".format(operation_future.metadata.state))
                                print ("Op name is: {}".format(operation_future.operation.name))
                                # if operation_future.metadata.state==3:
                                if operation_future.done and operation_future.error is None:
                                    print ("operation_future.metadata.state==3 (SUCCEEDED) is True")
                                    op_result = operation_future.result()
                                    for id,op in batch_ops.items():
                                        if op == operation_future:
                                            batch_id = id 
                                    logger.debug("Batch ID {} results:\n{}".format(batch_id,op_result))
                            else:
                                raise AttributeError 
                        except AttributeError as e:
                            logger.error("Unknown Batch Operation")   
                        except Exception as e:
                            logger.error("Error when creating jobs. Message: {}".format(e),exc_info=config.LOGGING['traceback'])

                    # Generate the batches to be posted
                    batch_ops = {}
                    for concurrent_batch in cts_helper.generate_file_batch(file=file,rows=batch_size,concurrent_batches=1):
                        for batch in concurrent_batch:
                            try:
                                batch_id,jobs = batch.popitem()
                                logger.debug("Batch {}: Parsing {} jobs".format(batch_id,len(jobs)))
                                parsed_jobs = cts_helper.parse_job(project_id=project_id,tenant_id=tenant_id,jobs=jobs)
                                if parsed_jobs is None:
                                    raise UnparseableJobError
                                parent = cts_helper.get_parent(project_id,tenant_id)
                                logger.debug("Batch {}: Parent is set to {}".format(batch_id,parent))

                            except UnparseableJobError as e:
                                logger.warning("Batch {}: Unable to parse one or more jobs between lines {} and {}".format(batch_id,\
                                    ((batch_id-1)*batch_size+1),((batch_id-1)*batch_size)+batch_size))
                            else:
                                try:
                                    logger.debug("Batch {}: Posting lines {} to {}".format(batch_id,((batch_id-1)*batch_size+1),\
                                        ((batch_id-1)*batch_size)+batch_size))
                                    # batch_req = client.batch_create_jobs(parent,parsed_jobs,metadata=[job_req_metadata])
                                    batch_ops[batch_id]= client.batch_create_jobs(parent,parsed_jobs,metadata=[job_req_metadata])
                                    print ("Operation outside name is: {}".format(batch_ops[batch_id].operation.name))
                                    batch_ops[batch_id].add_done_callback(operation_complete)
                                    # logger.debug("---------------- Batch {} ({} Jobs) --------------".format(batch_id,len(parsed_jobs)))
                                except RetryError as e:
                                    logger.error("Batch {}: API Retry failed due to {}.".format(batch_id,e),\
                                        exc_info=config.LOGGING['traceback'])
                                except GoogleAPICallError as e:
                                    logger.error("Batch {}: CTS API Error due to {}".format(batch_id,e),\
                                        exc_info=config.LOGGING['traceback'])

                    for id,op in batch_ops.items():
                        while not op.done():
                           logger.debug("Waiting on batch {}".format(id))
                           time.sleep(3)
                        print("Batch ID {} Status: {}".format(batch_id,batch_ops[batch_id].metadata.state))


                
                else:
                    raise FileNotFoundError("Missing input file.")

        except Exception as e:
            if input_job:
                logger.error("Error creating job:\n{}\nMessage:{}".format(input_job,e),exc_info=config.LOGGING['traceback'])
            else:
                logger.error("Error creating job from file {}. Message: {}".format(file,e),exc_info=config.LOGGING['traceback'])
            # self.delete_job(project_id,tenant_id,job_string.external_id,forced=True)

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
        logger.debug("CALLED: get_job({},{},{},{},{},{},{} by {})".format(project_id,tenant_id,external_id,languages,status,all,scope,\
            inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()    

            if project_id is not None:
                job_key = project_id
            else:
                logging.error("Missing arguments: project_id.",exc_info=config.LOGGING['traceback'])
                raise ValueError

            if tenant_id is not None:
                # Calculate the tenant_id part of the job_key to look up from the DB
                logger.debug("Tenant: {}\n".format(tenant_id))
                job_key = job_key + "-" + tenant_id
            
            if company_id is not None:
                # Calculate the company_id part of the job_key to look up from the DB
                job_key = job_key + "-" +company_id
            else:
                logging.error("Missing arguments: company_id.",exc_info=config.LOGGING['traceback'])
                raise ValueError                

            if external_id is not None:
                # Check if it is a SHOW operation but was passed a conflicting arg
                logger.debug("Req ID: {}\n".format(external_id))
                if all:
                    logging.error("Conflicting arguments: --external_id and --all are mutually exclusive."\
                        ,exc_info=config.LOGGING['traceback'])
                    raise ValueError
                #Return english listing by default
                lang_list = [str(l) for l in languages.split(',')]
                job_keys = [job_key+"-"+external_id+"-"+language for language in lang_list]
                logger.debug("Searching for job: {}".format(job_key))
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
                    logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
                    if tenant_obj is None:
                        logging.error("Unknown Tenant: {}".format(tenant_id),\
                            exc_info=config.LOGGING['traceback'])
                        exit(1)
                    parent = tenant_obj.name
                else:
                    parent = client.project_path(project_id)
                logger.debug("Parent path: {}".format(parent))
                #Default filter
                filter_ = "status = \"{}\"".format(status)

                # Look up company resource path for filtering
                company = cts_company.Company().get_company(project_id=project_id,tenant_id=tenant_id,external_id=company_id)
                logger.debug("Company retrieved:\n{}".format(company))
                if company is None:
                    logging.error("Unknown company: {}. Company is a mandatory attribute for a job listing, create the company \
                        before creating or looking up job listings.".format(company_id))
                    exit(1)
                else:
                    filter_ = filter_ + " AND companyName = \"{}\"".format(company[0].name)
                # Add status to the filter object
                
                logger.debug ("Listing all jobs for {}, filtered by: \n{}".format(parent,filter_))
                jobs = [t for t in client.list_jobs(parent,filter_)]
                return jobs

            # #Show operation
            # if not all:
            #     # Calculate the job_key to look up from the DB
            #     if company_id is not None:
            #         # Check if company name was passed in the attribute as well which would conflict with the command line option
            #         if filter_obj is not None: 
            #             if filter_obj['companyName'] is not None:
            #                 logging.exception("Conflicting arguments: --company_id and companyName filter are mutually exclusive.")
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
            #                     logging.exception("Conflicting arguments: --external_id and --all are mutually exclusive.")
            #                     raise ValueError
            #                 job_key = project_id+"-"+tenant_id+"-"+company_id+"-"+external_id 
            #             else:
            #                 # No requisition ID was passed
            #                 job_key = project_id+"-"+tenant_id+"-"+company_id
            #         else:
            #             # No tenant ID was passed - default tenant
            #             job_key = project_id+"-"+company_id
                    
            #         logger.debug("Searching for job: {}".format(job_key))
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
            #         logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
            #         if tenant_obj is None:
            #             logging.error("Unknown Tenant: {}".format(tenant_id))
            #             exit(1)
            #         parent = tenant_obj.name
            #     else:
            #         parent = client.project_path(project_id)
            #     logger.debug("Parent path: {}".format(parent))
            #     if company_id is not None:
            #         # Check if company name was passed in the attribute as well which would conflict with the command line option
            #         if filter_obj is not None: 
            #             if filter_obj['companyName'] is not None:
            #                 logging.exception("Conflicting arguments: --company_id and companyName filter are mutually exclusive.")
            #                 raise ValueError
            #         else: 
            #             filter_obj = {}
            #         company = cts_company.Company().get_company(project_id=project_id,tenant_id=tenant_id,external_id=company_id)
            #         logger.debug("Company retrieved:\n{}".format(company))
            #         if company is None:
            #             logging.error("Unknown company: {}. Company is a mandatory attribute for a job listing, create the company \
            #                 before creating or looking up a job listing.".format(company_id))
            #             exit(1)
            #         else:
            #             filter_obj['companyName'] = company.name
                
            #     logger.debug ("Listing all jobs for {}, filtered by: \n{}".format(parent,filter_obj))
            #     filter_ = json.dumps(filter_obj).encode('utf-8')
            #     job = [t for t in client.list_jobs(parent,filter_)]
 
            # return job
        except Exception as e:
            if external_id is not None:
                logger.error("Error getting job by name {}. Message: {}".format(\
                    external_id,e),exc_info=config.LOGGING['traceback'])
            else:
                logger.error("Error getting job for project {} company {}. Message: {}".format(
                project_id,company_id,e),exc_info=config.LOGGING['traceback'])
            raise 
