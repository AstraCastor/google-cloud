from google.cloud import talent_v4beta1
import os
import sys
import logging
import inspect
import re
import json
from datetime import datetime
import time
import collections 

from modules import cts_db,cts_tenant,cts_company,cts_helper
from conf import config as config
from google.cloud.talent_v4beta1.types import Job as CTS_Job
from google.cloud.talent_v4beta1.proto.common_pb2 import RequestMetadata
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError, RetryError
from modules.cts_errors import UnparseableJobError

#Get the root logger
logger = logging.getLogger()

class Job():

    def client(self):
        try:
            if 'secret_key' in config.APP:
                if os.path.exists(config.APP['secret_key']):
                    _job_client = talent_v4beta1.JobServiceClient\
                        .from_service_account_file(config.APP['secret_key'])
                    logger.debug("credentials: {}".format(config.APP['secret_key']))
                else:
                    raise Exception("Missing credential file.")
            else:
                _job_client = talent_v4beta1.JobServiceClient()
            logger.debug("Job client created: {}".format(_job_client))
            return _job_client
        except Exception as e:
            logging.error("Error instantiating Job client. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
            exit(1)

    def create_job(self,project_id,tenant_id=None,input_job=None,file=None):
        logger.debug("logger:CALLED: create_job({},{},{},{})".format(project_id,tenant_id,input_job,file))
        try:
            client = self.client()
            #Prepare Request Metadata
            job_req_metadata = config.APP['request_metadata']           
            logger.debug("Setting request Metadata to {}".format(job_req_metadata))

            # Create a single job. If no file arg is provided a job string is required as input.
            if file is None:
                try:
                    company_id = input_job['company']
                    external_id = input_job['requisition_id']
                    language = input_job['language_code'] or config.APP['default_language']
                    existing_job = self.get_job(project_id=project_id,company_id=company_id,\
                        tenant_id=tenant_id,external_id=external_id,languages=language,\
                            scope='limited')
                    if existing_job is not None:
                        print("Job {} already exists for the given parameters.".format(external_id))
                        logger.debug("Job {} already exists for the given parameters.".format(external_id))
                    else:
                        logger.debug("create_job: Parsing job string: {}".format(input_job))
                        parsed_job = cts_helper.parse_job(project_id=project_id,tenant_id=tenant_id,jobs=[input_job])[0]
                        if parsed_job is None:
                            raise UnparseableJobError
                        parent = cts_helper.get_parent(project_id,tenant_id)
                        logger.debug("create_job: Parent is set to {}".format(parent))
                        logger.debug("Posting parsed job {}".format(parsed_job))
                        new_job = client.create_job(parent,parsed_job,metadata=[job_req_metadata])
                        if new_job is not None:
                            if cts_db.persist_to_db(object=new_job,project_id=project_id,tenant_id=tenant_id,company_id=company_id):
                                return new_job
                                # print ("Created job requisition ID {} for company {}.".format(input_job.requisition_id,company_id))
                        else:
                            raise
                        
                except UnparseableJobError as e:
                    logger.error("Unable to parse the input job string.")
                except AlreadyExists as e:
                    # Sync with DB if it doesn't exist in DB
                    logger.warning("Job already exists. Message: {}".format(e))
                    logger.info("Local DB out of sync. Syncing local db..")
                    print("Job already exists. Message: {}".format(e))
                    print("Local DB out of sync. Syncing local db..")
                    sync_job_name = re.search("^Job (.*) already exists.*$",e.message).group(1)
                    sync_job = client.get_job(sync_job_name)
                    if cts_db.persist_to_db(sync_job,project_id=project_id,tenant_id=tenant_id,company_id=company_id):
                        logger.warning("Job requisition ID {} for company {} synced to DB.".format(parsed_job['requisition_id'],\
                            company_id))
                        print("Job requisition ID {} for company {} synced to DB.".format(parsed_job['requisition_id'],\
                            company_id))
                    else:
                        raise Exception("Error when syncing job requisition ID {} for company {} to DB.".format(sync_job.requisition_id))
                except ValueError:
                    logger.error("Invalid Parameters")
                    return None
                except GoogleAPICallError as e:
                    logger.error("API error when creating job. Message: {}".format(e))
                    return None
                except Exception as e:
                    logger.error("Error when creating job. Message: {}".format(e))
                    return None

            # Create a batch of jobs. Job strings are provided in a new line delimited JSON file as input.
            else:                
                if os.path.exists(file):
                    logger.debug("Reading input file from {}".format(file))
                    batch_size = config.BATCH_PROCESS['batch_size'] or 200
                    conc_batches = config.BATCH_PROCESS['concurrent_batches'] or 1
                    batch_info = {}
                    logger.debug("Batching the file @ {} rows in {} concurrent batches to be posted".format(batch_size,conc_batches))
                    # Generate the batches to be posted
                    for concurrent_batch in cts_helper.generate_file_batch(file=file,rows=batch_size,concurrent_batches=conc_batches):
                        for batch in concurrent_batch:
                            try:
                                for batch_id,jobs in batch.items():
                                    batch_info.update({batch_id:{"batch":{},"operation":"","posted":"","done":False,"error":""}})
                                    # Check each job in the batch exists already and remove it from the batch
                                    parsed_jobs = collections.OrderedDict()
                                    for line,job in jobs.items():
                                        batch_info[batch_id]['batch'].update({line:{}})
                                        try:
                                            job_json = json.loads(job)
                                            company_id = job_json['company']
                                            external_id = job_json['requisition_id']
                                            language=job_json['language_code']
                                            batch_info[batch_id]['batch'].update({line:{"company":company_id,"requisition_id":external_id,\
                                                "language_code":language,"status":"READ","errors":[]}})
                                        except (AttributeError,json.JSONDecodeError) as e:
                                            batch_info[batch_id]['batch'].update({line:{'status':'ERROR','errors':[]}})
                                            batch_info[batch_id]['batch'][line]['errors'].append(e)
                                            continue
                                        # Skip the already existing jobs
                                        if self.get_job(project_id=project_id,company_id=company_id,tenant_id=tenant_id,\
                                            external_id=external_id,languages=language,scope='limited'):
                                            print("Skipping existing job on line {}".format(line))
                                            batch_info[batch_id]['batch'][line].update({'status':'SKIPPED'})
                                            batch_info[batch_id]['batch'][line].update({'errors':['Job already exists.']})
                                            continue
                                        
                                        # Parse the job if it isn't a malformed or an existing job
                                        parsed_job = cts_helper.parse_job(project_id=project_id,tenant_id=tenant_id,jobs=[job])
                                        if parsed_job and 'ERROR' not in parsed_job[0]:
                                            # returned parsed_job is a list, add to the parsed_jobs OrderedDict for the entire batch
                                            parsed_jobs.update({line:parsed_job[0]})
                                            logger.info("Job {}:{} for company {} parsed.".format(external_id,\
                                                language,company_id))
                                            batch_info[batch_id]['batch'][line].update({'status':'PARSED'})
                                        else:
                                            logger.warning("Line {}: Parse job failed for job {}:{} for company {}: {}".format(line, external_id,\
                                                language,company_id,parsed_job[0]))
                                            print("Line {}: Parse job failed for job {}:{} for company {}: {}".format(line,external_id,\
                                                language,company_id,parsed_job[0]))                                            
                                            batch_info[batch_id]['batch'][line].update({'status':'PARSE_FAILED','errors':[]})
                                            batch_info[batch_id]['batch'][line]['errors'].append(parsed_job[0])
                                    if parsed_jobs:
                                        # Getting ready to post the batch: get the parent
                                        parent = cts_helper.get_parent(project_id,tenant_id)
                                        logger.debug("Batch {}: Parent is set to {}".format(batch_id,parent))
                                        logger.debug("Batch {}: Posting {} jobs between lines {} to {}".format(batch_id,len(parsed_jobs),\
                                            list(batch_info[batch_id]['batch'].keys())[0],list(batch_info[batch_id]['batch'].keys())[-1]))
                                        print("Batch {}: Posting {} jobs between lines {} to {}".format(batch_id,len(parsed_jobs),\
                                            list(batch_info[batch_id]['batch'].keys())[0],list(batch_info[batch_id]['batch'].keys())[-1]))
                                        # Post the jobs
                                        batch_info[batch_id]['operation']= client.batch_create_jobs(parent,parsed_jobs.values(),metadata=[job_req_metadata])
                                        batch_info[batch_id]['posted'] = parsed_jobs.keys()
                                        for posted_line in batch_info[batch_id]['posted']:
                                            batch_info[batch_id]['batch'][posted_line]['status'] = 'POSTED'
                                    else:
                                        logger.warning("No jobs to post in batch {}".format(batch_id))
                                        print("No jobs to post in batch {}".format(batch_id))
                                        batch_info[batch_id]['done']=True

                            except RetryError as e:
                                batch_info[batch_id]['done'] = True
                                batch_info[batch_id]['error']= "API Retry failed due to {}.".format(e)
                                logger.error("Batch {}: API Retry failed due to {}.".format(batch_id,e),\
                                    exc_info=config.LOGGING['traceback'])
                            except GoogleAPICallError as e:
                                batch_info[batch_id]['error']="API Retry failed due to {}.".format(e)
                                logger.error("Batch {}: CTS API Error due to {}".format(batch_id,e),\
                                    exc_info=config.LOGGING['traceback'])

                    # Check if all batches are done
                    all_done = False
                    while not all_done:    
                        for batch_id in batch_info:
                            if 'operation' in batch_info[batch_id]:
                                batch_op = batch_info[batch_id]['operation']
                                if (batch_op.done() and not batch_info[batch_id]['done']):
                                    batch_result = batch_op.result()
                                    logger.debug("Batch ID {} results:\n".format(batch_id))
                                    for posted_line,result in zip(batch_info[batch_id]['posted'],batch_result.job_results):
                                        logger.debug("Job {}:{} for company {} creation status: {}"\
                                            .format(batch_info[batch_id]['batch'][posted_line]['requisition_id'],\
                                                batch_info[batch_id]['batch'][posted_line]['language_code'],\
                                                    batch_info[batch_id]['batch'][posted_line]['company'],result.status.code))
                                        # Job created successfully
                                        if result.status.code == 0:
                                            batch_info[batch_id]['batch'][posted_line]['status'] = 'SUCCESS'
                                            cts_db.persist_to_db(result.job,project_id=project_id,tenant_id=tenant_id,\
                                                company_id=batch_info[batch_id]['batch'][posted_line]['company'])
                                            logger.info("Job {}:{} for company {} created."\
                                            .format(batch_info[batch_id]['batch'][posted_line]['requisition_id'],\
                                                batch_info[batch_id]['batch'][posted_line]['language_code'],\
                                                    batch_info[batch_id]['batch'][posted_line]['company']))
                                            print("Job {}:{} for company {} created."\
                                            .format(batch_info[batch_id]['batch'][posted_line]['requisition_id'],\
                                                batch_info[batch_id]['batch'][posted_line]['language_code'],\
                                                    batch_info[batch_id]['batch'][posted_line]['company']))
                                        # Handle out of sync jobs
                                        elif result.status.code == 6:
                                            logger.info("Job {}:{} for company {} already exists in the server and will be synced to the client."\
                                            .format(batch_info[batch_id]['batch'][posted_line]['requisition_id'],\
                                                batch_info[batch_id]['batch'][posted_line]['language_code'],\
                                                    batch_info[batch_id]['batch'][posted_line]['company']))
                                            print("Job {}:{} for company {} already exists in the server and will be synced to the client."\
                                            .format(batch_info[batch_id]['batch'][posted_line]['requisition_id'],\
                                                batch_info[batch_id]['batch'][posted_line]['language_code'],\
                                                    batch_info[batch_id]['batch'][posted_line]['company']))
                                            batch_info[batch_id]['batch'][posted_line]['status'] = 'SYNC'
                                            if self.sync_job(project_id=project_id,tenant_id=tenant_id,\
                                                company_id=batch_info[batch_id]['batch'][posted_line]['company'],\
                                                    external_id=batch_info[batch_id]['batch'][posted_line]['requisition_id']):
                                                batch_info[batch_id]['batch'][posted_line]['status'] = 'SUCCESS'
                                                logger.info("Synced to the client.".format(result.job.requisition_id))
                                                print("Synced to the client.".format(result.job.requisition_id))
                                            else:
                                                batch_info[batch_id]['batch'][posted_line]['status'] = 'SYNC_FAILED'
                                                logger.info("Sync failed.".format(result.job.requisition_id))
                                                print("Sync failed.".format(result.job.requisition_id))
                                        # Handle job creation failures
                                        else:
                                            batch_info[batch_id]['batch'][posted_line]['status'] = 'FAILED'
                                            batch_info[batch_id]['batch'][posted_line]['errors'].append(result.status)
                                            logger.info("Failed to create job {}:{} for company {}."\
                                            .format(batch_info[batch_id]['batch'][posted_line]['requisition_id'],\
                                                batch_info[batch_id]['batch'][posted_line]['language_code'],\
                                                    batch_info[batch_id]['batch'][posted_line]['company']))
                                            print("Failed to create job {}:{} for company {}."\
                                            .format(batch_info[batch_id]['batch'][posted_line]['requisition_id'],\
                                                batch_info[batch_id]['batch'][posted_line]['language_code'],\
                                                    batch_info[batch_id]['batch'][posted_line]['company']))
                                    batch_info[batch_id]['done']=True
                                else:
                                    logger.debug("Waiting on batch {}".format(id))
                            else:
                                logger.warn("No jobs were posted in batch {}".format(batch_id))
                                continue
                        time.sleep(2)
                        # Check which batches are done and compare if all batches have 'DONE' key
                        done_batches = [batch_id for batch_id,batch in batch_info.items() if batch['done']]
                        all_done = True if len(done_batches) == len(batch_info) else False
                    
                    total_jobs_created = 0
                    total_jobs_skipped = 0
                    total_jobs_failed = 0
                    for batch_id,batch in batch_info.items():
                        for line,line_item in batch['batch'].items():
                            total_jobs_created+=1 if line_item['status']=='SUCCESS' else 0
                            total_jobs_skipped+=1 if line_item['status']=='SKIPPED' else 0
                            total_jobs_failed+=1 if line_item['status']=='FAILED' else 0

                            if line_item['status'] != 'SUCCESS':
                                print("Line {}:\nJob {}:{} for company {} {}\nErrors: {}".format(line, \
                                    line_item['requisition_id'],line_item['language_code'],line_item['company'],\
                                        line_item['status'],line_item['errors']))
                    print("Total Jobs created: {}".format(total_jobs_created))
                    print("Total Jobs skipped: {}".format(total_jobs_skipped))
                    print("Total Jobs failed: {}".format(total_jobs_failed))
                    #Check all the batches for errors: batch_errors = {batch_id:errors[]}
                    return True
                else:
                    raise FileNotFoundError("Missing input file.")

        except ValueError:
            logger.error("Invalid Parameters")
        except GoogleAPICallError as e:
            logger.error("API error when creating job {}. Message: {}".format(e))
        except Exception as e:
            if input_job:
                logger.error("Error creating job:\n{}\nMessage:{}".format(input_job,e),exc_info=config.LOGGING['traceback'])
            else:
                logger.error("Error creating job from file {}. Message: {}".format(file,e),exc_info=config.LOGGING['traceback'])
            raise e
                   

    def update_job(self,tenant_id,project_id=None,job=None,file=None):
        print ("update job") if file is None else print ("batch update job")
    
    def delete_job(self,project_id,tenant_id=None,company_id=None,external_id=None,languages=config.APP['default_language'] or "en-US",all=False,force=False):
        """ Delete a CTS company by external name.
        Args:
            project_id: project where the company will be created - string
            external_id: unique ID of the company - string
        Returns:
            None - If company is not found.
        """
        logger.debug("CALLED: delete_job({},{},{},{},{},{},{} by {})".format(project_id,tenant_id,company_id,\
            external_id,languages,all,force,inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()  

            if external_id:
                # One or more languages delete
                if force:
                    # If DB is out of sync with the server, get all the jobs for a company from the server directly
                    # and loop through the list to find the job 
                    # get_job(all,scope=full) gets everything from the server directly for a given company
                    all_jobs = self.get_job(project_id=project_id,tenant_id=tenant_id,company_id=company_id, status='ALL', all=True,scope='full')
                    logger.debug("Total jobs retrieved: {}".format(len(all_jobs)))
                    if languages == 'ALL':
                        existing_jobs = [job for job in all_jobs if job.requisition_id == external_id]
                    else:
                        lang_list = [str(l) for l in languages.split(',')]
                        existing_jobs = [job for job in all_jobs if job.requisition_id == external_id and job.language_code in lang_list]
                    
                else:
                    # Deleting specific jobs by external_id and language_code
                    logger.debug("Calling get_job({},{},{},{},{},{})".format(project_id,tenant_id,company_id,external_id,languages,'limited'))
                    existing_jobs = self.get_job(project_id=project_id,company_id=company_id,tenant_id=tenant_id,\
                        external_id=external_id,languages=languages,scope='limited')
                    logger.debug("Existing job? {}".format(existing_jobs))
            else:
                # If job external_id (requisition_id) is not provided, bulk delete
                logger.debug("Calling get_job({},{},{},{},{})".format(project_id,tenant_id,company_id,all,'full' if force else 'limited'))
                existing_jobs = self.get_job(project_id=project_id,company_id=company_id,tenant_id=tenant_id,all=True,scope='full' if force else 'limited')

            if existing_jobs:
                confirmation = cts_helper.user_confirm("{} job(s) from {} will be deleted in {} tenant. Confirm (y/n/Enter):"\
                    .format(len(existing_jobs),company_id or "all companies",tenant_id or 'DEFAULT'))
                if confirmation: 
                    logger.debug("User confirmation: {}".format(confirmation))
                    deleted = 0
                    for job in existing_jobs:
                        try:
                            logger.info("Deleting job id {}: {} for company {}({})...".format(job.requisition_id,job.language_code, job.company_display_name,job.company))
                            client.delete_job(job.name)
                            db.execute("DELETE FROM job where job_name = ?",(job.name,))
                            logger.info("Deleted.".format(job.requisition_id,job.language_code,job.company_display_name))
                            print("Job {}:{} deleted for company {}({}).".format(job.requisition_id,job.language_code,job.company_display_name,job.company))
                            deleted += 1
                            print("Deleted {} of {} jobs".format(deleted,len(existing_jobs)))
                        except GoogleAPICallError as e:
                            logger.error("API error when deleting job {}:{} for company {}. Message: {}".format(job.requisition_id,\
                                job.language_code,company_id,e))
                        except Exception as e:
                            logger.error("Error when deleting job {}:{} for company {}. Message: {}".format(job.requisition_id,\
                                job.language_code,company_id,e))                            
                    print("Total jobs deleted: {}".format(deleted))
                    logger.info("Total jobs deleted: {}".format(deleted))
                else:
                    print ("Aborted.")
            else:
                logger.warning("No {}{}{} {} for {} in the {} tenant.".format("job id" if external_id else "jobs",\
                    external_id or "",":"+languages if external_id else "",'exists' if external_id else 'exist', \
                        company_id or "any companies",tenant_id or 'default'))
                print("No {}{}{} {} for {} in the {} tenant.".format("job id" if external_id else "jobs",\
                    external_id or "",":"+languages if external_id else "",'exists' if external_id else 'exist', \
                        company_id or "any companies",tenant_id or 'default'))
                return None

        except ValueError:
            logger.error("Invalid Parameters")
        except GoogleAPICallError as e:
            logger.error("API error when creating job. Message: {}".format(e))        
        except Exception as e:
            logger.error("Error deleting job ID {} for company {}. Message: {}".format(external_id,company_id,e),\
                exc_info=config.LOGGING['traceback'])
            raise


    def get_job(self,project_id,company_id,tenant_id=None,external_id=None,languages=config.APP['default_language'],status='OPEN',all=False,scope='full'):
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

            if all:
                # LIST operation, could be a tenant level listing (named or default tenant), or a company level listing
                if external_id:
                    logging.error("Conflicting arguments: --all and --external_id are mutually exclusive."\
                        ,exc_info=config.LOGGING['traceback'])
                    raise ValueError
                elif company_id:        
                    # List jobs by company ID
                    if scope=='full':
                        # Calculate the parent path : Get tenant name if tenant ID was provided, if not default tenant under the 
                        # given project.
                        if tenant_id is not None:
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
                        #Default filter
                        # Add status to the filter object
                        filter_ = "status = \"{}\"".format(status)

                        # Look up company resource path for filtering
                        company = cts_company.Company().get_company(project_id=project_id,tenant_id=tenant_id,external_id=company_id,scope='limited')
                        logger.debug("Company retrieved:\n {}".format(company))
                        if company is None:
                            logger.error("Unknown company: {}. Company is a mandatory attribute for a job listing, create the company before creating or looking up job listings.".format(company_id))
                            exit(1)
                        else:
                            filter_ = filter_ + " AND companyName = \"{}\"".format(company[0].name)

                        # List by company ID with full scope --> Query CTS to get the full job objects with company filter
                        logger.debug ("Retrieving all jobs for company {} from tenant {}, filtered by: \n{}".format(company_id,\
                            tenant_id or 'DEFAULT',filter_))
                        jobs = [t for t in client.list_jobs(parent,filter_)]
                        logger.debug("Retreived {} jobs".format(len(jobs)))
                        return jobs
                    else:
                        # Scope = 'limited', retrieve only the IDs from local DB, not the full job objects from the server
                        if tenant_id is not None:
                            # Calculate the tenant_id part of the job_key to look up from the DB
                            job_key = job_key + "-" + tenant_id
                            logger.debug("Tenant added to job key:{}".format(job_key))
                        
                        # Add the company part of the job key
                        job_key = job_key + "-" + company_id
                        logger.debug("Company added to job key: {}".format(job_key))
                        logger.debug("Searching for all jobs for company {} in tenant {}".format(company_id, tenant_id or 'DEFAULT'))
                else:
                    # If no company_id was provided, list jobs by tenant (default or a named tenant)
                    # Note: this is not an op supported by CTS, this is implemented only as a local DB lookup followed by server lookup for full scope listings.
                    if tenant_id is not None:
                        # Calculate the tenant_id part of the job_key to look up from the DB
                        job_key = job_key + "-" + tenant_id
                        logger.debug("Searching for all jobs from tenant {}".format(tenant_id or 'DEFAULT'))
                
                #SQL Pattern search for all jobs based on the job_key
                job_key = job_key + '%'
                logger.debug("Search for all jobs based on the job_key: {}".format(job_key))
                db.execute("SELECT distinct job_name,external_id,language_code,company_name FROM job where job_key like (?)",(job_key,))

            # Show operation, for a specific job requisition ID (external_id) and one, more or all languages      
            else:
                # Calculate the tenant_id part of the job_key to look up from the DB
                if tenant_id is not None:
                    job_key = job_key + "-" + tenant_id
                    logger.debug("Tenant added to job key: {}".format(job_key))

                # Calculate the company_id part of the job_key to look up from the DB
                if company_id:
                    job_key = job_key + "-" +company_id
                    logger.debug("Company added to job key: {}".format(job_key))

                if external_id:
                    # Check if it is a SHOW operation but was passed a conflicting arg
                    if all:
                        logging.error("Conflicting arguments: --external_id and --all are mutually exclusive."\
                            ,exc_info=config.LOGGING['traceback'])
                        raise ValueError

                    # Add the requisition ID to the job key
                    job_key = job_key + "-" +external_id
                    logger.debug("Requisition ID added to job key: {}".format(job_key))


                    #Return english listing by default
                    if languages=='ALL':
                        logger.debug("Searching for job: {} all languages".format(job_key))
                        #SQL Pattern search for all languages for a given external ID
                        job_key = job_key + '%'
                        db.execute("SELECT distinct job_name,external_id,language_code,company_name FROM job where job_key like (?)",(job_key,))
                    else:
                    # Get one or more language listings
                        lang_list = [str(l) for l in languages.split(',')]
                        job_keys = [job_key+"-"+language for language in lang_list]
                        logger.debug("Searching for job keys: \n{}".format(job_keys))
                        db.execute("SELECT distinct job_name,external_id,language_code,company_name FROM job where job_key in ({})".format(','.join('?'*len(job_keys))),(job_keys))

            # After one of the SQL statements execute, get all the rows and return the processed data.    
            rows = db.fetchall()
            logger.debug("Jobs from DB: {}".format(len(rows)))
            if rows == []:
                return None
            else:
                if scope=='limited':
                    # jobs = [Job(name=row[0],requisition_id=row[1],language_code=row[2],company=row[3]) for row in rows]
                    jobs = []
                    for row in rows:
                        job = CTS_Job()
                        job.name = row[0]
                        job.requisition_id=row[1]
                        job.language_code=row[2]
                        job.company=row[3]
                        jobs.append(job)
                else:
                    jobs = [client.get_job(row[0]) for row in rows]
                logger.info("Jobs retrieved from CTS: {}".format(len(jobs)))
                return jobs     

        except Exception as e:
            if external_id is not None:
                logger.error("Error getting job by name {}. Message: {}".format(\
                    external_id,e),exc_info=config.LOGGING['traceback'])
            else:
                logger.error("Error getting job for project {} company {}. Message: {}".format(
                project_id,company_id,e),exc_info=config.LOGGING['traceback'])
            raise 
    
    def sync_job(self,project_id,tenant_id=None,company_id=None,external_id=None):
        """ 
        Sync a job or a batch of jobs for a company from the server to the client.
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

            #Default filter
            # Add status to the filter object
            state_filter = "status = \"OPEN\""

            # Add the requisition ID for filtering
            if external_id:
                if company_id:
                    req_filter = " AND requisitionId = \"{}\"".format(external_id)
                else:
                    logger.error("Invalid arguments. Missing mandatory company id for job {}.".format(external_id)\
                        ,config.LOGGING['traceback'])
                    raise ValueError
                    exit(1)


            # Look up company resource path for filtering
            if company_id:
                companies = cts_company.Company().get_company(project_id=project_id,tenant_id=tenant_id,external_id=company_id,scope='limited')
                logger.debug("Company retrieved:\n {}".format(companies))
            else:
                # this will be looping over all companies now
                logger.info("Sync jobs for all companies in the {} tenant...".format(tenant_id or "default"))
                print("Sync jobs for all companies in the {} tenant...".format(tenant_id or "default"))
                companies = cts_company.Company().get_company(project_id=project_id,tenant_id=tenant_id,all=True,scope='limited')
                logger.debug("Companies retrieved:\n {}".format(companies))
                print([company.external_id for company in companies])
            if companies:
                synced_jobs = 0           
                for company in companies:
                    # Sync jobs company by company for all companies
                    company_filter = " AND companyName = \"{}\"".format(company.name)
                    company_id = company.external_id

                    filter_ = state_filter + company_filter + (req_filter if external_id else "")
                    # List by company ID with full scope --> Query CTS to get the full job objects with company filter
                    logger.debug ("Retrieving all jobs for company {} from {} tenant, filtered by: \n{}".format(company_id,\
                        tenant_id or 'default',filter_))
                    lookedup_jobs = [t for t in client.list_jobs(parent,filter_)]
                    logger.debug("Retreived job count: {}".format(len(lookedup_jobs)))

                    # Check if the retrieved jobs exist in the database (i.e. scope = limited) and persist if not found
                    for job in lookedup_jobs:
                        if not self.get_job(project_id=project_id,company_id=company_id,tenant_id=tenant_id,external_id=job.requisition_id,\
                            languages=job.language_code,scope='limited'):
                            if cts_db.persist_to_db(job,project_id=project_id,tenant_id=tenant_id,company_id=company_id):
                                logger.info("Synced job {}:{} for company {} in {} tenant.".format(job.requisition_id,job.language_code,\
                                    company_id,tenant_id))
                                print("Synced job {}:{} for company {} in {} tenant.".format(job.requisition_id,job.language_code,\
                                    company_id,tenant_id))
                                synced_jobs += 1
                            else:
                                print("Sync failed for job {}:{} for company {} in {} tenant.".format(job.requisition_id,job.language_code,\
                                    company_id,tenant_id))
                        else:
                            logger.info("Job {}:{} for company {} already exists in {} tenant.".format(job.requisition_id,job.language_code,\
                                    company_id,tenant_id))
                            print("Job {}:{} for company {} already exists in {} tenant.".format(job.requisition_id,job.language_code,\
                                    company_id,tenant_id))
                if synced_jobs > 0:
                    logger.info("Synced {} jobs.".format(synced_jobs))
                    print("Synced {} jobs.".format(synced_jobs))
                else:
                    logger.info("Nothing to sync.")
                    print("Nothing to sync.")                    
                return True
            else:
                logger.error("No companies found in the {} tenant. Companies need to be created/synced before \
                    jobs can be synced.".format(tenant_id))

        except Exception as e:
            if external_id is not None:
                logger.error("Error syncing job by id {}. Message: {}".format(\
                    external_id,e),exc_info=config.LOGGING['traceback'])
            else:
                logger.error("Error syncing job for project {} company {}. Message: {}".format(
                project_id,company_id,e),exc_info=config.LOGGING['traceback'])
            raise 