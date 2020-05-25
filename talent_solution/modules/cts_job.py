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
from conf import config as config
from google.cloud.talent_v4beta1.types import Job as CTS_Job
from google.cloud.talent_v4beta1.proto.common_pb2 import RequestMetadata
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError, RetryError
from modules.cts_errors import UnparseableJobError

#Get the root logger
logger = logging.getLogger()

class Job():

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
                    logger.warning("Local DB out of sync. Syncing local db..")
                    sync_job_name = re.search("^Job (.*) already exists.*$",e.message).group(1)
                    sync_job = client.get_job(sync_job_name)
                    if cts_db.persist_to_db(sync_job,project_id=project_id,tenant_id=tenant_id,company_id=company_id):
                        logger.warning("Job requisition ID {} for company {} synced to DB.".format(parsed_job['requisition_id'],\
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

                    #TODO: Replace with a batch_info object to unify all the batch output metrics.
                    # batch_info[batch_id]={"start":starting_line,"end":ending_line,"input":"","posted":"",\
                    # "operation":"","created":"","errors":[]}
                    total_jobs_created=0

                    def operation_complete(operation_future):
                        nonlocal total_jobs_created
                        try:
                            operation = operation_future.operation
                            op_result = operation_future.result()
                            for id,op in batch_ops.items():
                                if op == operation_future:
                                    batch_id = id 
                            logger.debug("Batch ID {} results:\n".format(batch_id))
                            job_count = 0
                            for result in op_result.job_results:
                                if result.job.requisition_id is not None and result.job.requisition_id is not "":
                                    cts_db.persist_to_db(result.job,project_id=project_id,tenant_id=tenant_id,company_id=company_id)
                                    logger.debug("Job {} created.".format(result.job.requisition_id))
                                    job_count += 1
                                else:
                                    error_row = (batch_id-1)*batch_size+list(op_result.job_results).index(result)+1
                                    print ("Error when creating job in row {}".format(error_row))
                                    logger.warning("Error when creating job in row {}".format(error_row))  
                        except Exception as e:
                            logger.error("Error when creating jobs. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
                        else:
                            print("Batch {}: {} jobs created.".format(batch_id,job_count))
                            logger.debug("Batch {}: {} jobs created.".format(batch_id,job_count))
                            total_jobs_created += job_count

                    logger.debug("Batching the file @ {} rows in {} concurrent batches to be posted".format(batch_size,conc_batches))
                    # Generate the batches to be posted
                    batch_ops = {}
                    batch_errors={}
                    for concurrent_batch in cts_helper.generate_file_batch(file=file,rows=batch_size,concurrent_batches=conc_batches):
                        for batch in concurrent_batch:
                            try:
                                batch_id,jobs = batch.popitem()
                                batch_errors[batch_id]=[]
                                starting_line = (batch_id-1)*batch_size+1
                                ending_line = (batch_id-1)*batch_size
                                # Check each job in the batch exists already and remove it from the batch
                                for job in list(jobs):
                                    try:
                                        ending_line += 1 
                                        job_json = json.loads(job)
                                        company_id = job_json['company']
                                        external_id = job_json['requisition_id']
                                        language=job_json['language_code']
                                    except AttributeError as e:
                                        raise UnparseableJobError(e)
                                    # Remove the already existing jobs
                                    if self.get_job(project_id=project_id,company_id=company_id,tenant_id=tenant_id,\
                                        external_id=external_id,languages=language,scope='limited'):
                                        print("Skipping existing job on line {}".format(ending_line))
                                        jobs.remove(job)
                                # If Jobs got cleared out completely, skip the batch altogether
                                # else parse the remaining jobs in the batch
                                if not jobs:
                                    print("All jobs between lines {} - {} exist already.".format(starting_line,ending_line))
                                    logger.debug("All jobs in batch {} exist already.".format(batch_id))
                                    continue
                                else:
                                    logger.debug("Batch {}: Parsing {} jobs".format(batch_id,len(jobs)))
                                    parsed_jobs = cts_helper.parse_job(project_id=project_id,tenant_id=tenant_id,jobs=jobs)
                                if parsed_jobs is None:
                                    raise UnparseableJobError
                                
                                # Getting ready to post the batch: get the parent
                                parent = cts_helper.get_parent(project_id,tenant_id)
                                logger.debug("Batch {}: Parent is set to {}".format(batch_id,parent))
                                logger.debug("Batch {}: Posting {} jobs between lines {} to {}".format(batch_id,len(parsed_jobs),starting_line,ending_line))
                                batch_ops[batch_id]= client.batch_create_jobs(parent,parsed_jobs,metadata=[job_req_metadata])
                                batch_ops[batch_id].add_done_callback(operation_complete)

                            except UnparseableJobError as e:
                                batch_errors[batch_id].append({"message":"Unable to parse one or more jobs between lines {} and {}".format(\
                                    starting_line,ending_line),"jobs":e
                                    })
                                logger.warning("Batch {}: Unable to parse one or more jobs between lines {} and {}".format(batch_id,\
                                    starting_line,ending_line))
                            except RetryError as e:
                                batch_errors[batch_id].append({"message":"API Retry failed due to {}.".format(e)})
                                logger.error("Batch {}: API Retry failed due to {}.".format(batch_id,e),\
                                    exc_info=config.LOGGING['traceback'])
                            except GoogleAPICallError as e:
                                batch_errors[batch_id].append({"message":"API Retry failed due to {}.".format(e)})
                                logger.error("Batch {}: CTS API Error due to {}".format(batch_id,e),\
                                    exc_info=config.LOGGING['traceback'])

                    for id,op in batch_ops.items():
                        while not op.done():
                            logger.debug("Waiting on batch {}".format(id))
                            time.sleep(3)
                        logger.debug("Batch ID {} Status: {}".format(id,batch_ops[id].metadata.state))

                    print("Total Jobs created: {}".format(total_jobs_created))
                    #Check all the batches for errors: batch_errors = {batch_id:errors[]}
                    for errors in batch_errors.values():
                        if errors:
                            raise Exception(batch_errors)
                            return None
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
                existing_jobs = self.get_job(project_id=project_id,company_id=company_id,tenant_id=tenant_id,all=True,scope='limited')

            if existing_jobs:
                confirmation = cts_helper.user_confirm("{} job(s) from {} will be deleted in {} tenant. Confirm (y/n/Enter):"\
                    .format(len(existing_jobs),company_id,tenant_id or 'DEFAULT'))
                if confirmation: 
                    logger.debug("User confirmation: {}".format(confirmation))
                    deleted = 0
                    for job in existing_jobs:
                        try:
                            logger.info("Deleting job id {}: {} for company {}".format(job.requisition_id,job.language_code, company_id))
                            client.delete_job(job.name)
                            db.execute("DELETE FROM job where job_name = ?",(job.name,))
                            logger.info("Job {}:{} deleted for company {}.".format(job.requisition_id,job.language_code,company_id))
                            print("Job {}:{} deleted for company {}.".format(job.requisition_id,job.language_code,company_id))
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
                logger.warning("Job {} for company {} does not exist.".format(external_id,company_id))
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
