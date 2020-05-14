import logging
import os
import sys
from datetime import datetime
import json
import re

from modules import cts_tenant,cts_company,cts_job, cts_db
from modules.cts_errors import UnparseableJobError,UnknownCompanyError
from conf import config

from google.cloud.talent_v4beta1.proto.common_pb2 import CustomAttribute
from google.protobuf.timestamp_pb2 import Timestamp


#Get the root logger
logger = logging.getLogger()


def get_parent(project_id,tenant_id=None):
    try:
        logger.debug("logger:CALLED: get_parent({},{})".format(project_id,tenant_id))
        client = cts_tenant.Tenant()
        if tenant_id is not None:
            # To set the parent of the company to be created to the tenant_name 
            # for the given tenant_id(tenant external_id)
            tenant_obj = client.get_tenant(project_id,tenant_id,scope='limited')
            if tenant_obj is None:
                logging.error("Unknown Tenant: {}".format(tenant_id),exc_info=config.LOGGING['traceback'])
                exit(1)
            else:
                parent = tenant_obj.name
                logger.debug("Tenant retrieved:\n{}".format(tenant_obj))
        else:
            parent = client.project_path(project_id)
        logger.debug("Parent path set to: {}".format(parent))
    except Exception as e:
        logger.error("Error occured when looking up parent. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
    else:
        return parent

def parse_job(project_id,tenant_id,jobs=[]):
    try:
        job_batch = []
        for job in jobs:
            if isinstance(job,str):
                job_batch.append(json.loads(job))
            elif isinstance(job,dict):
                job_batch.append(job)
            else:                
                raise UnparseableJobError
        
        company_ids=list(set([job['company'] for job in job_batch]))
        logger.debug("Looking up companies for the current job batch:\n{}".format(company_ids))
        if company_ids is not None and "" not in company_ids:
            company_client = cts_company.Company()
            companies = company_client.get_company(project_id=project_id,tenant_id=tenant_id,external_id=(",").join(company_ids),scope='limited')
            if companies is not None:
                logger.debug("get_company returned: {}".format([c.external_id for c in companies]))
            # Check if all companies were looked up
                if len(company_ids)!=len(companies):
                    companies_ids = [comp['external_id'] for comp in companies]
                    raise UnknownCompanyError("Missing or unknown company ID(s): {}".format(company_ids - companies))
            else:
                raise UnknownCompanyError("Missing or unknown company ID(s): {}".format(company_ids))
        else:
            logger.error("Missing company ID(s) in the input jobs")
            raise UnknownCompanyError

        
        parsed_batch = []
        #Parse the jobs now
        for job in job_batch:
            errors = {}
            try:
                # Mandatory fields
                if "requisition_id" not in job:
                    raise UnparseableJobError("Missing requisition ID for job {}".format(job))
                if "title" not in job or job['title'] is "":
                    raise UnparseableJobError("Missing job title for job requisition ID {}".format(job['requisition_id']))                
                if "description" not in job or job['description'] is "":
                    raise UnparseableJobError("Missing job description for job requisition ID {}".format(job['requisition_id']))
                if "company" in job:
                    job['company']=[company.name for company in companies if job['company']==company.external_id].pop()
                else:
                    raise UnparseableJobError("Missing company ID for job requisition ID {}".format(job['requisition_id']))
                if "language_code" not in job:
                    raise UnparseableJobError("Missing language code for job requisition ID {}".format(job['requisition_id']))
                # Optional fields parsing
                # The rest of the fields are string fields or repeated fields. If the format doesn't match the whole job will be rejected by CTS.
                if "promotion_value" in job:
                   job['promotion_value']=int(job['promotion_value'])
                if "job_start_time" in job:
                    job['job_start_time']=Timestamp(seconds=int(job['job_start_time']))
                if "job_end_time" in job:
                    job['job_end_time']=Timestamp(seconds=int(job['job_end_time']))
                if "posting_publish_time" in job:
                    job['posting_publish_time']=Timestamp(seconds=int(job['posting_publish_time']))
                if "posting_expire_time" in job:
                    job['posting_expire_time']=Timestamp(seconds=int(job['posting_expire_time']))
                for attr in list(job['custom_attributes']):
                    # job['custom_attributes'][key]=CustomAttribute(string_values=[job['custom_attributes'][key]['string_values'][0]],filterable=True)
                    if 'string_values' in job['custom_attributes'][attr]:
                        if job['custom_attributes'][attr]['string_values'] != [""] and job['custom_attributes'][attr]['string_values'] is not None:
                            job['custom_attributes'][attr]=CustomAttribute(string_values=job['custom_attributes'][attr]['string_values'],filterable=job['custom_attributes'][attr]['filterable'])
                        else: 
                            del(job['custom_attributes'][attr])
                    elif 'long_values' in job['custom_attributes'][attr]:
                        if job['custom_attributes'][attr]['string_values'] != [""] and job['custom_attributes'][attr]['string_values'] is not None:
                            job['custom_attributes'][attr]=CustomAttribute(long_values=job['custom_attributes'][attr]['long_values'],filterable=job['custom_attributes'][attr]['filterable'])
                        else: 
                            del(job['custom_attributes'][attr])
                    else:
                        raise UnparseableJobError("Error parsing custom attribute {} for job requisition ID {}.".format(attr,job['requisition_id']))
                parsed_batch.append(job)
            except UnparseableJobError as e:
                errors[job]=e
                # logger.error("Passed job string is not a valid Job JSON. Error when parsing:\n {}".format(job),\
                #         exc_info=config.LOGGING['traceback'])
        if errors:
            raise UnparseableJobError(errors)
    except Exception as e:
        logger.error("Error occured when parsing job string:\n {}".format(job),exc_info=config.LOGGING['traceback'])
    else:
        return parsed_batch


def generate_file_batch(file,rows=5,concurrent_batches=1):
    """
    A generator function that reads a file in batches of rows and returns a list of n batches at a time.

    Parameters:\n
    file: Full path to the location of the file. Required, Type: String.\n
    rows: No of rows from file in each batch. Default: 5, Type: Int.\n
    concurrent_batches: Number of batches to be returned at a time. Default: 1, Type: Int. Set to an appropriate number when batching operations \
        like batched HTTP requests or multi threading/processing.\n

    Returns:
    A generator object that returns a list of dict of structure [{batch id:[batch]}]. 
    """
    if os.path.exists(file):
        with open(file,'r') as f_handle:
            batch_id = 1
            concurrent_batch = []
            batch = []
            for line_no,line in enumerate(f_handle,1):
                logger.debug("Reading line # {} and adding to batch {} at {}".format(line_no,batch_id,len(batch)) )
                batch.append(line)
                if len(batch) == rows:
                    concurrent_batch.append({batch_id:batch})
                    logger.debug("Concurrent batch of size {}".format(len(concurrent_batch)))
                    if len(concurrent_batch) == concurrent_batches:
                        logger.debug("Sending concurrent batch ")
                        yield concurrent_batch
                        concurrent_batch.clear()
                    batch_id += 1
                    batch.clear()
            else:
                # Add the last batch
                if len(batch)>0:
                    logger.debug("Sending the last batch {}".format(batch_id))
                    concurrent_batch.append({batch_id:batch})
                    yield concurrent_batch
    else:
        raise FileNotFoundError("Missing file {}.".format(file))



