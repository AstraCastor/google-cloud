import logging
from modules import cts_tenant,cts_company
from res import config
import json

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
            tenant_obj = client.get_tenant(project_id,tenant_id)
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
            else:
                raise TypeError
        
        company_ids=list(set([job['company'] for job in job_batch]))
        logger.debug("Looking up companies for the current job batch:\n{}".format(company_ids))
        if company_ids is not None and "" not in company_ids:
            company_client = cts_company.Company()
            companies = company_client.get_company(project_id=project_id,tenant_id=tenant_id,external_id=(",").join(company_ids),scope='limited')
            logger.debug("get_company returned: {}".format([c.external_id for c in companies]))
        else:
            logger.error("Missing or unknown company ID in the input jobs".format())
            raise KeyError
        # Check if all companies were looked up
        if len(company_ids)!=len(companies):
            companies_ids = [comp['external_id'] for comp in companies]
            raise ValueError("Missing or unknown company external ids:: {}".format(company_ids - companies))
        
        parsed_batch = []
        #Parse the jobs now
        for job in job_batch:
            job['company']=[company.name for company in companies if job['company']==company.external_id].pop()
            job['promotion_value']=int(job['promotion_value'])
            job['job_start_time']=Timestamp(seconds=int(job['job_start_time']))
            job['job_end_time']=Timestamp(seconds=int(job['job_end_time']))
            job['posting_publish_time']=Timestamp(seconds=int(job['posting_publish_time']))
            job['posting_expire_time']=Timestamp(seconds=int(job['posting_expire_time']))
            for key in job['custom_attributes']:
                job['custom_attributes'][key]=CustomAttribute(string_values=[job['custom_attributes'][key]['string_values'][0]],filterable=True)
            parsed_batch.append(job)
        
    except TypeError:
        logger.error("Passed job string is not a valid JSON convertible string. Error when parsing:\n {}".format(job),\
            exc_info=config.LOGGING['traceback'])
    except Exception as e:
        logger.error("Error occured when parsing job string:\n {}".format(job),exc_info=config.LOGGING['traceback'])
    else:
        return parsed_batch