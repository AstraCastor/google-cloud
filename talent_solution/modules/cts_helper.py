import logging
from modules import cts_tenant

#Get the root logger
logger = logging.getLogger()

def get_parent(project_id,tenant_id=None):
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
    return parent

