from google.cloud import talent_v4beta1
import os
import sys
import logging
import argparse

#General logging config
log_level = os.environ.get('LOG_LEVEL','INFO')
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger_format = logging.Formatter('%(asctime)s %(filename)s:%(lineno)s %(levelname)-8s %(message)s','%Y-%m-%d %H:%M',)

#Console Logging
console_logger = logging.StreamHandler()
console_logger.setLevel(log_level)
console_logger.setFormatter(logger_format)
logger.addHandler(console_logger)

def get_all_tenants(tenant_client,db_connection,project_id):
    """ Get a CTS tenant name by external name.
    Args:
        talent_client: an instance of TalentServiceClient()
        project_id: project where the tenant will be created - string
        tenant_name: unique ID of the tenant - string
    Returns:
        an instance of Tenant or None if tenant was not found.
    """
    try:
        parent = tenant_client.project_path(project_id)
        tenants = tenant_client.list_tenants(parent)
        tenants_from_db = db_connection.execute("SELECT * FROM tenant")
        return tenants
    except Exception as e:
        print("Error when getting tenant by external ID. Message: {}".format(e))
        raise

def get_tenant_by_name(tenant_client,db_connection,project_id,tenant_name):
    """ Get a CTS tenant name by external name.
    Args:
        talent_client: an instance of TalentServiceClient()
        project_id: project where the tenant will be created - string
        tenant_name: unique ID of the tenant - string
    Returns:
        an instance of Tenant or None if tenant was not found.
    """
    try:
        parent = tenant_client.project_path(project_id)
        for tenant in get_all_tenants(tenant_client,project_id):
            if tenant.external_id == tenant_name:
                return tenant
    except Exception as e:
        print("Error getting tenant by name {}. Message: {}".format(tenant_name,e))
        raise 

def create_tenant(tenant_client,db_connection,project_id,tenant_name):
    """ Create a CTS tenant by external name.
    Args:
        project_id: project where the tenant will be created - string
        tenant_name: unique ID of the tenant - string
    Returns:
        an instance of Tenant or None if tenant was not created.
    """
    try:
        existing_tenant = get_tenant_by_name(tenant_client,project_id,tenant_name)
        if existing_tenant is None:
            parent = tenant_client.project_path(project_id)
            tenant_object = {'external_id':tenant_name}
            new_tenant = tenant_client.create_tenant(parent,tenant_object)
            logger.info("Tenant {} created.\n{}".format(tenant_name,new_tenant))
            return new_tenant
        else:
            logger.error("Tenant {} exists.\n{}".format(tenant_name,existing_tenant))
            return None
    except Exception as e:
        print("Error creating tenant {}: {}".format(tenant_name,e))
        raise

def delete_tenant(tenant_client,project_id,tenant_name):
    """ Delete a CTS tenant by external name.
    Args:
        project_id: project where the tenant will be created - string
        tenant_name: unique ID of the tenant - string
    Returns:
        None - If tenant is not found.
    """
    try:
        existing_tenant = get_tenant_by_name(tenant_client,project_id,tenant_name)
        if existing_tenant is not None: 
            #Tenant.name is not the same as tenant_name. It's confusing, yes, but CTS chose to confuse you.
            tenant_client.delete_tenant(existing_tenant.name)
            logger.info("Tenant {} deleted.".format(tenant_name))
        else:
            logger.error("Tenant {} does not exist.".format(tenant_name,existing_tenant))
            return None
    except Exception as e:
        print("Error deleting tenant {}: {}".format(tenant_name,e))
        raise

def main():

    try:
        main_dir = os.path.dirname(__file__)
        credential_file = os.path.join(main_dir,'../res/secrets/pe-cts-poc-0bbb0b044fea.json')
        tenant_client = talent_v4beta1.TenantServiceClient.from_service_account_file(credential_file)
    except Exception as e:
        logging.exception(e)

if __name__ == '__main__':
    main()