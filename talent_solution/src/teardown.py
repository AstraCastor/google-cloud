from google.cloud import talent_v4beta1

import sys
import json
import logging

def create_tenant(project_id,tenant_external_id):
    """ Create a CTS tenant for the first time setup.
    Args:
        project_id: project where the tenant will be created - string
        tenant_external_id: unique ID of the tenant - string
    Returns:
        name - Name of the tenant assigned by CTS - string
    """
    talent_client = talent_v4beta1.TenantServiceClient()
    try:
        if talent_client.tenant_path(project_id,tenant_external_id) is None:
            parent = talent_client.project_path(project_id)
            tenant = {'external_id':tenant_external_id}
            resp = talent_client.create_tenant(parent,tenant)
            print ("Tenant Created - External ID: {}, Tenant Name: {}".format(tenant_external_id,resp.name))
    except Exception as e:
        print("Error when creating client. Message: {}".format(e))
        raise
