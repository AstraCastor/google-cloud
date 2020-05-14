from google.cloud import talent_v4beta1
from google.cloud.talent_v4beta1.types import Tenant as CTS_Tenant
from google.api_core.exceptions import AlreadyExists, NotFound, GoogleAPICallError, RetryError

import os
import sys
import logging
import inspect
import re
from datetime import datetime
from modules import cts_db
from conf import config as config

#Get the root logger
logger = logging.getLogger()

class Tenant:

    def __init__(self,external_id=None,name=None):
        try:
            self.external_id = external_id
            self.name = name
            logging.debug("Tenant instantiated.")
        except Exception as e:
            logging.error("Error instantiating Tenant. Message: {}".format(e),exc_info=config.LOGGING['traceback'])
    
    def client(self):
        try:
            credential_file = config.APP['secret_key']
            logger.debug("credentials: {}".format(credential_file))
            _tenant_client = talent_v4beta1.TenantServiceClient.from_service_account_file(credential_file)
            logger.debug("Tenant client created: {}".format(_tenant_client))
            return _tenant_client
        except Exception as e:
            logging.error("Error instantiating Tenant client. Message: {}".format(e),exc_info=config.LOGGING['traceback'])

    def get_tenant(self,project_id,external_id=None,all=False,scope="full"):
        """ Get CTS tenant by name or get all CTS tenants by project.
        Args:
            talent_client: an instance of TalentServiceClient()
            project_id: project where the tenant will be created - string
            tenant_name: unique ID of the tenant - string
        Returns:
            an instance of Tenant or None if tenant was not found.
        """
        logger.debug("CALLED: get_tenant({},{},{},{} by {})".format(project_id,external_id,all,scope,inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()

            if external_id is not None:
                if all:
                    logging.exception("Conflicting arguments: --external_id and --all are mutually exclusive.")
                    raise ValueError
                elif project_id is not None:
                    logger.debug("Tenant key to be retrieved: {}".format(project_id+"-"+external_id))
                    db.execute("SELECT distinct external_id,tenant_name,project_id FROM tenant where tenant_key = ?",(project_id+"-"+external_id,))
                rows = db.fetchall()
                if rows == []:
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    if scope=='limited':
                        tenant = CTS_Tenant()
                        tenant.external_id = rows[0][0]
                        tenant.name = rows[0][1]
                    else:
                        tenant = client.get_tenant(rows[0][1])
            elif all:
                parent = client.project_path(project_id)
                tenant = [t for t in client.list_tenants(parent)]
            return tenant       
        except Exception as e:
            logger.error("Error getting tenant by name {}. Message: {}".format(external_id,e),exc_info=config.LOGGING['traceback'])
            raise        

    def create_tenant(self,project_id,external_id):
        """ Create a CTS tenant by external name.
        Args:
            project_id: project where the tenant will be created - string
            external_id: unique ID of the tenant - string
        Returns:
            an instance of Tenant or None if tenant was not created.
        """
        logger.debug("CALLED: create_tenant({},{} by {})".format(project_id,external_id,inspect.currentframe().f_back.f_code.co_name))
        try:
            client = self.client()
            existing_tenant = self.get_tenant(project_id=project_id,external_id=external_id)
            if existing_tenant is None:
                parent = client.project_path(project_id)
                tenant_object = {'external_id':external_id}
                new_tenant = client.create_tenant(parent,tenant_object)
                if cts_db.persist_to_db(new_tenant,project_id):
                    logger.info("Tenant {} created.\n{}".format(external_id,new_tenant))
                    return new_tenant
                else:
                    raise Exception("Error when persisting tenant {} to DB.".format(external_id))
            else:
                logger.warning("Tenant {} already exists.\n{}".format(external_id,existing_tenant))
                print("Tenant {} already exists.\n{}".format(external_id,existing_tenant))
                return None

        except AlreadyExists as e:                    
            logger.warning("Tenant {} exists in server. Creating local record..".format(external_id))
            # Sync with DB if it doesn't exist in DB
            logger.warning("Local DB out of sync. Syncing local db..")
            sync_tenant = CTS_Tenant()
            sync_tenant.name = re.search("^Tenant (.*) already exists.*$",e.message).group(1)
            sync_tenant.external_id = external_id
            if cts_db.persist_to_db(sync_tenant,project_id=project_id):
                logger.warning("Company {} record synced to DB.".format(external_id))                        
            else:
                raise Exception("Error when syncing tenant {} to DB.".format(sync_tenant.external_id)) 
        except Exception as e:
            logger.error("Error creating tenant {}: {}".format(external_id,e),exc_info=config.LOGGING['traceback'])
            self.delete_tenant(project_id,external_id,forced=True)
            raise

    def delete_tenant(self,project_id,external_id,forced=False):
        """ Delete a CTS tenant by external name.
        Args:
            project_id: project where the tenant will be created - string
            external_id: unique ID of the tenant - string
        Returns:
            None - If tenant is not found.
        """
        logger.debug("CALLED: delete_tenant({},{},{} by {})".format(project_id,external_id,forced,inspect.currentframe().f_back.f_code.co_name))
        try:
            db = cts_db.DB().connection
            client = self.client()
            if forced:
                all_tenants = self.get_tenant(project_id=project_id,all=True)
                for tenant in all_tenants:
                    if tenant.external_id == external_id:
                        existing_tenant = tenant
                        break
                    else:
                        existing_tenant = None
            else:
                existing_tenant = self.get_tenant(project_id=project_id,external_id=external_id)
                logger.debug("Existing Tenant? {}".format(existing_tenant))
            if existing_tenant is not None:
                logger.info("Deleting tenant name: {}".format(existing_tenant.external_id))
                client.delete_tenant(existing_tenant.name)
                # db.execute("DELETE FROM tenant where external_id = '{}'".format(existing_tenant.external_id))
                db.execute("DELETE FROM tenant where external_id = ?",(existing_tenant.external_id,))
                logger.info("Tenant {} deleted.".format(external_id))
            else:
                logger.error("Tenant {} does not exist.".format(external_id,existing_tenant),exc_info=config.LOGGING['traceback'])
                return None
        except Exception as e:
            logger.error("Error deleting tenant {}: {}".format(external_id,e),exc_info=config.LOGGING['traceback'])
            raise

    if __name__ == '__main__':
        Tenant()