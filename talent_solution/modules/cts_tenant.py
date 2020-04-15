from google.cloud import talent_v4beta1
import os
import sys
import logging
import argparse
from datetime import datetime
from . import cts_db

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
logger.info("Setting log level to {}".format(log_level))

class Tenant:

    def __init__(self):
        try:
            main_dir = os.path.dirname(__file__)
            credential_file = os.path.join(main_dir,'../res/secrets/pe-cts-poc-0bbb0b044fea.json')
            logger.debug("credentials: {}".format(credential_file))
            self._tenant_client = talent_v4beta1.TenantServiceClient.from_service_account_file(credential_file)
            logger.debug("Tenant client created: {}".format(self._tenant_client))
            self._db_connection = cts_db.DB().connection()
            logger.debug("Tenant db connection obtained: {}".format(self._db_connection))
        except Exception as e:
            logging.exception("Error instantiating Tenant. Message: {}".format(e))
    

    def client(self):
        return self._tenant_client

    def get_tenant(self,project_id,external_id=None,all=False):
        """ Get CTS tenant by name or get all CTS tenants by project.
        Args:
            talent_client: an instance of TalentServiceClient()
            project_id: project where the tenant will be created - string
            tenant_name: unique ID of the tenant - string
        Returns:
            an instance of Tenant or None if tenant was not found.
        """
        logger.debug("Get tenant called.")
        db = self._db_connection
        client = self._tenant_client
        try:
            if external_id is not None:
                if all:
                    logging.exception("Conflicting arguments: --external_id and --all are mutually exclusive.")
                    raise ValueError
                elif project_id is not None:
                    db.execute("SELECT external_id,tenant_name,project_id FROM tenant where tenant_key = '{}'".format(project_id+"-"+external_id))
                rows = db.fetchall()
                if rows == []:
                    return None
                else:
                    logger.debug("db lookup:{}".format(rows))
                    tenant = client.get_tenant(rows[0][1])
            elif all:
                parent = client.project_path(project_id)
                tenant = [t for t in client.list_tenants(parent)]
            return tenant
        except Exception as e:
            logger.error("Error getting tenant by name {}. Message: {}".format(external_id,e))
            raise        

    def create_tenant(self,project_id,external_id):
        """ Create a CTS tenant by external name.
        Args:
            project_id: project where the tenant will be created - string
            external_id: unique ID of the tenant - string
        Returns:
            an instance of Tenant or None if tenant was not created.
        """
        db = self._db_connection
        client = self._tenant_client
        try:
            existing_tenant = self.get_tenant(project_id=project_id,external_id=external_id)
            if existing_tenant is None:
                parent = client.project_path(project_id)
                tenant_object = {'external_id':external_id}
                new_tenant = client.create_tenant(parent,tenant_object)
                logger.debug("Query:INSERT INTO tenant (tenant_key,external_id,tenant_name,project_id,suspended,create_time) VALUES ('{}','{}','{}','{}','{:d}','{}')".format(project_id+"-"+external_id,new_tenant.external_id,new_tenant.name,project_id,1,datetime.now()))
                db.execute("INSERT INTO tenant (tenant_key,external_id,tenant_name,project_id,suspended,create_time) VALUES ('{}','{}','{}','{}',{:d},'{}')".format(project_id+"-"+external_id,new_tenant.external_id,new_tenant.name,project_id,1,datetime.now()))
                logger.info("Tenant {} created.\n{}".format(external_id,new_tenant))
                return new_tenant
            else:
                logger.error("Tenant {} already exists.\n{}".format(external_id,existing_tenant))
                return None
        except Exception as e:
            print("Error creating tenant {}: {}".format(external_id,e))
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
        db = self._db_connection
        client = self._tenant_client
        try:
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
                db.execute("DELETE FROM tenant where external_id = '{}'".format(existing_tenant.external_id))
                logger.info("Tenant {} deleted.".format(external_id))
            else:
                logger.error("Tenant {} does not exist.".format(external_id,existing_tenant))
                return None
        except Exception as e:
            print("Error deleting tenant {}: {}".format(external_id,e))
            raise

    def sync_tenants(self,project_id):
        db = self._db_connection
        client = self._tenant_client
        try:
            db.execute("SELECT sync_time FROM metadata where table='tenant'")
            last_sync =  db.fetchall()

            db.execute("SELECT * FROM tenant")
            tenants_from_db = db.fetchall()
            print ("DB tenants:\n {}".format(tenants_from_db))
            
            parent = client.project_path(project_id)
            tenants_from_cloud = list(client.list_tenants(parent))
            # tenants_from_cloud = list(tenant for tenant in tenants)

            tenants_delta = [tenant for tenant in tenants_from_cloud if tenant['external_id'] not in tenants_from_db['external_id']]
            print ("Delta is: \n{}".format(tenants_delta))
            if tenants_delta is not None:
                for t in tenants_delta:
                    db.execute("INSERT INTO tenant VALUES \({},{},{},{}\)".format(t.external_id,t.tenant_name,1,datetime.now()))
                    logging.info("Inserted tenant record for {}".format(t.external_id))
        except Exception as e:
            print("Error when syncing tenants in project {}. Message: {}".format(project_id, e))
            raise

    if __name__ == '__main__':
        Tenant()