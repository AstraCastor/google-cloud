from google.cloud import talent_v4beta1
import os
import sys
import logging
import argparse
from datetime import datetime
import src.db

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

class Tenant:

    def __init__(self):
        try:
            main_dir = os.path.dirname(__file__)
            credential_file = os.path.join(main_dir,'../res/secrets/pe-cts-poc-0bbb0b044fea.json')
            logger.info("credentials: {}".format(credential_file))
            self._tenant_client = talent_v4beta1.TenantServiceClient.from_service_account_file(credential_file)
            logger.info("Tenant client created: {}".format(self._tenant_client))
            self._db_connection = src.db.cts_db().connection()
            logger.info("DB Connection obtained: {}".format(self._db_connection))
        except Exception as e:
            logging.exception("Error instantiating Tenant. Message: {}".format(e))

    def client(self):
        return self._tenant_client

    def get_all_tenants(self,project_id):
        """ Get a CTS tenant name by external name.
        Args:
            talent_client: an instance of TalentServiceClient()
            project_id: project where the tenant will be created - string
            tenant_name: unique ID of the tenant - string
        Returns:
            an instance of Tenant or None if tenant was not found.
        """
        db = self._db_connection
        client = self._tenant_client
        try:
            db.execute("SELECT * FROM tenant where project_id = \'{}\'".format(project_id))
            rows = db.fetchall()
            tenants_from_db = [{"external_id":row[0],"name":row[1],"project_id":row[2],   \
                    "suspended":row[3]} for row in rows]
            return tenants_from_db
        except Exception as e:
            print("Error when listing tenants for project {}. Message: {}".format(project_id,e))
            raise

    def get_tenant_by_external_id(self,external_id,project_id=None):
        """ Get a CTS tenant name by external name.
        Args:
            talent_client: an instance of TalentServiceClient()
            project_id: project where the tenant will be created - string
            tenant_name: unique ID of the tenant - string
        Returns:
            an instance of Tenant or None if tenant was not found.
        """
#        try:
#            parent = tenant_client.project_path(project_id)
            # for tenant in get_all_tenants(project_id):
            #     if tenant.external_id == tenant_name:
            #         return tenant
        db = self._db_connection
        client = self._tenant_client
        try:
            if project_id is not None:
                db.execute("SELECT external_id,tenant_name FROM tenant where external_id = \'{}\' and project_id=\'{}\'"    \
                    .format(external_id,project_id))
            else:
                db.execute("SELECT external_id,tenant_name FROM tenant where external_id = \'{}\'".format(external_id))
            tenant_from_db = db.fetchall()
            if tenant_from_db == []:
                return None
            else:
                return {"external_id":tenant_from_db[0][0],"name":tenant_from_db[0][1],"project_id":tenant_from_db[0][2],   \
                    "suspended":tenant_from_db[0][3]}
        except Exception as e:
            logger.error("Error getting tenant by name {}. Message: {}".format(external_id,e))
            raise 

    def create_tenant(self,external_id,project_id):
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
            existing_tenant = self.get_tenant_by_external_id(external_id)
            if existing_tenant is None:
                parent = client.project_path(project_id)
                tenant_object = {'external_id':external_id}
                new_tenant = client.create_tenant(parent,tenant_object)
                logger.info("Query:INSERT INTO tenant (external_id,tenant_name,project_id,suspended,create_time) VALUES ({},{},{:d},{})".format(new_tenant.external_id,new_tenant.name,1,datetime.now()))
                db.execute("INSERT INTO tenant (external_id,tenant_name,project_id,suspended,create_time) VALUES ('{}','{}','{}',{:d},'{}')".format(new_tenant.external_id,new_tenant.name,project_id,1,datetime.now()))
                db.close()
                logger.info("Tenant {} created.\n{}".format(external_id,new_tenant))
                return new_tenant
            else:
                logger.error("Tenant {} already exists.\n{}".format(external_id,existing_tenant))
                return None
        except Exception as e:
            print("Error creating tenant {}: {}".format(external_id,e))
            self.delete_tenant(external_id,project_id)
            raise

    def delete_tenant(self,external_id,project_id,skip_check=False):
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
            existing_tenant = self.get_tenant_by_external_id(external_id,project_id)
            if existing_tenant is not None: 
                client.delete_tenant(existing_tenant.name)
                logger.info("Tenant {} deleted.".format(external_id))
            else:
                logger.error("Tenant {} does not exist.".format(external_id,existing_tenant))
                return None
        except Exception as e:
            print("Error deleting tenant {}: {}".format(external_id,e))
            raise

    def sync_tenant(self, tenant_client,db_connection,project_id):
        try:
            db_connection.execute("SELECT sync_time FROM metadata where table='tenant'")
            last_sync =  db_connection.fetchall()

            db_connection.execute("SELECT * FROM tenant")
            tenants_from_db = db_connection.fetchall()
            print ("DB tenants: {}".format(tenants_from_db))
            
            parent = tenant_client.project_path(project_id)
            tenants_from_cloud = list(tenant_client.list_tenants(parent))
            # tenants_from_cloud = list(tenant for tenant in tenants)

            tenants_delta = [tenant for tenant in tenants_from_cloud if tenant['external_id'] not in tenants_from_db['external_id']]
            print ("Delta is: \n{}".format(tenants_delta))
            if tenants_delta is not None:
                for t in tenants_delta:
                    db_connection.execute("INSERT INTO tenant VALUES \({},{},{},{}\)".format(tenant_list[t].external_id,tenant_list[t].tenant_name,1,datetime.now()))
                    logging.info("Inserted tenant record for {}".format(tenant_list[t].external_id))

            return tenant_list
        except Exception as e:
            print("Error when getting tenant by external ID. Message: {}".format(e))
            raise


    if __name__ == '__main__':
        Tenant()