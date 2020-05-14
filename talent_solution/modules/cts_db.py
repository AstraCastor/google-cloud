import sqlite3
import logging
import sys
import os
from datetime import datetime
import re

from conf import config
from modules.cts_errors import CTSSchemaError
from google.cloud.talent_v4beta1.types import Job as CTS_Job
from google.cloud.talent_v4beta1.types import Company as CTS_Company
from google.cloud.talent_v4beta1.types import Tenant as CTS_Tenant


#General logging config
logger = logging.getLogger()


class DB():

    def __init__(self):
        try:
            # __connection = sqlite3.connect("./res/cts.db",isolation_level=None)
            __connection = sqlite3.connect(config.DATABASE['file'],isolation_level=None)
            self.__cursor = __connection.cursor()
            logger.debug("CTS Database connected.")
            self.check_cts_schema(self.__cursor)
        except ConnectionError as e:
            logger.error("Error when opening DB connection. Message:{}".format(e),exc_info=config.LOGGING['traceback'])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self.__cursor

    def cts_schema(self):
        sql_cts_table = {}

        sql_cts_table['metadata'] = "CREATE TABLE IF NOT EXISTS metadata (   \
                                        table_nm TEXT PRIMARY KEY, \
                                        sync_time INTEGER)"

        sql_cts_table['tenant'] = "CREATE TABLE IF NOT EXISTS tenant (  \
                                    tenant_key TEXT PRIMARY KEY,     \
                                    tenant_name TEXT NOT NULL,   \
                                    external_id TEXT,   \
                                    project_id TEXT NOT NULL,    \
                                    suspended INTEGER,  \
                                    create_time INTEGER \
                                    )"

        sql_cts_table['company'] = "CREATE TABLE IF NOT EXISTS company (  \
                                    company_key TEXT PRIMARY KEY, \
                                    external_id TEXT NOT NULL,   \
                                    company_name TEXT,   \
                                    tenant_name TEXT, \
                                    project_id TEXT NOT NULL,    \
                                    suspended INTEGER,  \
                                    create_time INTEGER    \
                                    )"

        sql_cts_table['job'] = "CREATE TABLE IF NOT EXISTS job (  \
                                    job_key TEXT PRIMARY KEY, \
                                    external_id TEXT NOT NULL,   \
                                    language_code TEXT NOT NULL, \
                                    job_name TEXT, \
                                    company_name TEXT NOT NULL,   \
                                    tenant_name TEXT, \
                                    project_id TEXT NOT NULL,    \
                                    suspended INTEGER,  \
                                    create_time INTEGER    \
                                    )"
        return sql_cts_table


    def check_cts_schema(self,cursor):
        try:
            schema = self.cts_schema()
            errors = []
            for table in schema:
                try:
                    cursor.execute("select True from {}".format(table))
                except sqlite3.OperationalError as e:
                    logger.error(e)
                    logger.warn("DB Check: {} is missing and will be recreated.".format(table)) 
                    self.create_cts_table(cursor,table)
                except Exception as e:
                    errors.append(e)
                else:
                    logger.debug("DB check: {} OK".format(table))
            if errors:
                #TODO: Create custom MultipleErrors
                raise CTSSchemaError(errors)
        except Exception as e:
            logger.error("Schema check failed. Message: {}".format(e),exc_info=config.LOGGING['traceback'])


    def create_cts_table(self,cursor,table=None):
        try:
            schema = self.cts_schema()
            if table is None:
                for t in schema:
                    cursor.execute(schema[t])
                    logger.debug("Table {} is created.".format(t))
            else:
                cursor.execute(schema[table])
                logger.debug("Table {} is created.".format(table))

        except Exception as e:
            logger.error("Error when creating table {}. Message: {}".format(table,e),exc_info=config.LOGGING['traceback'])


def persist_to_db(object,project_id,tenant_id=None,company_id=None):
    try:
        db = DB().connection
        if isinstance(object,CTS_Job):
            job = object
            external_id = job.requisition_id
            language = job.language_code
            company_name = job.company

            if project_id is not None:
                job_key = project_id
            else:
                logging.error("Missing arguments: project_id.",exc_info=config.LOGGING['traceback'])
                raise ValueError

            if tenant_id is not None:
                # Calculate the tenant_id part of the job_key to look up from the DB
                logger.debug("Tenant: {}\n".format(tenant_id))
                job_key = job_key + "-" + tenant_id
                tenant_name = re.search('(.*)\/jobs\/.*$',job.name).group(1)
            
            if company_id is not None:
                job_key = job_key + "-" +company_id
            else:
                logging.error("Missing arguments: company_id.",exc_info=config.LOGGING['traceback'])
                raise ValueError                

            if external_id is not None:
                job_key = job_key+"-"+external_id
            else:
                logging.error("Missing arguments: external_id.",exc_info=config.LOGGING['traceback'])
            
            if language is not None:
                job_key = job_key+"-"+language
            else:
                logging.error("Missing arguments: external_id.",exc_info=config.LOGGING['traceback'])

            logger.debug("Inserting record for job key:{}".format(job_key))
            logger.debug("Query: INSERT INTO job (job_key,external_id,language_code,job_name,company_name,tenant_name,project_id,suspended,create_time)    \
                VALUES ('{}','{}','{}','{}','{}','{}','{}','{:d}','{}')".format(job_key,external_id,language,job.name,company_name,\
                    tenant_name,project_id,0,datetime.now()))
            db.execute("INSERT INTO job (job_key,external_id,language_code,job_name,company_name,tenant_name,project_id,suspended,create_time) \
                VALUES (?,?,?,?,?,?,?,?,?)",(job_key,external_id,language,job.name,company_name,tenant_name,project_id,0,datetime.now()))
            logger.debug("Job req ID {} created in DB for company {}.".format(external_id,company_id))
            return True
        # Persisting a Company_Object
        elif isinstance(object,CTS_Company):
            company = object
            company_key = project_id+"-"+tenant_id+"-"+company.external_id if tenant_id is not None else project_id+"-"+company.external_id           
            tenant_name = re.search('(.*)\/companies\/.*$',company.name).group(1) if tenant_id is not None else ""
            logger.debug("Inserting record for company key:{}".format(company_key))
            logger.debug("Query: INSERT INTO company (company_key,external_id,company_name,tenant_name,project_id,suspended,create_time)    \
                VALUES ('{}','{}','{}','{}','{}','{:d}','{}')".format(company_key,company.external_id,    \
                    company.name,tenant_name,project_id,0,datetime.now()))
            db.execute("INSERT INTO company (company_key,external_id,company_name,tenant_name,project_id,suspended,create_time) \
                VALUES (?,?,?,?,?,?,?)",\
                (company_key,company.external_id,company.name,tenant_name,project_id,0,datetime.now()))        
            return True    
        elif isinstance(object,CTS_Tenant):
            tenant = object
            logger.debug("Query:INSERT INTO tenant (tenant_key,external_id,tenant_name,project_id,suspended,create_time) \
            VALUES ('{}','{}','{}','{}','{:d}','{}')".format(project_id+"-"+external_id,tenant.external_id,tenant.name,project_id,1,datetime.now()))
            db.execute("INSERT INTO tenant (tenant_key,external_id,tenant_name,project_id,suspended,create_time) \
            VALUES (?,?,?,?,?,?)",(project_id+"-"+external_id,tenant.external_id,tenant.name,project_id,1,datetime.now()))

    except Exception as e:
        logger.error("Error when persisting object in DB: {}. Message: {}".format(object,e))
        return False
