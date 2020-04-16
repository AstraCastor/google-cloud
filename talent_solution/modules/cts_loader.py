from google.cloud import storage,tasks_v2
import os,logging,inspect

log_level = os.environ.get('LOG_LEVEL','INFO')
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger_format = logging.Formatter('%(asctime)s %(filename)s:%(lineno)s %(levelname)-8s %(message)s','%Y-%m-%d %H:%M',)

#Console Logging
console_logger = logging.StreamHandler()
console_logger.setLevel(log_level)
console_logger.setFormatter(logger_format)
logger.addHandler(console_logger)

#TODO:Add file logging


class LoadTask:
    def __init__(self,project_id=None,entity=None,operation=None,filename=None):
        try:
            self.project_id = project_id
            self.entity = entity
            self.operation = operation
            self.filename = filename
            main_dir = os.path.dirname(__file__)
            credential_file = os.path.join(main_dir,'../res/secrets/pe-cts-poc-0bbb0b044fea.json')
            logger.debug("credentials: {}".format(credential_file))
            self._task_client = tasks_v2.CloudTasksClient.from_service_account_file(credential_file)
            logger.debug("Task client created: {}".format(self._task_client))
            self._storage_client = storage.Client.from_service_account_json(json_credentials_path=credential_file)
            logger.debug("Storage client created: {}".format(self._storage_client))
            logger.info("Tasks instantiated.")
        except Exception as e:
                logging.exception("Error instantiating Cloud Task. Message: {}".format(e))


if __name__ == "__main__":
    LoadTask()