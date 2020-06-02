APP = {
    "gcp_project" : "<your_project_name>",
    "secret_key" : "res/secrets/your_secret_key",
    "default_language":"en-US", # BCP
    "request_metadata" : {"user_id":"test2","session_id":"test2"}
}

DATABASE = {
    "file" : "res/cts.db"
}

LOGGING = {
    "log_level" : "ERROR",
    "log_format" : "%(asctime)s %(levelname)-8s %(module)s:%(funcName)s: %(lineno)s %(message)s",
    "traceback" : "True"
}

BATCH_PROCESS = {
    "batch_size" : 2,
    "concurrent_batches":1,
    "api_qps_limit": 200    
}