import os
from google.cloud import storage
from settings import project_config

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = project_config["service_account_path"]
storage_client = storage.Client()
