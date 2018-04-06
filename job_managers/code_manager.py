import os
from os.path import basename
import zipfile
from google.cloud import storage
from datetime import datetime
from settings import project_config

bucket_name = project_config.get('gcs_bucket_name')
from job_managers import storage_client


class CodeManager(object):

    def __init__(self, job_name):
        self.job_name = job_name

    # Creates the new bucket
    # bucket = storage_client.create_bucket(bucket_name)
    def upload_blob(self, bucket_name, source_file_name, destination):
        """Uploads a file to the bucket."""
        storage_client = storage.Client()
        file_name = source_file_name.split('/')[-1]
        bucket = storage_client.get_bucket(bucket_name)
        destination_blob_name = '{}/{}/{}'.format(self.job_name,
                                                  destination,
                                                  file_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        # print('File {} uploaded to {}.'.format(
        #     source_file_name,
        #     destination_blob_name))

        return blob.name

    def zipdir(self, path, ziph):
        # ziph is zipfile handle
        for root, dirs, files in os.walk(path):
            for file in files:
                ziph.write(os.path.join(root, file))

    def prepare_upload_file_names(self):
        upload_file_paths = {}

        current_file_path = os.path.realpath(__file__)
        job_runner_dir = os.path.dirname(os.path.abspath(__file__))
        jobs_dir = job_runner_dir.replace('/job_managers', '/jobs')
        temp_dir = job_runner_dir.replace('/job_managers', '/temp')

        zip_fie_name = '{}/{}.zip'.format(temp_dir, self.job_name)
        zipf = zipfile.ZipFile(zip_fie_name, 'w', zipfile.ZIP_DEFLATED)
        requested_job_location = '{}/{}'.format(jobs_dir, self.job_name)
        shell_file_path = requested_job_location + '/setup.sh'
        exec_file_path = requested_job_location + '/spark_files/main.py'
        spark_files_path = requested_job_location + '/spark_files'
        zip_file_path = '{}'.format(zip_fie_name)

        upload_file_paths['main.py'] = exec_file_path
        upload_file_paths['setup.sh'] = shell_file_path
        upload_file_paths['files.zip'] = zip_file_path

        # Add Spark script path to tar.gz file
        files = os.listdir(spark_files_path)

        for f in files:
            fname_path = '{}/{}'.format(spark_files_path, f)
            zipf.write(fname_path, basename(fname_path))

        zipf.close()
        return upload_file_paths

    def generate_job_name(self, job_name, job_creation_time_string):
        job_name = "{}_{}".format(job_name, job_creation_time_string)
        return job_name

    def upload_files_to_gcs(self):

        job_creation_time_string = datetime.now().strftime("%Y%m%d.%H%M%S.%f")
        upload_these_files = self.prepare_upload_file_names()

        prepared_file_json = {}

        for each_file, each_file_path in upload_these_files.items():
            gcs_file_path = self.upload_blob(bucket_name, each_file_path,
                                             self.generate_job_name(self.job_name, job_creation_time_string))
            prepared_file_json[each_file] = 'gs://{}/{}'.format(bucket_name, gcs_file_path)

        return prepared_file_json
