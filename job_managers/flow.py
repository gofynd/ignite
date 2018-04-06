from datetime import datetime
from code_manager import CodeManager
from cluster_manager import ClusterManager
from job_managers import slacker
import io
from subprocess import Popen, PIPE
import os


class Flow(object):
    def __init__(self, project_config, cluster_config, job_config):
        self.project_config = project_config
        self.cluster_config = cluster_config
        self.cluster_config['cluster_name'] = ''.join(e for e in cluster_config.get('cluster_name') if e.isalnum())
        self.job_config = job_config
        self.job_name = job_config.get('job_name')

    def run(self, is_local="false"):
        if is_local == "true":
            self.run_local()
            return

        start_time = datetime.now()
        end_time = datetime.now()
        time_diff = end_time - start_time
        total_seconds = time_diff.total_seconds()

        slacker.do_slack("Dataproc Cluster Status", self.job_name,
                         [{"title": "Cluster Status", "value": "Intiating"}], 0)

        dpm = ClusterManager(self.project_config, self.cluster_config)
        dpm.create_cluster()
        dpm.wait_for_cluster_creation()

        slacker.do_slack("Dataproc Cluster Status", self.job_name,
                         [{"title": "Cluster Status", "value": "Created"}], 0)

        cm = CodeManager(self.job_config.get('job_name'))
        job_config_gcs = cm.upload_files_to_gcs()

        job_config_gcs.update(self.job_config)
        job_id = dpm.submit_pyspark_job(job_config_gcs)

        slacker.do_slack("Dataproc Job Status", self.job_name,
                         [{"title": "Job Status", "value": "Submitted and Running"}], 0)

        dpm.wait_for_job(job_id)

        slacker.do_slack("Dataproc Job Status", self.job_name,
                         [{"title": "Job Status", "value": "Completed"}], 0)

        dpm.delete_cluster()

        slacker.do_slack("Dataproc Cluster Status", self.job_name,
                         [{"title": "Cluster Status", "value": "Deleting"}], 0)


    def exec_shell_and_wait(self, command):
        pipe = Popen(command.split(" "), stdout=PIPE)
        for line in iter(pipe.stdout.readline, ''):
            print(line)


    def run_local(self):
        start_time = datetime.now()
        end_time = datetime.now()
        time_diff = end_time - start_time
        total_seconds = time_diff.total_seconds()

        job_runner_dir = os.path.dirname(os.path.abspath(__file__))
        jobs_dir = job_runner_dir.replace('/job_managers', '/jobs')
        requested_job_location = '{}/{}'.format(jobs_dir, self.job_name)

        docker_image_name = 'ignite_spark_{}'.format(self.job_name)

        print("JOB NAME - {} - Docker File Creation Initiated".format(self.job_name))

        with open('dockerfile-base') as rf:
            setup_content = rf.read()
            setup_content = setup_content.replace("{{job_dir}}", "jobs/{}".format(self.job_name))
            with open('Dockerfile', "w") as wf:
                wf.write(setup_content)

        print("JOB NAME - {} - Docker Image Creation Initiated".format(self.job_name))

        self.exec_shell_and_wait('docker build -t {} .'.format(docker_image_name))

        print("JOB NAME - {} - Docker Creating Container and Running Job".format(self.job_name))
        self.exec_shell_and_wait(
            'docker run -t --name spark_container --volume={}:/tmp/job --rm --publish=9999:80 {} python /tmp/job/spark_files/main.py'.format(
                requested_job_location, docker_image_name))

        print("JOB NAME - {} - Docker Job Fininshed".format(self.job_name))
