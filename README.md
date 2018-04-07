## Ignite - Dataproc Cluster Manager for Spark Jobs
Ignite helps you run and manage multiple Spark jobs with variable environment on Google Dataproc. For development phase, the same can be simulated on Docker. User can also configure slack to get cluster status updates.

### Features:
1. Manage Dataproc Cluster
2. Uploads Spark job code to GCS
3. Docker based Spark Job execution for development
4. Spark Job Submission
5. Slack Integration 

## Prerequisites:
1. Create Google Service Account credentials with BigQuery, Dataproc, Storage (gcp_creds.json) with and place inside Ignite root directory. [link](https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances)
2. Docker installed. [link](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

## Quick Start

### Project Config
Put appropriate values in settings.py file. Note that this step is not mandatory to run locally.
```python
"project_id": "your-project-id"
"region": "your-gcloud-region"
"zone": "your-gcloud-zone"
"gcs_bucket_name": "your-gcs-bucket-name"
"SLACK_TOKEN": "your-slack-token"
"SLACK_CHANNEL": "channel-name"
"SLACK_USERNAME": "alerting-bot-name"
"slack_enabled": False/True
```
Also put service account json file on project root directory named "gcp_creds.json".

### Required basic folder structure for spark job
```
job_sample_job/
├── __init__.py     # Contains config for dataproc cluster, job_cluster.
├── setup.sh        # Contains basic installation scripts for third party tools (including pip install lib).
└── spark_files     # Folder- Contains all python files
    ├── __init__.py  
    └── main.py      # Main Python file with PySpark code.
1 directory, 4 files    
```

### Add your configuration file (cluster_config, job_config) to `job_sample_job/__init__.py`
```python
from job_managers.flow import Flow

project_config = {}

cluster_config = {
    "cluster_name": "spark_demo_job",
    "numMasterInstances": 1,
    "MasterMachineType": "n1-standard-1",
    "numWorkerInstances": 3,
    "WorkerMachineType": "n1-standard-1",
}

job_config = {
    "job_name": "spark_demo_job",
    "args": ["gs://fynd-new-bucket/a/README.txt"]
}

spark_demo_job = Flow(project_config, cluster_config, job_config) # Register this flow object to job_managers/executor.py

``` 

### Currrently there is one demo job registered with this framework.
1. Spark Demo Job

To register new job, add job into `registered_job_flow` inside `job_managers/executor.py` file.
 
### Run

##### Command to run the job flow- 
1. To run SparkJob on Dataproc cluster - ```python job_managers/executor.py -j dataproc_sample_job```
2. To run SparkJob locally on Docker environment - ```python job_managers/executor.py -j dataproc_sample_job -d true```

