from job_managers.flow import Flow

project_config = {}

# Dataproc cluster configuration - # https://cloud.google.com/dataproc/docs/guides/create-cluster
cluster_config = {
    "cluster_name": "spark_demo_job",
    "numMasterInstances": 1,
    "MasterMachineType": "n1-standard-1",
    "numWorkerInstances": 2,
    "WorkerMachineType": "n1-standard-1",
}

# Dataproc job configuration
job_config = {
    "job_name": "spark_demo_job",
    "args": []
}

spark_demo_job = Flow(project_config, cluster_config, job_config)
