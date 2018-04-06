#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import os
import googleapiclient.discovery
from settings import project_config


class ClusterManager(object):

    def __init__(self, project_config, cluster_config):
        self.dataproc = self.get_client()
        self.project_config = project_config
        self.cluster_config = cluster_config

    def get_region_from_zone(self, zone):
        try:
            region_as_list = zone.split('-')[:-1]
            return '-'.join(region_as_list)
        except (AttributeError, IndexError, ValueError):
            raise ValueError('Invalid zone provided, please check your input.')

    # [START create_cluster]
    def create_cluster(self):
        project = self.project_config.get('project', 'fynd-1088')
        region = self.project_config.get('region', 'asia-south1')
        zone = self.project_config.get('zone', 'asia-south1-a')
        cluster_name = self.cluster_config.get('cluster_name', 'test')

        print('Creating cluster for {} in zone - {}'.format(
            project, zone
        ))
        zone_uri = \
            'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                project, zone)

        cluster_data = {
            'projectId': project,
            'clusterName': self.cluster_config.get('cluter_name', cluster_name),
            'config': {
                'gceClusterConfig': {
                    'zoneUri': zone_uri
                },
                'masterConfig': {
                    'numInstances': self.cluster_config.get('numMasterInstances', 1),
                    'machineTypeUri': self.cluster_config.get('MasterMachineType', 'n1-standard-1')
                },
                'workerConfig': {
                    'numInstances': self.cluster_config.get('numWorkerInstances', 2),
                    'machineTypeUri': self.cluster_config.get('WorkerMachineType', 'n1-standard-1')
                }
            }
        }
        result = self.dataproc.projects().regions().clusters().create(
            projectId=project,
            region=region,
            body=cluster_data).execute()
        return result

    # [END create_cluster]

    def wait_for_cluster_creation(self):
        print('Waiting for cluster creation...')

        project = self.project_config.get('project', 'fynd-1088')
        region = self.project_config.get('region', 'asia-south1')
        cluster_name = self.cluster_config.get('cluster_name', 'test')

        while True:
            result = self.dataproc.projects().regions().clusters().list(
                projectId=project,
                region=region).execute()
            cluster_list = result['clusters']
            cluster = [c
                       for c in cluster_list
                       if c['clusterName'] == cluster_name][0]
            if cluster['status']['state'] == 'ERROR':
                raise Exception(result['status']['details'])
            if cluster['status']['state'] == 'RUNNING':
                print("Cluster created.")
                break

    # [START list_clusters_with_detail]
    def list_clusters_with_details(self, dataproc, project, region):
        result = dataproc.projects().regions().clusters().list(
            projectId=project,
            region=region).execute()
        cluster_list = result['clusters']
        for cluster in cluster_list:
            print("{} - {}"
                .format(cluster['clusterName'], cluster['status']['state']))
        return result

    # [END list_clusters_with_detail]

    def get_cluster_id_by_name(self, cluster_list, cluster_name):
        """Helper function to retrieve the ID and output bucket of a cluster by
        name."""
        cluster = [c for c in cluster_list if c['clusterName'] == cluster_name][0]
        return cluster['clusterUuid'], cluster['config']['configBucket']

    # [START submit_pyspark_job]
    def submit_pyspark_job(self, job_config):
        """Submits the Pyspark job to the cluster, assuming `filename` has
        already been uploaded to `bucket_name`"""

        job_config = {
            "args": [
                "gs://fynd-new-bucket/a/README.txt"
            ],
            "pythonFileUris": [
                job_config.get('files.zip')
            ],
            "mainPythonFileUri": job_config.get('main.py')
        }

        project = self.project_config.get('project', 'fynd-1088')
        region = self.project_config.get('region', 'asia-south1')
        cluster_name = self.cluster_config.get('cluster_name', 'test')

        pythonFileUris = job_config.get('pythonFileUris')
        mainPythonFileUri = job_config.get('mainPythonFileUri')
        subm_args = job_config.get('args', [])

        job_details = {
            'projectId': project,
            'job': {
                'placement': {
                    'clusterName': cluster_name
                },
                "pysparkJob": {
                    "mainPythonFileUri": mainPythonFileUri,
                    "args": subm_args,
                    "pythonFileUris": pythonFileUris
                }
            }
        }
        result = self.dataproc.projects().regions().jobs().submit(
            projectId=project,
            region=region,
            body=job_details).execute()
        job_id = result['reference']['jobId']
        print('Submitted job ID {}'.format(job_id))
        return job_id

    # [END submit_pyspark_job]

    # [START delete]
    def delete_cluster(self):

        project = self.project_config.get('project', 'fynd-1088')
        region = self.project_config.get('region', 'asia-south1')
        cluster_name = self.cluster_config.get('cluster_name', 'test')

        print('Tearing down cluster - {}'.format(cluster_name))

        result = self.dataproc.projects().regions().clusters().delete(
            projectId=project,
            region=region,
            clusterName=cluster_name).execute()

        return result

    # [END delete]

    # [START wait]
    def wait_for_job(self, job_id):
        print('Waiting for job to finish...')

        project = self.project_config.get('project', 'fynd-1088')
        region = self.project_config.get('region', 'asia-south1')
        cluster_name = self.cluster_config.get('cluster_name', 'test')

        while True:
            result = self.dataproc.projects().regions().jobs().get(
                projectId=project,
                region=region,
                jobId=job_id).execute()
            # Handle exceptions
            if result['status']['state'] == 'ERROR':
                raise Exception(result['status']['details'])
            elif result['status']['state'] == 'DONE':
                print('Job finished.')
                return result

    # [END wait]

    # [START get_client]
    def get_client(self):
        """Builds an http client authenticated with the service account
        credentials."""
        dataproc = googleapiclient.discovery.build('dataproc', 'v1')
        return dataproc
