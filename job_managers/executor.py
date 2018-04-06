import os, sys

sys.path.extend([os.getcwd()])

from optparse import OptionParser
from jobs.spark_demo_job import spark_demo_job

registered_job_flow = {
    'spark_demo_job': spark_demo_job,
}

parser = OptionParser()
parser.add_option("-j", "--job", help="Name of SparkJob")
parser.add_option("-d", "--docker_mode", help="Run SparkJob on docker mode", default="false")
(options, args) = parser.parse_args()

flow_name = options.job
is_local = options.docker_mode

if flow_name in registered_job_flow:
    flow_executor = registered_job_flow.get(flow_name)
    flow_executor.run(is_local)
else:
    raise Exception("Flow is not registered. Register your SparkJob flow inside job_manager/flow.py")
