from phidata.asset.aws.s3 import S3Object
from phidata.infra.aws.resource.emr.cluster import EmrCluster
from phidata.task import TaskArgs
from phidata.workflow import Workflow
from phidata.utils.log import logger

from workspace.config import data_vpc_stack, data_s3_bucket

##############################################################################
## An example data pipeline that creates an EMR cluster,
## runs a job and then tears it down
##############################################################################

# Cluster name
cluster_name = "test-emr"
# Cluster log destination
emr_cluster_logs = S3Object(
    key=f"{cluster_name}/logs/",
    bucket=data_s3_bucket,
)
# Step 1: Define the EmrCluster
test_emr_cluster = EmrCluster(
    name=cluster_name,
    release_label="emr-6.5.0",
    log_uri=emr_cluster_logs.uri,
    applications=[
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterHub"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Presto"},
        {"Name": "Spark"},
    ],
    instances={
        "InstanceGroups": [
            {
                "Name": "DriverNodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m3.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "WorkerNodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m3.xlarge",
                "InstanceCount": 2,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp2",
                                "SizeInGB": 8,
                            },
                            "VolumesPerInstance": 2,
                        },
                    ],
                    "EbsOptimized": True,
                },
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    job_flow_role="EMR_EC2_DefaultRole",
    service_role="EMR_DefaultRole",
    tags=[
        {
            "Key": "environment",
            "Value": "test",
        },
        {
            "Key": "for-use-with-amazon-emr-managed-policies",
            "Value": "true",
        },
    ],
)

# Step 2: Create the workflow
emr = Workflow(name="emr_test")


# 2.1: Define typed inputs for our workflow
class EmrWorkflowArgs(TaskArgs):
    cluster: EmrCluster = test_emr_cluster


# 2.2: Write a task to create the cluster
@emr.task()
def create_emr_cluster(**kwargs) -> bool:
    args = EmrWorkflowArgs.from_kwargs(kwargs)
    # update the vpc subnet ids

    create_success = args.cluster.create()
    if create_success:
        logger.info("Create EmrCluster: success")
    else:
        logger.error("Create EmrCluster: failed")
    return create_success


# 2.3: Write a task to add/run the job
@emr.task()
def add_job(**kwargs) -> bool:
    args = EmrWorkflowArgs.from_kwargs(kwargs)

    calculate_pi_job = {
        "Name": "calculate_pi",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        },
    }

    logger.info(f"Adding job:\n{calculate_pi_job}")
    return True


# 2.4: Write a task to delete the cluster
@emr.task()
def delete_emr_cluster(**kwargs) -> bool:
    args = EmrWorkflowArgs.from_kwargs(kwargs)

    delete_success = args.cluster.delete()
    if delete_success:
        logger.info("Delete EmrCluster: success")
    else:
        logger.error("Delete EmrCluster: failed")
    return delete_success
