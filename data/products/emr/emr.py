from phidata.product import DataProduct
from phidata.infra.aws.resource.emr.cluster import EmrCluster
from phidata.workflow.aws.emr.create_cluster import CreateEmrCluster
from phidata.workflow.aws.emr.delete_cluster import DeleteEmrCluster
from phidata.utils.log import logger

from workspace.config import data_vpc_stack

##############################################################################
## An example data pipeline that creates an EMR cluster
##############################################################################

cluster_subnets = []
try:
    private_subnets = data_vpc_stack.get_private_subnets()
    public_subnets = data_vpc_stack.get_public_subnets()
    if private_subnets:
        cluster_subnets.extend(private_subnets)
    if public_subnets:
        cluster_subnets.extend(public_subnets)
except Exception as e:
    logger.error("Could not get subnets")

calculate_pi_job = {
    "Name": "calculate_pi",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
    },
}

test_emr_cluster = EmrCluster(
    name="test-emr-cluster",
    release_label="emr-6.5.0",
    applications=[
        {"Name": "Spark"},
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterHub"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Livy"},
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
        "Ec2SubnetIds": cluster_subnets,
    },
    steps=[calculate_pi_job],
    job_flow_role="EMR_EC2_DefaultRole",
    service_role="EMR_DefaultRole",
    tags=[
        {
            "Key": "environment",
            "Value": "test",
        },
    ],
)

create = CreateEmrCluster(cluster=test_emr_cluster)
delete = DeleteEmrCluster(cluster=test_emr_cluster)

# Create a DataProduct for these tasks
emr = DataProduct(
    name="emr",
    workflows=[
        create,
        delete,
    ],
)
