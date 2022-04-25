from pathlib import Path

from phidata.app.airflow import (
    AirflowWebserver,
    AirflowScheduler,
    AirflowWorker,
)
from phidata.app.databox import Databox
from phidata.app.postgres import PostgresDb, PostgresVolumeType
from phidata.app.redis import Redis, RedisVolumeType
from phidata.app.traefik import IngressRoute, LoadBalancerProvider
from phidata.infra.aws.config import AwsConfig
from phidata.infra.aws.create.iam.role import create_glue_iam_role
from phidata.infra.aws.resource.acm.certificate import AcmCertificate
from phidata.infra.aws.resource.cloudformation.stack import CloudFormationStack
from phidata.infra.aws.resource.ec2.volume import EbsVolume
from phidata.infra.aws.resource.eks.cluster import EksCluster
from phidata.infra.aws.resource.eks.node_group import EksNodeGroup
from phidata.infra.aws.resource.group import AwsResourceGroup
from phidata.infra.aws.resource.s3.bucket import S3Bucket
from phidata.infra.k8s.config import K8sConfig
from phidata.infra.k8s.create.core.v1.service import ServiceType
from phidata.infra.k8s.enums.image_pull_policy import ImagePullPolicy
from phidata.workspace import WorkspaceConfig

from workspace.whoami import (
    whoami_k8s_rg,
    whoami_service,
    whoami_port,
)

ws_key = "aws-dp"
aws_az = "us-east-1a"
aws_region = "us-east-1"
domain = "awsdataplatform.com"
ws_dir_path = Path(__file__).parent.resolve()

######################################################
## Configure AWS resources
######################################################

######################################################
## AWS Infrastructure
######################################################

## S3 buckets
# S3 bucket for storing data
data_s3_bucket = S3Bucket(
    name=f"phi-{ws_key}-data",
    acl="private",
)
# S3 bucket for storing logs
logs_s3_bucket = S3Bucket(
    name=f"phi-{ws_key}-logs",
    acl="private",
)

## EbsVolumes
# EbsVolume for prd-db
prd_db_volume = EbsVolume(
    name=f"prd-db-{ws_key}",
    size=1024,
    availability_zone=aws_az,
    # skip_delete=True,
)
# EbsVolume for airflow-db
airflow_db_volume = EbsVolume(
    name=f"airflow-db-{ws_key}",
    size=64,
    availability_zone=aws_az,
    # skip_delete=True,
)
# EbsVolume for prd-redis
prd_redis_volume = EbsVolume(
    name=f"prd-redis-{ws_key}",
    size=8,
    availability_zone=aws_az,
    # skip_delete=True,
)

## Iam Roles
# Iam Role for Glue crawlers
glue_iam_role = create_glue_iam_role(
    name=f"{ws_key}-glue-crawler-role",
    s3_buckets=[data_s3_bucket],
    # skip_delete=True,
)

# Vpc stack
data_vpc_stack = CloudFormationStack(
    name=f"{ws_key}-vpc",
    template_url="https://amazon-eks.s3.us-west-2.amazonaws.com/cloudformation/2020-10-29/amazon-eks-vpc-private-subnets.yaml",
    # skip_delete=True implies this resource will NOT be deleted with `phi ws down`
    # uncomment when workspace is production-ready
    # skip_delete=True,
)

## Databases
# airflow_prd_db_subnet_group = DbSubnetGroup(
#     name="airflow-db-subnet",
#     description="DBSubnetGroup for airflow-db",
#     vpc_stack=data_vpc_stack,
#     # skip_delete=True,
# )
# airflow_prd_db = DbCluster(
#     name="airflow-db",
#     engine="aurora-postgresql",
#     availability_zones=[aws_az],
#     engine_version="13.6",
#     vpc_stack=data_vpc_stack,
#     db_subnet_group=airflow_prd_db_subnet_group,
#     storage_encrypted=True,
#     secrets_file=ws_dir_path.joinpath("secrets/airflow_prd_db_secrets.yml"),
#     # skip_delete=True,
# )

# EKS cluster
data_eks_cluster = EksCluster(
    name=f"{ws_key}-cluster",
    vpc_stack=data_vpc_stack,
    # skip_delete=True,
)

# EKS cluster nodegroup
data_eks_nodegroup = EksNodeGroup(
    name=f"{ws_key}-ng",
    eks_cluster=data_eks_cluster,
    min_size=3,
    max_size=5,
    # skip_delete=True,
)

# ACM certificate for awsdataplatform.com
acm_certificate = AcmCertificate(
    name=domain,
    domain_name=domain,
    subject_alternative_names=[
        f"www.{domain}",
        f"traefik.{domain}",
        f"whoami.{domain}",
        f"airflow.{domain}",
        f"superset.{domain}",
        f"jupyter.{domain}",
        f"meta.{domain}",
        f"*.{domain}",
    ],
    store_cert_summary=True,
    certificate_summary_file=ws_dir_path.joinpath("aws", "acm", domain),
)

aws_resources = AwsResourceGroup(
    # This certificate is already created so we comment it out
    # acm_certificates=[acm_certificate],
    s3_buckets=[data_s3_bucket, logs_s3_bucket],
    volumes=[prd_db_volume, airflow_db_volume, prd_redis_volume],
    iam_roles=[glue_iam_role],
    cloudformation_stacks=[data_vpc_stack],
    eks_cluster=data_eks_cluster,
    eks_nodegroups=[data_eks_nodegroup],
)

prd_aws_config = AwsConfig(
    env="prd",
    resources=aws_resources,
)

######################################################
## Applications running on EKS Cluster
######################################################

# Prd database: A postgres instance for storing prd data
prd_db = PostgresDb(
    name="prd-db",
    volume_type=PostgresVolumeType.AWS_EBS,
    ebs_volume=prd_db_volume,
    secrets_file=ws_dir_path.joinpath("secrets/prd_postgres_secrets.yml"),
)
pg_db_connection_id = "pg_db"
prd_airflow_connections = {
    pg_db_connection_id: prd_db.get_db_connection_url_k8s()
}

# Airflow database: A postgres instance to use as the database for airflow
prd_airflow_db = PostgresDb(
    name="airflow-db",
    volume_type=PostgresVolumeType.AWS_EBS,
    ebs_volume=airflow_db_volume,
    secrets_file=ws_dir_path.joinpath("secrets/airflow_postgres_secrets.yml"),
)

# Prd redis: A redis instance to use as the celery backend for airflow
prd_redis = Redis(
    name="prd-redis",
    volume_type=RedisVolumeType.AWS_EBS,
    ebs_volume=prd_redis_volume,
    command=["redis-server", "--save", "60", "1", "--loglevel", "debug"],
)

# Databox: A containerized environment to run prd workflows
prd_databox = Databox(
    image_name=databox_image_name,
    image_tag=databox_image_tag,
    init_airflow=True,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    # Creates an airflow user using details from secrets/databox_secrets.yml
    create_airflow_test_user=True,
    secrets_file=ws_dir_path.joinpath("secrets/databox_secrets.yml"),
    db_connections=prd_airflow_connections,
)

## Airflow: For orchestrating prod workflows
# Airflow webserver
prd_airflow_ws = AirflowWebserver(
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    init_airflow_db=True,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    replicas=2,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_airflow_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    # Creates an airflow user using details from secrets/airflow_secrets.yml
    create_airflow_test_user=True,
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections=prd_airflow_connections,
)

# Airflow scheduler
prd_airflow_scheduler = AirflowScheduler(
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_airflow_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections=prd_airflow_connections,
)

# Airflow default worker queue
prd_airflow_default_workers = AirflowWorker(
    name="airflow-default-workers",
    queue_name="default,high_pri",
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    replicas=2,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_airflow_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections=prd_airflow_connections,
)

# Airflow high-pri worker queue
prd_airflow_high_pri_workers = AirflowWorker(
    name="airflow-high-pri-workers",
    queue_name="high_pri",
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    replicas=2,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_airflow_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections=prd_airflow_connections,
)

# Traefik Ingress: For routing web requests within the EKS cluster
routes = [
    {
        "match": f"Host(`whoami.{domain}`)",
        "kind": "Rule",
        "services": [
            {
                "name": whoami_service.service_name,
                "port": whoami_port.service_port,
            }
        ],
    },
    {
        "match": f"Host(`airflow.{domain}`)",
        "kind": "Rule",
        "services": [
            {
                "name": prd_airflow_ws.get_service_name(),
                "port": prd_airflow_ws.get_service_port(),
            }
        ],
    },
]

traefik_ingress_route = IngressRoute(
    name="traefik-prd",
    domain_name=domain,
    access_logs=True,
    web_enabled=True,
    web_routes=routes,
    # You can add an ACM certificate and enable HTTPS/websecure
    forward_web_to_websecure=True,
    websecure_enabled=True,
    websecure_routes=routes,
    # Read ACM certificate from a summary file
    acm_certificate_summary_file=acm_certificate.certificate_summary_file,
    # The dashboard is available at traefik.{domain.com}
    dashboard_enabled=True,
    # The dashboard is gated behind a user:password, which is generated using
    #   htpasswd -nb user password
    # You can provide the "users:password" list as a dashboard_auth_users param
    # or as DASHBOARD_AUTH_USERS in the secrets_file
    # Using the secrets_file is recommended
    # dashboard_auth_users="user:$apr1$flZrFgnm$VlaA8hNJsicUS2kCihgk00",
    # Use a LOAD_BALANCER service provided by AWS
    service_type=ServiceType.LOAD_BALANCER,
    load_balancer_provider=LoadBalancerProvider.AWS,
    # Use a Network Load Balancer: recommended
    use_nlb=True,
    nlb_target_type="ip",
    load_balancer_scheme="internet-facing",
    access_logs_to_s3=True,
    access_logs_s3_bucket=logs_s3_bucket.name,
    access_logs_s3_bucket_prefix="prd",
    secrets_file=ws_dir_path.joinpath("secrets/taefik_secrets.yml"),
)

prd_k8s_config = K8sConfig(
    env="prd",
    apps=[
        prd_db,
        prd_redis,
        prd_databox,
        prd_airflow_ws,
        prd_airflow_db,
        prd_airflow_scheduler,
        prd_airflow_default_workers,
        prd_airflow_high_pri_workers,
        traefik_ingress_route,
    ],
    create_resources=[whoami_k8s_rg],
    eks_cluster=data_eks_cluster,
)

######################################################
## Configure the workspace
######################################################
workspace = WorkspaceConfig(
    default_env="dev",
    docker=[dev_docker_config],
    k8s=[prd_k8s_config],
    aws=[prd_aws_config],
)
