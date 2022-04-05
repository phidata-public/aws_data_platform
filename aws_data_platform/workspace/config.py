from pathlib import Path

from phidata.app.airflow import (
    AirflowWebserver,
    AirflowScheduler,
    AirflowWorker,
    AirflowFlower,
)
from phidata.app.databox import Databox
from phidata.app.jupyter import Jupyter
from phidata.app.devbox import Devbox
from phidata.app.postgres import PostgresDb, PostgresVolumeType
from phidata.app.redis import Redis, RedisVolumeType
from phidata.app.traefik import IngressRoute, LoadBalancerProvider
from phidata.infra.aws.config import AwsConfig
from phidata.infra.aws.create.iam.role import create_glue_iam_role
from phidata.infra.aws.resource.acm.certificate import AcmCertificate
from phidata.infra.aws.resource.cloudformation import CloudFormationStack
from phidata.infra.aws.resource.ec2.volume import EbsVolume
from phidata.infra.aws.resource.eks.cluster import EksCluster
from phidata.infra.aws.resource.eks.node_group import EksNodeGroup
from phidata.infra.aws.resource.group import AwsResourceGroup
from phidata.infra.aws.resource.s3 import S3Bucket
from phidata.infra.docker.config import DockerConfig
from phidata.infra.k8s.config import K8sConfig
from phidata.infra.k8s.create.core.v1.service import ServiceType
from phidata.infra.k8s.enums.image_pull_policy import ImagePullPolicy
from phidata.workspace import WorkspaceConfig

from aws_data_platform.workspace.whoami import (
    whoami_k8s_rg,
    whoami_service,
    whoami_port,
)

ws_dir_path = Path(__file__).parent.resolve()

######################################################
## Configure docker resources
## Applications:
##  - Dev database: A postgres db running in a container for storing dev data
######################################################

dev_db = PostgresDb(
    name="dev-db",
    db_user="dev",
    db_password="dev",
    db_schema="dev",
    # You can connect to this db on port 5532 (on the host machine)
    container_host_port=5532,
)
pg_db_connection_id = "pg_db"
dev_redis = Redis(command=["redis-server", "--save", "60", "1", "--loglevel", "debug"])
devbox = Devbox(
    init_airflow=True,
    # Mount Aws config on the container
    mount_aws_config=True,
    # Init Airflow webserver when the container starts
    init_airflow_webserver=True,
    # Init Airflow scheduler as a daemon process
    init_airflow_scheduler=True,
    # Creates an airflow user with username: test, pass: test,
    create_airflow_test_user=True,
    airflow_webserver_host_port=8280,
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    install_phidata_dev=True,
    db_connections={pg_db_connection_id: dev_db.get_db_connection_url_docker()},
)
dev_databox = Databox(
    init_airflow=True,
    # Mount Aws config on the container
    mount_aws_config=True,
    # Init Airflow webserver when the container starts
    init_airflow_webserver=True,
    # Init Airflow scheduler as a daemon process,
    init_airflow_scheduler=True,
    # Creates an airflow user using details from env/databox_env.yml
    create_airflow_test_user=True,
    airflow_webserver_host_port=8180,
    env_file=ws_dir_path.joinpath("env/databox_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/databox_secrets.yml"),
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    db_connections={pg_db_connection_id: dev_db.get_db_connection_url_docker()},
)
dev_airflow_ws = AirflowWebserver(
    # Mount Aws config on the container
    mount_aws_config=True,
    init_airflow_db=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # Creates an airflow user using details from the airflow_env.yaml
    create_airflow_test_user=True,
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    db_connections={pg_db_connection_id: dev_db.get_db_connection_url_docker()},
)
dev_airflow_scheduler = AirflowScheduler(
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    container_host_port=8081,
    db_connections={pg_db_connection_id: dev_db.get_db_connection_url_docker()},
)
def_airflow_default_workers = AirflowWorker(
    name="airflow-default-workers",
    queue_name="default,high_pri",
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    container_host_port=8082,
    db_connections={pg_db_connection_id: dev_db.get_db_connection_url_docker()},
)
dev_airflow_high_pri_workers = AirflowWorker(
    name="airflow-high-pri-workers",
    queue_name="high_pri",
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/dev_airflow_secrets.yml"),
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    container_host_port=8083,
    worker_log_host_port=8794,
    db_connections={pg_db_connection_id: dev_db.get_db_connection_url_docker()},
)
dev_airflow_flower = AirflowFlower(
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    db_connections={pg_db_connection_id: dev_db.get_db_connection_url_docker()},
)
dev_docker_config = DockerConfig(
    apps=[
        dev_db,
        dev_redis,
        devbox,
        dev_databox,
        dev_airflow_ws,
        dev_airflow_scheduler,
        def_airflow_default_workers,
        dev_airflow_high_pri_workers,
        dev_airflow_flower,
    ],
)

######################################################
## Configure AWS resources:
## Infrastructure:
##  - S3 bucket
##  - Iam Role for Glue crawlers
##  - EKS cluster + nodegroup + vpc stack
##  - ACM certificate for awsdataplatform.com
## Applications:
##  - Databox: A containerized environment for running prod workflows.
##  - Jupyter: For analyzing prod data
##  - Airflow: For orchestrating prod workflows
##  - Postgres Database: For storing prod data
##  - Traefik: The Ingress which routes web requests to our infrastructure
##      allowing us to access the airflow dashboard and jupyter notebooks
##  - WhoAmiI: A tiny server for checking our infrastructure is working
######################################################

######################################################
## AWS Infrastructure
######################################################

ws_key = "aws-data-platform"
domain = "awsdataplatform.com"
aws_az = "us-east-1a"
aws_region = "us-east-1"

# S3 bucket for storing data
data_s3_bucket = S3Bucket(
    name=f"phi-{ws_key}",
    acl="private",
)
# EbsVolume for postgres data
prd_db_volume = EbsVolume(
    name="prd-db-volume",
    size=1,
    availability_zone=aws_az,
    skip_delete=True,
)
# EbsVolume for redis
prd_redis_volume = EbsVolume(
    name="prd-redis-volume",
    size=1,
    availability_zone=aws_az,
    skip_delete=True,
)
# Iam Role for Glue crawlers
glue_iam_role = create_glue_iam_role(
    name=f"glue-crawler-role",
    s3_buckets=[data_s3_bucket],
    skip_delete=True,
)
# EKS cluster + nodegroup + vpc stack
data_vpc_stack = CloudFormationStack(
    name=f"{ws_key}-vpc",
    template_url="https://amazon-eks.s3.us-west-2.amazonaws.com/cloudformation/2020-10-29/amazon-eks-vpc-private-subnets.yaml",
    # skip_delete=True implies this resource will NOT be deleted with `phi ws down`
    # uncomment when workspace is production-ready
    skip_delete=True,
)
data_eks_cluster = EksCluster(
    name=f"{ws_key}-cluster",
    vpc_stack=data_vpc_stack,
    # skip_delete=True,
)
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
    # acm_certificates=[acm_certificate],
    s3_buckets=[data_s3_bucket],
    volumes=[prd_db_volume, prd_redis_volume],
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
prd_db = PostgresDb(
    name="prd-db",
    db_user="prd",
    db_schema="prd",
    volume_type=PostgresVolumeType.AWS_EBS,
    ebs_volume=prd_db_volume,
    secrets_file=ws_dir_path.joinpath("secrets/prd_postgres_secrets.yml"),
)
prd_redis = Redis(
    name="prd-redis",
    volume_type=RedisVolumeType.AWS_EBS,
    ebs_volume=prd_redis_volume,
    command=["redis-server", "--save", "60", "1", "--loglevel", "debug"],
)
prd_databox = Databox(
    init_airflow=True,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    # Init Airflow webserver when the container starts
    init_airflow_webserver=True,
    # Init Airflow scheduler as a daemon process,
    init_airflow_scheduler=True,
    # Creates an airflow user using details from env/databox_env.yml
    create_airflow_test_user=True,
    env_file=ws_dir_path.joinpath("env/databox_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/databox_secrets.yml"),
    db_connections={pg_db_connection_id: prd_db.get_db_connection_url_k8s()},
)
prd_airflow_ws = AirflowWebserver(
    init_airflow_db=True,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    # Creates an airflow user using details from the airflow_env.yaml
    create_airflow_test_user=True,
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections={pg_db_connection_id: prd_db.get_db_connection_url_k8s()},
)
prd_airflow_scheduler = AirflowScheduler(
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections={pg_db_connection_id: prd_db.get_db_connection_url_k8s()},
)
prd_airflow_default_workers = AirflowWorker(
    name="airflow-default-workers",
    queue_name="default,high_pri",
    replicas=2,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections={pg_db_connection_id: prd_db.get_db_connection_url_k8s()},
)
prd_airflow_high_pri_workers = AirflowWorker(
    name="airflow-high-pri-workers",
    queue_name="high_pri",
    replicas=2,
    image_pull_policy=ImagePullPolicy.ALWAYS,
    # Mount the workspace on the container using git-sync
    git_sync_repo="https://github.com/phidata-public/aws_data_platform.git",
    git_sync_branch="main",
    db_app=prd_db,
    redis_app=prd_redis,
    wait_for_db=True,
    wait_for_redis=True,
    executor="CeleryExecutor",
    env_file=ws_dir_path.joinpath("env/airflow_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    db_connections={pg_db_connection_id: prd_db.get_db_connection_url_k8s()},
)

jupyter = Jupyter(enabled=False)

# Traefik Ingress
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
    access_logs_s3_bucket=data_s3_bucket.name,
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
        prd_airflow_scheduler,
        prd_airflow_default_workers,
        prd_airflow_high_pri_workers,
        jupyter,
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
    aws_region=aws_region,
    # path to source code, this should be updated
    # if the directory containing the products dir is renamed
    src_dir="aws_data_platform",
)
