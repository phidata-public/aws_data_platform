from pathlib import Path

from phidata.app.airflow import Airflow
from phidata.app.databox import Databox
from phidata.app.jupyter import Jupyter
from phidata.app.devbox import Devbox, DevboxDevModeArgs
from phidata.app.postgres import PostgresDb
from phidata.app.traefik import IngressRoute, LoadBalancerProvider
from phidata.infra.aws.config import AwsConfig
from phidata.infra.aws.create.iam.role import create_glue_iam_role
from phidata.infra.aws.resource.acm.certificate import AcmCertificate
from phidata.infra.aws.resource.cloudformation import CloudFormationStack
from phidata.infra.aws.resource.eks.cluster import EksCluster
from phidata.infra.aws.resource.eks.node_group import EksNodeGroup
from phidata.infra.aws.resource.group import AwsResourceGroup
from phidata.infra.aws.resource.s3 import S3Bucket
from phidata.infra.docker.config import DockerConfig
from phidata.infra.k8s.config import K8sConfig
from phidata.infra.k8s.create.core.v1.service import ServiceType
from phidata.workspace import WorkspaceConfig

from data.workspace.whoami import whoami_k8s_rg, whoami_service, whoami_port

######################################################
## Configure docker resources
## Applications:
##  - Dev database: A postgres db running in a container for storing dev data
######################################################

dev_db = PostgresDb(
    name="dev-db",
    postgres_db="dev",
    postgres_user="dev",
    postgres_password="dev",
    # You can connect to this db on port 5532 (on the host machine)
    container_host_port=5532,
)
devbox = Devbox(
    # Mount Aws config on the container
    mount_aws_config=True,
    # Init Airflow webserver when the container starts
    init_airflow_webserver=True,
    # Init Airflow scheduler as a deamon process,
    # init_airflow_scheduler=True,
    # Create a soft link from airflow_dir to airflow_home,
    # Useful when debugging the airflow conf,
    link_airflow_home=True,
    # Creates an airflow user with username: test, pass: test,
    create_airflow_test_user=True,
    airflow_webserver_host_port=8180,
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    dev_mode=DevboxDevModeArgs(),
    db_connections={dev_db.name: dev_db.get_connection_url_docker()},
)
dev_databox = Databox(
    # Mount Aws config on the container
    mount_aws_config=True,
    # Init Airflow webserver when the container starts
    init_airflow_webserver=True,
    # Init Airflow scheduler as a deamon process,
    # init_airflow_scheduler=True,
    # Creates an airflow user with username: test, pass: test,
    create_airflow_test_user=True,
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    db_connections={dev_db.name: dev_db.get_connection_url_docker()},
)
dev_docker_config = DockerConfig(
    apps=[dev_db, devbox, dev_databox],
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

ws_name = "aws-data-platform"
domain = "awsdataplatform.com"
ws_dir_path = Path(__file__).parent.resolve()

# S3 bucket for storing data
data_s3_bucket = S3Bucket(
    name=f"phi-{ws_name}",
    acl="private",
)
# Iam Role for Glue crawlers
glue_iam_role = create_glue_iam_role(
    name=f"glue-crawler-role",
    s3_buckets=[data_s3_bucket],
)
# EKS cluster + nodegroup + vpc stack
data_vpc_stack = CloudFormationStack(
    name=f"{ws_name}-vpc",
    template_url="https://amazon-eks.s3.us-west-2.amazonaws.com/cloudformation/2020-10-29/amazon-eks-vpc-private-subnets.yaml",
    # skip_delete=True implies this resource will NOT be deleted with `phi ws down`
    # uncomment when workspace is production-ready
    # skip_delete=True,
)
data_eks_cluster = EksCluster(
    name=f"{ws_name}-cluster",
    vpc_stack=data_vpc_stack,
    # skip_delete=True,
)
data_eks_nodegroup = EksNodeGroup(
    name=f"{ws_name}-ng",
    eks_cluster=data_eks_cluster,
    min_size=2,
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
    acm_certificates=[acm_certificate],
    s3_buckets=[data_s3_bucket],
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
    postgres_user="prd",
    # The password is created on K8s as a Secret, it needs to be in base64
    # echo "password" | base64
    postgres_password="cHJkCg==",
    postgres_db="prd",
)
prd_databox = Databox(
    env={"findme_key": "findme_value"},
    secrets_file=ws_dir_path.joinpath("secrets/databox_secrets.yaml"),
    # Mount Aws config on the container
    mount_aws_config=True,
    # Init Airflow webserver when the container starts
    init_airflow_webserver=True,
    # Init Airflow scheduler as a deamon process,
    # init_airflow_scheduler=True,
    # Creates an airflow user with username: test, pass: test,
    create_airflow_test_user=True,
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    use_cache=False,
    db_connections={prd_db.name: prd_db.get_connection_url_docker()},
)
airflow = Airflow(enabled=False)
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
    # The dashboard is gated behind a user password, which is generated using
    #   htpasswd -nb user password | openssl base64
    dashboard_auth_users="cGFuZGE6JGFwcjEkSVYxWng4eXYkYmtFOHc4cGVSLnNzVEwyMTJINnJCLwoK",
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
)

prd_k8s_config = K8sConfig(
    env="prd",
    apps=[prd_db, prd_databox, airflow, jupyter, traefik_ingress_route],
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
    aws_region="us-east-1",
)
