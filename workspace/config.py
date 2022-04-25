from phidata.workspace import WorkspaceConfig

from workspace.dev_config import dev_docker_config
from workspace.prd_config import prd_aws_config, prd_k8s_config

######################################################
## Configure the workspace
######################################################
workspace = WorkspaceConfig(
    default_env="dev",
    docker=[dev_docker_config],
    k8s=[prd_k8s_config],
    aws=[prd_aws_config],
)
