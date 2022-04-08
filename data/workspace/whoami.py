from phidata.infra.k8s.create.apps.v1.deployment import CreateDeployment
from phidata.infra.k8s.create.core.v1.container import CreateContainer
from phidata.infra.k8s.create.core.v1.service import CreateService
from phidata.infra.k8s.create.common.port import CreatePort
from phidata.infra.k8s.create.group import CreateK8sResourceGroup
from phidata.utils.common import (
    get_default_service_name,
    get_default_container_name,
    get_default_deploy_name,
    get_default_pod_name,
)

whoami_name = "whoami"
whoami_port = CreatePort(
    name="http",
    container_port=80,
    service_port=80,
    target_port="http",
)
whoami_container = CreateContainer(
    container_name=get_default_container_name(whoami_name),
    app_name=whoami_name,
    image_name="traefik/whoami",
    image_tag="v1.8.0",
    ports=[whoami_port],
)
whoami_deployment = CreateDeployment(
    deploy_name=get_default_deploy_name(whoami_name),
    pod_name=get_default_pod_name(whoami_name),
    app_name=whoami_name,
    containers=[whoami_container],
)
whoami_service = CreateService(
    service_name=get_default_service_name(whoami_name),
    app_name=whoami_name,
    deployment=whoami_deployment,
    ports=[whoami_port],
)
whoami_k8s_rg = CreateK8sResourceGroup(
    name=whoami_name,
    services=[whoami_service],
    deployments=[whoami_deployment],
)
