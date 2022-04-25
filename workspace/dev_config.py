from pathlib import Path

from phidata.app.airflow import (
    AirflowWebserver,
    AirflowScheduler,
    AirflowWorker,
    AirflowFlower,
)
from phidata.app.databox import Databox
from phidata.app.postgres import PostgresDb
from phidata.app.redis import Redis
from phidata.infra.docker.config import DockerConfig
from phidata.infra.docker.resource.image import DockerImage

ws_key = "aws-dp"
ws_dir_path = Path(__file__).parent.resolve()

######################################################
## Configure docker resources
######################################################

# Dev database: A postgres instance for storing dev data
# This db can be accessed on port 5532 locally or using the 'pg_db' conn_id in airflow
dev_db = PostgresDb(
    name=f"dev-db-{ws_key}",
    db_user="dev",
    db_password="dev",
    db_schema="dev",
    # Connect to this db on port 3532
    container_host_port=3532,
)
pg_db_connection_id = "pg_db"
dev_airflow_connections = {
    pg_db_connection_id: dev_db.get_db_connection_url_docker()
}

# Dev Airflow db: A postgres instance to use as the database for airflow
dev_airflow_db = PostgresDb(
    name=f"airflow-db-{ws_key}",
    db_user="airflow",
    db_password="airflow",
    db_schema="airflow",
    # Connect to this db on port 3432
    container_host_port=3432,
)

# Dev redis: A redis instance to use as the celery backend for airflow
dev_redis = Redis(command=["redis-server", "--save", "60", "1", "--loglevel", "debug"])

# Databox image
databox_image_name = f"phidata/databox-{ws_key}"
databox_image_tag = "2.2.5"
databox_image = DockerImage(
    name=databox_image_name,
    tag=databox_image_tag,
    path=str(ws_dir_path.parent),
    dockerfile="workspace/databox.Dockerfile",
    print_build_log=True,
    use_verbose_logs=True,
    use_cache=False,
    push_image=True,
)

# Databox: A containerized environment to run dev workflows
dev_databox = Databox(
    image_name=databox_image_name,
    image_tag=databox_image_tag,
    init_airflow=True,
    # Mount Aws config on the container
    mount_aws_config=True,
    # Init Airflow webserver when the container starts
    init_airflow_webserver=True,
    # Creates an airflow user using details from env/databox_env.yml
    create_airflow_test_user=True,
    airflow_webserver_container_port=7080,
    airflow_webserver_host_port=6180,
    env_file=ws_dir_path.joinpath("env/databox_env.yml"),
    secrets_file=ws_dir_path.joinpath("secrets/databox_secrets.yml"),
    # use_cache=False implies the container will be recreated every time we run `phi ws up`
    use_cache=False,
    install_phidata_dev=True,
    db_connections=dev_airflow_connections,
)

# Airflow image
airflow_image_name = f"phidata/airflow-{ws_key}"
airflow_image_tag = "2.2.5"
airflow_image = DockerImage(
    name=airflow_image_name,
    tag=airflow_image_tag,
    path=str(ws_dir_path.parent),
    dockerfile="workspace/airflow.Dockerfile",
    print_build_log=True,
    use_cache=False,
    push_image=True,
)

# Airflow webserver
dev_airflow_ws = AirflowWebserver(
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    # Mount Aws config on the container
    mount_aws_config=True,
    init_airflow_db=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_airflow_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # Creates an airflow user using details from the airflow_env.yaml
    create_airflow_test_user=True,
    # use_cache=False implies the container will be recreated every time we run `phi ws up`
    # use_cache=False,
    container_host_port=6080,
    db_connections=dev_airflow_connections,
)

# Airflow scheduler
dev_airflow_scheduler = AirflowScheduler(
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_airflow_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # use_cache=False implies the container will be recreated every time we run `phi ws up`
    # use_cache=False,
    db_connections=dev_airflow_connections,
)

# Airflow default worker queue
dev_airflow_default_workers = AirflowWorker(
    name="airflow-default-workers",
    queue_name="default,high_pri",
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_airflow_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # use_cache=False implies the container will be recreated every time we run `phi ws up`
    # use_cache=False,
    db_connections=dev_airflow_connections,
)

# Airflow high-pri worker queue
dev_airflow_high_pri_workers = AirflowWorker(
    name="airflow-high-pri-workers",
    queue_name="high_pri",
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_airflow_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/dev_airflow_secrets.yml"),
    worker_log_host_port=8794,
    # use_cache=False implies the container will be recreated every time we run `phi ws up`
    # use_cache=False,
    db_connections=dev_airflow_connections,
)

dev_airflow_flower = AirflowFlower(
    image_name=airflow_image_name,
    image_tag=airflow_image_tag,
    # Mount Aws config on the container
    mount_aws_config=True,
    wait_for_db=True,
    wait_for_redis=True,
    db_app=dev_airflow_db,
    redis_app=dev_redis,
    executor="CeleryExecutor",
    secrets_file=ws_dir_path.joinpath("secrets/airflow_secrets.yml"),
    # use_cache=False implies the container will be recreated every time you run `phi ws up`
    # use_cache=False,
    db_connections=dev_airflow_connections,
)

dev_docker_config = DockerConfig(
    apps=[
        dev_db,
        dev_redis,
        dev_databox,
        dev_airflow_db,
        dev_airflow_ws,
        dev_airflow_scheduler,
        dev_airflow_default_workers,
        dev_airflow_high_pri_workers,
        dev_airflow_flower,
    ],
    images=[databox_image, airflow_image],
)
