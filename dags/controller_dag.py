import datetime as dt
import json
import os
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.empty import EmptyOperator
from docker.types import Mount


with DAG(
        dag_id="BioFlo_DAG",
        description="DAG to extract data from the BioFlo experiment.",
        start_date=dt.datetime.now(),
        schedule_interval=None,
        catchup=False,
        is_paused_upon_creation=True
) as dag:
        
    # directory path for mounting the docker volume
    try:
        host_path = Variable.get("host_path", deserialize_json=True)
    except:
        host_path = os.environ.get("DAGS_DIR", "./dags")

    remote_path = "/opt/airflow/dags"

    # duration of the experiment in hours
    t_duration = 24 


    def base_docker_node(task_id, command, retries=0, retry_delay=dt.timedelta(minutes=2),
                        execution_timeout=dt.timedelta(minutes=30), trigger_rule='all_success', image="smb_connector"):

        return DockerOperator(
            task_id=task_id,
            image=image,
            auto_remove="force",
            working_dir=f"{remote_path}/scripts/controller_dag",
            command=command,
            environment=os.environ,
            mounts=[Mount(source=host_path, target=remote_path, type='bind')],
            mount_tmp_dir=False,
            network_mode="bridge", # check this in Linux
            retries=retries,
            retry_delay=retry_delay,
            execution_timeout=execution_timeout,
            trigger_rule=trigger_rule
        )

    start = EmptyOperator(task_id="start")  

    last_node = start      

    # iterations every hour to group tasks
    for hours in range(1, int(t_duration) + 1):

        wait_update = TimeDeltaSensor(
            task_id=f"wait_{hours}_hour{'s' if hours > 1 else ''}",
            poke_interval=10, trigger_rule='all_done',
            delta=dt.timedelta(hours=hours)
        )

        get_data = base_docker_node(
            task_id=f"get_data_{hours}_hour{'s' if hours > 1 else ''}",
            image="smb_connector",
            command=["python", "-c", f"from SMB_connector import get_measurements; get_measurements('{{{{ dag_run.get_task_instance('start').start_date }}}}', {hours})"],
        )

        last_node >> wait_update >> get_data
        last_node = wait_update