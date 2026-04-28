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
        dag_id="Setpoints_BioFlo_DAG",
        description="DAG to test setpoints in the BioFlo bioreactor.",
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

    save_profile_1 = base_docker_node(
        task_id=f"save_profile_1",
        image="smb_connector",
        command=["python", "-c", f"from SMB_connector import save_setpoints; save_setpoints('setpoints/Pump_profile1.json')"],
    )

    wait_1 = TimeDeltaSensor(
        task_id=f"wait_30_min",
        poke_interval=10, trigger_rule='all_done',
        delta=dt.timedelta(minutes=30)
    )

    save_profile_2 = base_docker_node(
        task_id=f"save_profile_2",
        image="smb_connector",
        command=["python", "-c", f"from SMB_connector import save_setpoints; save_setpoints('setpoints/Pump_profile2.json')"],
    )

    wait_2 = TimeDeltaSensor(
        task_id=f"wait_50_min",
        poke_interval=10, trigger_rule='all_done',
        delta=dt.timedelta(minutes=50)
    )

    save_profile_3 = base_docker_node(
        task_id=f"save_profile_3",
        image="smb_connector",
        command=["python", "-c", f"from SMB_connector import save_setpoints; save_setpoints('setpoints/Pump_profile3.json')"],
    )

    start >> save_profile_1 >> wait_1 >> save_profile_2 >> wait_2 >> save_profile_3