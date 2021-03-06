import datetime
import pendulum
from multiprocessing import Pipe
from airflow.utils import timezone
from airflow.utils.email import send_email_smtp
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from singerly_airflow.pipeline import Pipeline, get_pipelines
from singerly_airflow.operator import SingerlyOperator


def send_email_alert(pipeline: Pipeline):
    def failure_callback(context):
        # task_instance = context['task_instance']
        pass

    def success_callback(context):
        subject = "[Airflow] DAG {0} - Task {1}: Success".format(
            context["task_instance_key_str"].split("__")[0],
            context["task_instance_key_str"].split("__")[1],
        )
        html_content = """
    DAG: {0}<br>
    Task: {1}<br>
    Succeeded on: {2}
    """.format(
            context["task_instance_key_str"].split("__")[0],
            context["task_instance_key_str"].split("__")[1],
            datetime.now(),
        )
        send_email_smtp(
            to=pipeline.get_email_list(),
            subject=subject,
            html_content=html_content,
            conn_id=None,
        )

    return (failure_callback, success_callback)


default_args = {
    "owner": "airflow",
    "start_date": timezone.utcnow(),
    "depends_on_past": False,
    "retries": 1,
    "email_on_failure": True,
    "email_on_retry": True,
    "email_on_success": False,
    "retry_delay": datetime.timedelta(hours=5),
}


def build_dag(pipeline: Pipeline) -> DAG:
    dag = DAG(
        dag_id=pipeline.id,
        schedule_interval=pipeline.schedule,
        max_active_runs=1,
        default_args={
            **default_args,
            "email": pipeline.get_email_list(),
            "start_date": pendulum.parse(pipeline.start_date),
        },
        is_paused_upon_creation=(not pipeline.is_enabled),
    )
    with dag:
        singerly_task = SingerlyOperator(
            task_id=pipeline.name,
            pipeline_id=pipeline.id,
            executor_config={
                "KubernetesExecutor": {
                    "request_cpu": pipeline.cpu_limit,
                    "request_memory": pipeline.memory_limit,
                    "limit_cpu": pipeline.cpu_limit,
                    "limit_memory": pipeline.memory_limit,
                    "labels": {"instance_type": pipeline.instance_type},
                }
            },
        )
        if pipeline.get_email_list():
            success_email = EmailOperator(
                task_id="email_success",
                trigger_rule="all_success",
                to=pipeline.get_email_list(),
                subject="""[Airflow] DAG {{ task_instance_key_str.split('__')[0] }}: Success""",
                html_content="""
        DAG: <b>{{ task_instance_key_str.split('__')[0] }}</b><br>
        Succeeded on: {{ macros.datetime.now() }}
        """,
            )
            failure_email = EmailOperator(
                task_id="email_failure",
                trigger_rule="all_failed",
                to=pipeline.get_email_list(),
                subject="""[Airflow] DAG {{ task_instance_key_str.split('__')[0] }}: Failed""",
                html_content="""
        DAG: <b>{{ task_instance_key_str.split('__')[0] }}</b><br>
        Failed on: {{ macros.datetime.now() }}
        Please follow the <a href="{{ ti.log_url }}">link</a> to view the logs.
        """,
            )
            singerly_task >> [success_email, failure_email]
        else:
            singerly_task
    return dag


def build_dags(project_id: str, globals):
    for pipeline in get_pipelines(project_id):
        globals[pipeline.id] = build_dag(pipeline)
