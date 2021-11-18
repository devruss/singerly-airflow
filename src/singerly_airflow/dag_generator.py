import datetime
from airflow.utils import dates
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from singerly_airflow.pipeline import Pipeline, get_pipelines
from singerly_airflow.operator import SingerlyOperator


default_args = {'owner': 'airflow',
                  'start_date': dates.days_ago(1),
                  'depends_on_past': False,
                  'retries': 1,
                  'retry_delay': datetime.timedelta(hours=5)
                  }

def build_dag(pipeline: Pipeline) -> DAG:
  dag = DAG(dag_id=pipeline.id, schedule_interval='@daily', default_args=default_args)
  with dag:
    singerly_task = SingerlyOperator(task_id=pipeline.name, pipeline_id=pipeline.id)
    start = DummyOperator(task_id="Start")
    end = DummyOperator(task_id="End")
    start >> singerly_task >> end
  return dag

def build_dags(project_id: str):
  for pipeline in get_pipelines(project_id):
    globals()[pipeline.id] = build_dag(pipeline=pipeline)