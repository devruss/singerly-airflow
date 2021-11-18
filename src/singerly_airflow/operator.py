from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import Context
from .pipeline import execute_pipeline, get_pipeline
import boto3

class SingerlyOperator(BaseOperator):
  def __init__(self, pipeline_id: str, **kwargs) -> None:
    super().__init__(**kwargs)
    self.pipeline_id = pipeline_id

  def execute(self, context: Context):
    pipeline = get_pipeline(self.pipeline_id)
    if (pipeline and pipeline.is_valid()):
      execute_pipeline(pipeline)
