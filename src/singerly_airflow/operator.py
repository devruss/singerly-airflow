from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import Context
from singerly_airflow.pipeline import get_pipeline
from singerly_airflow.executor import Executor
import os
import asyncio


class SingerlyOperator(BaseOperator):
    def __init__(self, pipeline_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.pipeline_id = pipeline_id

    def execute(self, context: Context):
        pipeline = get_pipeline(
            project_id=os.environ.get("PROJECT_ID"), id=self.pipeline_id
        )
        if pipeline and pipeline.is_valid():
            pipeline_executor = Executor(pipeline)
            asyncio.run(pipeline_executor.execute())
