from contextlib import suppress
import subprocess
from typing import List
import boto3
import os
from dataclasses import dataclass

from pendulum import datetime, now
from singerly_airflow.utils import timed_lru_cache, get_package_name
from singerly_airflow.venv import Venv
import asyncio


class PipelineConnectorExecutionException(Exception):
    pass


@dataclass
class Pipeline:
    id: int
    name: str
    tap_config: str
    tap_url: str
    target_url: str
    target_config: str
    tap_catalog: str
    pipeline_state: str
    project_id: str
    tap_executable: str = ""
    target_executable: str = ""
    email_list: str = ""
    uploaded_files: str = ""
    is_enabled: bool = False
    schedule: str = "@dayli"
    start_date: str = now().isoformat()

    def save_state(self) -> None:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(self.project_id)
        table.update_item(
            Key={
                "id": self.id,
            },
            UpdateExpression="set pipeline_state=:state",
            ExpressionAttributeValues={":state": self.pipeline_state},
        )

    def get_email_list(self):
        if not self.email_list:
            return []
        return [email.strip() for email in self.email_list.split(",")]

    def get_tap_executable(self) -> str:
        if self.tap_executable:
            return self.tap_executable
        return get_package_name(package_url=self.tap_url)

    def get_target_executable(self) -> str:
        if self.target_executable:
            return self.target_executable
        return get_package_name(package_url=self.target_url)

    def process_uploaded_files(self):
        uploaded_files = self.uploaded_files.split(",")
        work_dir = os.getcwd()
        if uploaded_files:
            os.chdir("/tmp")
            s3 = boto3.client("s3")
            for uploaded_file in uploaded_files:
                if not uploaded_file:
                    continue
                try:
                    s3.download_file(
                        "singerly-pipelines-uploads", uploaded_file, uploaded_file
                    )
                except Exception:
                    print(f"Cannot download file {uploaded_file}")
            os.chdir(work_dir)

    def execute(self) -> None:
        pass

    def is_valid(self) -> bool:
        return self.tap_config and self.tap_url and self.tap_catalog and self.target_url


@timed_lru_cache(seconds=30)
def get_pipeline(project_id: str, id: str) -> Pipeline:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(project_id)
    pipeline_raw = table.get_item(Key={"id": id})["Item"]
    return Pipeline(project_id=project_id, **pipeline_raw)


@timed_lru_cache(seconds=30)
def get_pipelines(project_id: str) -> List[Pipeline]:
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(project_id)
    result = table.scan()
    return [
        Pipeline(project_id=project_id, **pipeline_raw)
        for pipeline_raw in result["Items"]
    ]
