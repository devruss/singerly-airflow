import subprocess
from typing import List
import boto3
import os
from dataclasses import dataclass
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

    def save_state(self, state: str) -> None:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(self.project_id)
        table.update_item(
            Key={
                "id": self.id,
            },
            UpdateExpression="set pipeline_state=:state",
            ExpressionAttributeValues={":state": state},
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

    async def process_stdout(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        async for line in reader:
            if line:
                writer.write(line)
                await writer.drain()

    async def process_stderr(self, reader: asyncio.StreamReader):
        async for line in reader:
            if line:
                print(line.decode("utf-8"))

    async def execute(self) -> None:
        if not self.is_valid():
            return
        work_dir = "/tmp"
        os.chdir(work_dir)
        print(f"Installing source connector: {get_package_name(self.tap_url)}")
        tap_venv = Venv("tap", package_url=self.tap_url, work_dir=work_dir)
        print(f"Installing destination connector: {get_package_name(self.target_url)}")
        target_venv = Venv("target", package_url=self.target_url, work_dir=work_dir)
        await asyncio.gather(tap_venv.install_package(), target_venv.install_package())

        self.process_uploaded_files()
        with open(f"{os.getcwd()}/tap_config.json", "w") as tap_config_file:
            tap_config_file.write(self.tap_config)
        with open(f"{os.getcwd()}/target_config.json", "w") as target_config_file:
            target_config_file.write(self.target_config)
        with open(f"{os.getcwd()}/catalog.json", "w") as catalog_file:
            catalog_file.write(self.tap_catalog)
        tap_run_args = [
            f"{tap_venv.get_bin_dir()}/{self.get_tap_executable()}",
            "-c",
            "tap_config.json",
            "--catalog",
            "catalog.json",
            "-p",
            "catalog.json",
        ]
        if self.pipeline_state:
            with open(f"{os.getcwd()}/tap_state.json", "w") as tap_state_file:
                tap_state_file.write(self.pipeline_state)
            tap_run_args.extend(["-s", "tap_state.json"])
        target_run_args = [
            f"{target_venv.get_bin_dir()}/{self.get_target_executable()}",
            "-c",
            "target_config.json",
        ]
        print(
            "Starting pipeline execution",
            self.get_tap_executable(),
            "->",
            self.get_target_executable(),
        )
        tap_coro = await asyncio.subprocess.create_subprocess_exec(
            *tap_run_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            # bufsize=50 * 1024,
        )
        target_coro = await asyncio.subprocess.create_subprocess_exec(
            *target_run_args,
            stdout=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            stderr=subprocess.PIPE,
            # bufsize=50 * 1024,
        )

        loop = asyncio.get_running_loop()

        stdout_task = loop.create_task(
            self.process_stdout(reader=tap_coro.stdout, writer=target_coro.stdin)
        )
        stderr_task = loop.create_task(self.process_stderr(reader=tap_coro.stderr))
        await asyncio.gather(stdout_task, stderr_task)

        target_coro.stdin.close()
        await target_coro.stdin.wait_closed()

        stdout, _ = await target_coro.communicate()
        stdout_decoded_lines = stdout.decode("utf-8").splitlines()
        if len(stdout_decoded_lines):
            print(stdout_decoded_lines[-1])
            self.save_state(stdout_decoded_lines[-1])
        await asyncio.wait([tap_coro.wait(), target_coro.wait()])

        print("Finished data sync")
        # while True:
        #     try:
        #         next_line = tap_process.stdout.readline()
        #         if not next_line:
        #             break
        #         decoded_line = next_line.decode("utf-8").strip()
        #         if decoded_line:
        #             print(decoded_line)
        #         target_process.stdin.write(next_line)
        #     except Exception as e:
        #         print(e)
        #         stdout, stderr = target_process.communicate()
        #         decoded_stderr = stderr.decode("utf-8").strip()
        #         if decoded_stderr:
        #             print(decoded_stderr)
        #         raise e

        # tap_process.communicate()
        # stdout, stderr = target_process.communicate()
        # stdout_decoded_lines = stdout.decode("utf-8").splitlines()
        # if len(stdout_decoded_lines):
        #     print(stdout_decoded_lines[-1])
        #     self.save_state(stdout_decoded_lines[-1])
        # # if stderr and stderr.decode('utf-8'):
        # #   print(stderr.decode('utf-8'))

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
