import asyncio
from codecs import StreamReader
from contextlib import suppress

import os
from singerly_airflow.pipeline import Pipeline
from singerly_airflow.utils import get_package_name
from singerly_airflow.venv import Venv


class Executor:
    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.work_dir = "/tmp"

    async def enqueue_logs(self, reader: asyncio.StreamReader):
        while line := await reader.readline():
            await self.logs_queue.put(line)

    async def process_logs_queue(self):
        while line := await self.logs_queue.get():
            print(line.decode("utf-8"))

    async def enqueue_stream_data(self, reader: asyncio.StreamReader):
        while line := await reader.readline():
            await self.stream_queue.put(line)
        await self.stream_queue.put(None)

    async def process_stream_queue(self, writer: asyncio.StreamWriter):
        while line := await self.stream_queue.get():
            try:
                writer.write(line)
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError):
                with suppress(AttributeError):
                    writer.close()
                    await writer.wait_closed()
                    return

    async def install_connectors(self):
        print(f"Installing source connector: {get_package_name(self.pipeline.tap_url)}")
        self.tap_venv = Venv(
            "tap", package_url=self.pipeline.tap_url, work_dir=self.work_dir
        )
        print(
            f"Installing destination connector: {get_package_name(self.pipeline.target_url)}"
        )
        self.target_venv = Venv(
            "target", package_url=self.pipeline.target_url, work_dir=self.work_dir
        )
        await asyncio.gather(
            self.tap_venv.install_package(), self.target_venv.install_package()
        )

    async def execute(self):
        if not self.pipeline.is_valid():
            return
        os.chdir(self.work_dir)
        self.logs_queue = asyncio.Queue()
        self.stream_queue = asyncio.Queue()

        await self.install_connectors()

        # self.process_uploaded_files()
        with open(f"{os.getcwd()}/tap_config.json", "w") as tap_config_file:
            tap_config_file.write(self.pipeline.tap_config)
        with open(f"{os.getcwd()}/target_config.json", "w") as target_config_file:
            target_config_file.write(self.pipeline.target_config)
        with open(f"{os.getcwd()}/catalog.json", "w") as catalog_file:
            catalog_file.write(self.pipeline.tap_catalog)
        tap_run_args = [
            f"{self.tap_venv.get_bin_dir()}/{self.pipeline.get_tap_executable()}",
            "-c",
            "tap_config.json",
            "--catalog",
            "catalog.json",
            "-p",
            "catalog.json",
        ]
        if self.pipeline.pipeline_state:
            with open(f"{os.getcwd()}/tap_state.json", "w") as tap_state_file:
                tap_state_file.write(self.pipeline.pipeline_state)
            tap_run_args.extend(["-s", "tap_state.json"])
        target_run_args = [
            f"{self.target_venv.get_bin_dir()}/{self.pipeline.get_target_executable()}",
            "-c",
            "target_config.json",
        ]
        print(
            "Starting pipeline execution",
            self.pipeline.get_tap_executable(),
            "->",
            self.pipeline.get_target_executable(),
        )
        tap_proc = await asyncio.subprocess.create_subprocess_exec(
            *tap_run_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=5 * 1024 * 1024,
        )
        target_proc = await asyncio.subprocess.create_subprocess_exec(
            *target_run_args,
            stdout=asyncio.subprocess.DEVNULL,
            stdin=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=5 * 1024 * 1024,
        )

        tap_logs_enqueue_task = asyncio.create_task(
            self.enqueue_logs(reader=tap_proc.stderr)
        )
        target_logs_enqueue_task = asyncio.create_task(
            self.enqueue_logs(reader=target_proc.stderr)
        )
        target_stream_enqueue_task = asyncio.create_task(
            self.enqueue_stream_data(reader=tap_proc.stdout)
        )
        process_logs_task = asyncio.create_task(self.process_logs_queue())
        process_stream_task = asyncio.create_task(
            self.process_stream_queue(writer=target_proc.stdin)
        )

        logs_tasks = asyncio.gather(
            tap_logs_enqueue_task,
            target_logs_enqueue_task,
            process_logs_task,
        )

        stream_tasks = asyncio.gather(
            target_stream_enqueue_task,
            process_stream_task,
        )

        result = await asyncio.wait(
            [logs_tasks, stream_tasks], return_when=asyncio.FIRST_COMPLETED
        )

        print(result)

        print("Stream processing finished")
        with suppress(AttributeError):
            target_proc.stdin.close()
            await target_proc.stdin.wait_closed()
            target_proc.terminate()

        await self.logs_queue.put(None)

        print("Syncing state")
        self.pipeline.save_state()

        await tap_proc.communicate()
        await target_proc.communicate()

        await asyncio.wait(
            [tap_proc.wait(), target_proc.wait()], return_when=asyncio.ALL_COMPLETED
        )

        print("Finished pipeline execution")
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
