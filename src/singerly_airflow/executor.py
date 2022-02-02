import asyncio
from asyncio.subprocess import Process
from codecs import StreamReader
from contextlib import suppress

import os
from singerly_airflow.pipeline import Pipeline
from singerly_airflow.utils import get_package_name
from singerly_airflow.venv import Venv


class PipelineExecutionFailedException(Exception):
    pass


class Executor:
    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.work_dir = "/tmp"

    async def enqueue_logs(self, reader: asyncio.StreamReader):
        while not reader.at_eof():
            line = await reader.readline()
            if not line:
                continue
            line_decoded = line.decode("utf-8").strip()
            if line_decoded:
                await self.logs_queue.put(line_decoded)
        await self.logs_queue.put(None)

    async def process_logs_queue(self):
        while True:
            line = await self.logs_queue.get()
            if not line:
                break
            print(line)

    async def enqueue_stream_data(
        self, queue: asyncio.Queue, reader: asyncio.StreamReader
    ):
        while not reader.at_eof():
            line = await reader.readline()
            if not line:
                continue
            await queue.put(line)
        await queue.put(None)

    async def process_stream_queue(self, writer: asyncio.StreamWriter):
        while True:
            line = await self.stream_queue.get()
            if not line:
                break
            try:
                writer.write(line)
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError):
                with suppress(AttributeError):
                    writer.close()
                    await writer.wait_closed()
                    return
        with suppress(AttributeError):
            writer.close()
            await writer.wait_closed()
            return

    async def process_state_queue(self):
        while True:
            line = await self.state_queue.get()
            if not line:
                break
            with suppress(AttributeError):
                line_decoded = line.decode("utf-8").splitlines()[-1]
                if line_decoded:
                    self.pipeline.pipeline_state = line_decoded

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

    async def check_target_process(self, process: Process):
        while True:
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass
                # print("status is ", process.returncode)
            finally:
                if process.returncode is not None:
                    # print("exited with code", process.returncode)
                    with suppress(BaseException):
                        process.stdin.close()
                        process.terminate()
                        process.stdout.feed_eof()
                        process.stderr.feed_eof()
                    return

    async def execute(self):
        if not self.pipeline.is_valid():
            return
        os.chdir(self.work_dir)
        self.logs_queue = asyncio.Queue()
        self.stream_queue = asyncio.Queue()
        self.state_queue = asyncio.Queue()

        await self.install_connectors()

        self.pipeline.process_uploaded_files()

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
            stdout=asyncio.subprocess.PIPE,
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
            self.enqueue_stream_data(reader=tap_proc.stdout, queue=self.stream_queue)
        )
        target_state_enqueue_task = asyncio.create_task(
            self.enqueue_stream_data(reader=target_proc.stdout, queue=self.state_queue)
        )
        process_logs_task = asyncio.create_task(self.process_logs_queue())
        process_stream_task = asyncio.create_task(
            self.process_stream_queue(writer=target_proc.stdin)
        )
        process_state_task = asyncio.create_task(self.process_state_queue())

        logs_tasks = asyncio.gather(
            tap_logs_enqueue_task,
            target_logs_enqueue_task,
            process_logs_task,
        )

        stream_tasks = asyncio.gather(
            target_stream_enqueue_task,
            target_state_enqueue_task,
            process_stream_task,
            process_state_task,
        )

        state_tasks = asyncio.gather(
            target_state_enqueue_task,
            process_state_task,
        )

        asyncio.create_task(self.check_target_process(target_proc))

        await asyncio.wait(
            [stream_tasks, state_tasks], return_when=asyncio.ALL_COMPLETED
        )

        await self.logs_queue.put(None)
        await self.stream_queue.put(None)
        await self.state_queue.put(None)

        with suppress(AttributeError, ProcessLookupError, OSError):
            target_proc.stdin.close()
            await target_proc.stdin.wait_closed()
            tap_proc.terminate()
            target_proc.terminate()

        print("Syncing state")
        self.pipeline.save_state()

        await asyncio.wait(
            [tap_proc.wait(), target_proc.wait()], return_when=asyncio.FIRST_COMPLETED
        )

        logs_tasks.cancel()
        await logs_tasks

        if tap_proc.returncode > 0 or target_proc.returncode > 0:
            message = (
                "Tap failed to run"
                if tap_proc.returncode > 0
                else "Target failed to run"
            )
            raise PipelineExecutionFailedException(message)

        print("Finished pipeline execution")
