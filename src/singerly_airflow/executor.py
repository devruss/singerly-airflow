import asyncio
from asyncio.subprocess import Process
from codecs import StreamReader
from contextlib import suppress
import json

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
            # print("[LOGS_QUEUE] Got line", line)
            if not line:
                continue
            line_decoded = line.decode("utf-8").strip()
            if line_decoded:
                await self.logs_queue.put(line_decoded)

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
            # print("[STREAM_QUEUE] Got line", line)
            try:
                json.loads(line)
                await queue.put(line)
            except:
                continue
        await queue.put(None)

    async def process_stream_queue(self, writer: asyncio.StreamWriter):
        while True:
            line = await self.stream_queue.get()
            # print("[STR] Got line", line)
            if not line:
                break
            try:
                # print(f"[Stream]", line.decode("utf-8"))
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
        self.target_venv = Venv(
            "target", package_url=self.pipeline.target_url, work_dir=self.work_dir
        )
        await self.tap_venv.install_package()
        print(
            f"Installing destination connector: {get_package_name(self.pipeline.target_url)}"
        )
        await self.target_venv.install_package()
        # await asyncio.gather(
        #     self.tap_venv.install_package(), self.target_venv.install_package()
        # )

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
        tap_stream_enqueue_task = asyncio.create_task(
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

        tap_stream_tasks = asyncio.gather(
            process_stream_task,
            process_state_task,
            tap_stream_enqueue_task,
        )

        target_stream_tasks = asyncio.gather(
            target_state_enqueue_task,
        )

        state_tasks = asyncio.gather(
            target_state_enqueue_task,
            process_state_task,
        )

        tap_future = asyncio.ensure_future(tap_proc.wait())
        target_future = asyncio.ensure_future(target_proc.wait())

        # asyncio.create_task(self.check_target_process(target_proc))

        await asyncio.wait(
            [
                tap_stream_tasks,
                target_stream_tasks,
                state_tasks,
                tap_future,
                target_future,
            ],
            return_when=asyncio.FIRST_EXCEPTION,
        )
        print("Finished stream processing")

        await self.logs_queue.put(None)
        await self.stream_queue.put(None)
        await self.state_queue.put(None)

        # with suppress(AttributeError, ProcessLookupError, OSError):
        #     target_proc.stdin.close()
        #     await target_proc.stdin.wait_closed()
        #     tap_proc.terminate()
        #     target_proc.terminate()

        done, _ = await asyncio.wait(
            [tap_future, target_future], return_when=asyncio.FIRST_COMPLETED
        )

        if target_future in done:
            target_code = target_future.result()

            if tap_future in done:
                tap_code = tap_future.result()
            else:
                # If the target completes before the tap, it failed before processing all tap output

                # Kill tap and cancel output processing since there's no more target to forward messages to
                tap_proc.kill()
                await tap_future
                await self.logs_queue.put(None)
                await self.stream_queue.put(None)
                await self.state_queue.put(None)

                tap_stream_tasks.cancel()

                # Pretend the tap finished successfully since it didn't itself fail
                tap_code = 0

            # Wait for all buffered target output to be processed
            await asyncio.wait([target_stream_tasks])
        else:  # if tap_process_future in done:
            # If the tap completes before the target, the target should have a chance to process all tap output
            tap_code = tap_future.result()

            # Wait for all buffered tap output to be processed
            await asyncio.wait([tap_stream_tasks, tap_logs_enqueue_task])

            # Close target stdin so process can complete naturally
            target_proc.stdin.close()
            with suppress(AttributeError):  # `wait_closed` is Python 3.7+
                await target_proc.stdin.wait_closed()

            # Wait for all buffered target output to be processed
            await asyncio.wait([target_stream_tasks])

            # Wait for target to complete
            target_code = await target_future

            print("Syncing state")
            self.pipeline.save_state()

        if tap_code and target_code:
            raise PipelineExecutionFailedException(
                "Tap and target failed",
            )
        elif tap_code:
            raise PipelineExecutionFailedException(f"Tap failed with code {tap_code}")
        elif target_code:
            raise PipelineExecutionFailedException(
                "Target failed with code {target_code}"
            )

        logs_tasks.cancel()
        await logs_tasks

        # if (tap_proc.returncode is not None) and (tap_proc.returncode > 0):
        #     message = "Tap failed to run"
        #     raise PipelineExecutionFailedException(message)

        # if (target_proc.returncode is not None) and (target_proc.returncode > 0):
        #     message = "Target failed to run"
        #     raise PipelineExecutionFailedException(message)

        print("Finished pipeline execution")
