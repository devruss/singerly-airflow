import os
import subprocess
from singerly_airflow.utils import get_package_name

from singerly_airflow.venv import Venv


class CatalogConnectorExecutionException(Exception):
    pass


def generate_catalog(tap_url, tap_config, tap_executable=""):
    os.chdir("/tmp")
    tap_executable = tap_executable if tap_executable else get_package_name(tap_url)
    tap_venv = Venv("tap", package_url=tap_url, work_dir="/tmp")
    with open(f"{os.getcwd()}/tap_config.json", "w") as tap_config_file:
        tap_config_file.write(tap_config)
    tap_run_args = [
        f"{tap_venv.get_bin_dir()}/{tap_executable}",
        "-c",
        "tap_config.json",
        "--discover",
    ]
    tap_process = subprocess.Popen(
        tap_run_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = tap_process.communicate()
    if tap_process.returncode != 0:
        raise CatalogConnectorExecutionException(stderr.decode("utf-8"))
    return stdout.decode("utf-8")
