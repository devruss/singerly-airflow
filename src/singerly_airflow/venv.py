import asyncio
import subprocess
import os


class VenvPythonMissingException(Exception):
    """Python v3 is not found"""

    pass


class VenvPackageInstallException(Exception):
    """Package could not be installed"""

    pass


class Venv:
    def __init__(self, name: str, package_url: str = "", work_dir="/var/run"):
        os.chdir(work_dir)
        self.name = name
        self.package_url = package_url
        python_check = subprocess.run(
            ["which", "python3"], capture_output=True, text=True
        )
        if python_check.returncode != 0:
            raise VenvPythonMissingException("Python v3 is missing")
        self.python_bin = python_check.stdout.strip()
        self.pip_bin = f"{os.getcwd()}/{self.name}/bin/pip3"

    async def install_package(self):
        venv_setup = await asyncio.subprocess.create_subprocess_exec(
            self.python_bin, *["-m", "venv", self.name], stdout=None, stderr=None
        )
        await venv_setup.wait()
        package_install = await asyncio.subprocess.create_subprocess_exec(
            self.pip_bin,
            *["install", self.package_url],
            # stdout=asyncio.subprocess.PIPE,
            # stderr=asyncio.subprocess.PIPE,
        )
        await package_install.wait()
        if package_install.returncode != 0:
            # print(package_install.stderr, package_install.stdout)
            raise VenvPackageInstallException(package_install.stderr)

    def get_bin_dir(self) -> str:
        return f"{os.getcwd()}/{self.name}/bin"


if __name__ == "__main__":
    venv = Venv("test_venv")
    venv.setup()
    venv.install_package("apache_airflow")
