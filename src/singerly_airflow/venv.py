import subprocess
import os


class VenvPythonMissingException(Exception):
  """Python v3 is not found"""


class VenvPackageInstallException(Exception):
  """Package could not be installed"""


class Venv:
  def __init__(self, name: str, package_url: str = ''):
    self.name = name
    python_check = subprocess.run(['which', 'python3'], capture_output=True, text=True)
    if python_check.returncode != 0:
      raise VenvPythonMissingException('Python v3 is missing')
    self.python_bin = python_check.stdout.strip()
    self.pip_bin = f'{os.getcwd()}/{self.name}/bin/pip3'
    self._setup()
    if (package_url):
      self.install_package(package_url)
  
  def _setup(self):
    subprocess.run([self.python_bin, '-m', 'venv', self.name], stdout=None, stderr=None)

  def install_package(self, package_url):
    package_install = subprocess.run([self.pip_bin, 'install', package_url])
    if package_install.returncode != 0:
      raise VenvPackageInstallException()

  def get_bin_dir(self) -> str:
    return f'{os.getcwd()}/{self.name}/bin'


if __name__ == '__main__':
  venv = Venv('test_venv')
  venv.setup()
  venv.install_package('apache_airflow')