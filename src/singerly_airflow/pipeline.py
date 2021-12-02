import subprocess
from typing import List
import boto3
import os
from dataclasses import dataclass
from singerly_airflow.utils import timed_lru_cache
from singerly_airflow.venv import Venv

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
  tap_executable: str = ''
  target_executable: str = ''
  email_list: str = ''
  is_enabled: bool = False

  def save_state(self, state: str) -> None:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('test-pipeline')
    table.update_item(
      Key={
        'id': self.id,
      },
      UpdateExpression="set pipeline_state=:state",
      ExpressionAttributeValues={
        ':state': state
      }
    )

  def get_email_list(self):
    if not self.email_list:
      return []
    return [email.strip() for email in self.email_list.split(',')]

  def get_package_name(self, package_url) -> str:
    return package_url.split('/')[-1].replace('.git', '')

  def get_tap_executable(self) -> str:
    if self.tap_executable:
      return self.tap_executable
    return self.get_package_name(package_url=self.tap_url)

  def get_target_executable(self) -> str:
    if self.target_executable:
      return self.target_executable
    return self.get_package_name(package_url=self.target_url)

  def generate_catalog(self):
    os.chdir('/tmp')
    tap_venv = Venv('tap', package_url=self.tap_url, work_dir='/tmp')
    with open(f'{os.getcwd()}/tap_config.json', 'w') as tap_config_file:
      tap_config_file.write(self.tap_config)
    tap_run_args = [
      f'{tap_venv.get_bin_dir()}/{self.get_tap_executable()}',
      '-c', 'tap_config.json',
      '--discover'
    ]
    tap_process = subprocess.Popen(tap_run_args, stdout=subprocess.PIPE)
    stdout = tap_process.communicate()[0]
    self.save_catalog(stdout.decode('utf-8'))

  def save_catalog(self, catalog: str):
    self.tap_catalog = catalog;
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(self.project_id)
    table.update_item(
        Key={
            'id': self.id,
        },
        UpdateExpression="set tap_catalog = :catalog",
        ExpressionAttributeValues={
            ':catalog': catalog,
        },
    )

  def execute(self) -> None:
    if not self.is_valid():
      return
    os.chdir('/var/run')
    tap_venv = Venv('tap', package_url=self.tap_url)
    target_venv = Venv('target', package_url=self.target_url)
    with open(f'{os.getcwd()}/tap_config.json', 'w') as tap_config_file:
      tap_config_file.write(self.tap_config)
    with open(f'{os.getcwd()}/target_config.json', 'w') as target_config_file:
      target_config_file.write(self.target_config)
    with open(f'{os.getcwd()}/catalog.json', 'w') as catalog_file:
      catalog_file.write(self.tap_catalog)
    tap_run_args = [
      f'{tap_venv.get_bin_dir()}/{self.get_tap_executable()}',
      '-c', 'tap_config.json',
      '-p', 'catalog.json'
    ]
    if self.pipeline_state:
      with open(f'{os.getcwd()}/tap_state.json', 'w') as tap_state_file:
        tap_state_file.write(self.pipeline_state)
      tap_run_args.extend(['-s', 'tap_state.json'])
    target_run_args = [
      f'{target_venv.get_bin_dir()}/{self.get_target_executable()}'
      '-c', 'target_config.json',
    ]
    tap_process = subprocess.Popen(tap_run_args, stdout=subprocess.PIPE)
    target_process = subprocess.Popen(target_run_args, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

    while True:
      next_line = tap_process.stdout.readline()
      if not next_line:
        break
      print(next_line.decode('utf-8'))
      target_process.stdin.write(next_line)
    
    stdout = target_process.communicate()[0]
    print(stdout)
    self.save_state(stdout.decode('utf-8'))

  def is_valid(self) -> bool:
    return (self.tap_config
      and self.tap_url
      and self.catalog
      and self.target_url
    )


def get_pipeline(project_id: str, id: str) -> Pipeline:
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(project_id)
  pipeline_raw = table.get_item(Key={
    'id': id
  })['Item']
  return Pipeline(project_id=project_id, **pipeline_raw)


@timed_lru_cache(seconds=30)
def get_pipelines(project_id: str) -> List[Pipeline]:
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(project_id)
  result = table.scan()
  return [Pipeline(project_id=project_id, **pipeline_raw) for pipeline_raw in result['Items']]
