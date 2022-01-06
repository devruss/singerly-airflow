import subprocess
from typing import List
import boto3
import os
from dataclasses import dataclass
from singerly_airflow.utils import timed_lru_cache, get_package_name
from singerly_airflow.venv import Venv
from urllib.parse import urlparse
import re

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
  tap_executable: str = ''
  target_executable: str = ''
  email_list: str = ''
  is_enabled: bool = False
  schedule: str = '@dayli'

  def save_state(self, state: str) -> None:
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(self.project_id)
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

  def get_tap_executable(self) -> str:
    if self.tap_executable:
      return self.tap_executable
    return get_package_name(package_url=self.tap_url)

  def get_target_executable(self) -> str:
    if self.target_executable:
      return self.target_executable
    return get_package_name(package_url=self.target_url)

  def execute(self) -> None:
    if not self.is_valid():
      return
    work_dir = '/tmp'
    os.chdir(work_dir)
    print(f'Installing source connector: {get_package_name(self.tap_url)}')
    tap_venv = Venv('tap', package_url=self.tap_url, work_dir=work_dir)
    print(f'Installing destination connector: {get_package_name(self.target_url)}')
    target_venv = Venv('target', package_url=self.target_url, work_dir=work_dir)
    with open(f'{os.getcwd()}/tap_config.json', 'w') as tap_config_file:
      tap_config_file.write(self.tap_config)
    with open(f'{os.getcwd()}/target_config.json', 'w') as target_config_file:
      target_config_file.write(self.target_config)
    with open(f'{os.getcwd()}/catalog.json', 'w') as catalog_file:
      catalog_file.write(self.tap_catalog)
    tap_run_args = [
      f'{tap_venv.get_bin_dir()}/{self.get_tap_executable()}',
      '-c', 'tap_config.json',
      '--catalog', 'catalog.json',
      '-p', 'catalog.json'
    ]
    if self.pipeline_state:
      with open(f'{os.getcwd()}/tap_state.json', 'w') as tap_state_file:
        tap_state_file.write(self.pipeline_state)
      tap_run_args.extend(['-s', 'tap_state.json'])
    target_run_args = [
      f'{target_venv.get_bin_dir()}/{self.get_target_executable()}',
      '-c', 'target_config.json',
    ]
    print(f'Starting pipeline execution {self.get_tap_executable()} -> {self.get_target_executable()}')
    tap_process = subprocess.Popen(tap_run_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    target_process = subprocess.Popen(target_run_args, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)

    for line in tap_process.stdout:
      if not line:
        break
      decoded_line = line.decode('utf-8').strip()
      if decoded_line:
        print(decoded_line)
      target_process.stdin.write(line)
    
    stdout, stderr = target_process.communicate()
    tap_process.communicate()

    stdout_decoded = stdout.decode('utf-8').strip()
    if stdout_decoded:
      print(stdout_decoded)
      self.save_state(stdout_decoded)
    if stderr and stderr.decode('utf-8'):
      print(stderr.decode('utf-8'))

  def is_valid(self) -> bool:
    return (self.tap_config
      and self.tap_url
      and self.tap_catalog
      and self.target_url
    )


@timed_lru_cache(seconds=30)
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
