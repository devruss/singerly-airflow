import subprocess
import boto3
import json
import os
from dataclasses import dataclass
from singerly_airflow.venv import Venv

@dataclass
class Pipeline:
  id: int
  name: str
  tap_config: str
  tap_url: str
  target_url: str
  catalog: str
  pipeline_state: str

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

  def get_package_name(self, package_url) -> str:
    return package_url.split('/')[-1].replace('.git', '')

  def execute(self) -> None:
    if not self.is_valid():
      return
    tap_venv = Venv('tap', package_url=self.tap_url)
    target_venv = Venv('target', package_url=self.target_url)
    with open(f'{os.getcwd()}/tap_config.json', 'w') as tap_config_file:
      tap_config_file.write(self.tap_config)
    with open(f'{os.getcwd()}/catalog.json', 'w') as catalog_file:
      catalog_file.write(self.catalog)
    tap_run_args = [
      f'{tap_venv.get_bin_dir()}/{self.get_package_name(self.tap_url)}',
      '-c', 'tap_config.json',
      '-p', 'catalog.json'
    ]
    if self.pipeline_state:
      with open(f'{os.getcwd()}/tap_state.json', 'w') as tap_state_file:
        tap_state_file.write(self.pipeline_state)
      tap_run_args.extend(['-s', 'tap_state.json'])
    tap_process = subprocess.Popen(tap_run_args, stdout=subprocess.PIPE)
    target_process = subprocess.Popen([f'{target_venv.get_bin_dir()}/{self.get_package_name(self.target_url)}'], stdout=subprocess.PIPE, stdin=subprocess.PIPE)

    while True:
      next_line = tap_process.stdout.readline()
      if not next_line:
        break
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


def get_pipeline(id: str) -> Pipeline:
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table('test-pipeline')
  pipeline_raw = table.get_item(Key={
    'id': id
  })['Item']
  return Pipeline(**pipeline_raw)


def get_pipelines(project_id: str) -> Pipeline:
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table(project_id)
  result = table.scan()
  return [Pipeline(**pipeline_raw) for pipeline_raw in result['Items']]
