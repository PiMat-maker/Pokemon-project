from sched import scheduler
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import json
import requests


AWS_CONN_ID = "s3"
SNOWPIPE_FILES_PREFIX = "{{var.value.snowpipe_files}}"
UNPROCESSED_FILES_PREFIX = "{{var.value.unprocessed_files}}"
PROCESSED_FILES_PREFIX = "{{var.value.processed_files}}"
GENERATIONS_FILENAME = "generation.json"
POKEMON_API = "https://pokeapi.co/api/v2/"


def __get_bucket_and_key(s3_prefix) -> "list[str]":
    bucket, key = S3Hook.parse_s3_url(s3_prefix)
    return [bucket, key]


def _print_message(message: str) -> None:
    print(message)


def __get_json_key_by_filename(filename: str, keys: "list[str]") -> str:
    for obj_key in keys: 
        if filename in obj_key:
            return obj_key

    return ""


def __get_api_file(url_prefix: str, filename: str):
    extension_position = filename.rfind(".")
    filename_without_extension = filename[:extension_position]

    url = f"{url_prefix}{filename_without_extension}"
    response = requests.get(url)
    return response.json()


def _check_new_generations(s3_prefix: str, generations_filename: str, url_prefix: str) -> "list[str]":
    bucket, key = __get_bucket_and_key(f'{s3_prefix}Kudlakov/')
    s3hook = S3Hook()
    keys = s3hook.list_keys(bucket_name=bucket, prefix=key, delimiter='/')

    json_key = __get_json_key_by_filename(generations_filename, keys)
    
    if json_key == "":
        #Later here will be logger
        _print_message("There is no generations file. So we upload it now")
        return ['trigger_load_dag']
    
    generations_info_json_str = s3hook.read_key(key=json_key, bucket_name=bucket)
    generations_info = json.loads(generations_info_json_str)

    generations_json_api = __get_api_file(url_prefix, generations_filename)

    if len(generations_info) != generations_json_api["count"]:
        return ['trigger_load_dag']

    return ['no_new_generations']


with DAG(
    dag_id='check_generations_dag',
    schedule_interval='@daily',
    start_date=days_ago(2),
    catchup=False,
    max_active_runs=1,
    tags=['Kudlakov', 'pet-project']
) as dag:
    start_task = PythonOperator(
        task_id='start',
        python_callable=_print_message,
        op_args=['Dag has started!']
    )

    branch_task = BranchPythonOperator(
        task_id='branch',
        python_callable=_check_new_generations,
        op_args=[SNOWPIPE_FILES_PREFIX, GENERATIONS_FILENAME, POKEMON_API]
    )

    no_new_generations_task = PythonOperator(
        task_id='no_new_generations',
        python_callable=_print_message,
        op_args=["There is no new generations on POKEMON API"]
    )

    trigger_load_dag = TriggerDagRunOperator(
        task_id='trigger_load_dag',
        trigger_dag_id='load_dag_Kudlakov'
    )

    success_task = PythonOperator(
        task_id='success',
        python_callable=_print_message,
        op_args=[f'Dag has finished successfully'],
        trigger_rule=TriggerRule.NONE_FAILED
    )

    failed_task = PythonOperator(
        task_id='failed',
        python_callable=_print_message,
        op_args=['Dah has failed'],
        trigger_rule=TriggerRule.ALL_FAILED
    )

    start_task >> branch_task >> [no_new_generations_task, trigger_load_dag]
    no_new_generations_task >> [success_task, failed_task]
    trigger_load_dag >> [success_task, failed_task]