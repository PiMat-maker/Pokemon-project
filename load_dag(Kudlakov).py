from sched import scheduler
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import info_collector

import json


AWS_CONN_ID = "s3"
SNOWPIPE_FILES_PREFIX = "{{var.value.snowpipe_files}}"
UNPROCESSED_FILES_PREFIX = "{{var.value.unprocessed_files}}"
PROCESSED_FILES_PREFIX = "{{var.value.processed_files}}"
POKEMON_API = "https://pokeapi.co/api/v2/"
POKEMON_FILENAME = "pokemon.json"
GENERATION_FILENAME = "generation.json"
TYPE_FILENAME = "type.json"


def __get_bucket_and_key(s3_prefix) -> "list[str]":
    bucket, key = S3Hook.parse_s3_url(s3_prefix)
    return [bucket, key]


def _print_message(message: str) -> None:
    print(message)


def _choose_branch(default_tasks: "list[str]", **kwards) -> "list[str]":
    config = kwards.get('dag_run').conf
    if 'copy' in config and config['copy']:
        return ['copy_generation_info', 'copy_type_info', 'copy_pokemon_info']

    return default_tasks


def _load_string_on_s3(data: str, key: str) -> None:
    s3hook = S3Hook()
    s3hook.load_string(string_data=data, key=key)


def _remove_file_on_s3(bucket, key):
    s3hook = S3Hook()
    s3hook.delete_objects(bucket=bucket, keys=key)


def _remove_files_on_s3(bucket, key, filenames: "list[str]"):
    for filename in filenames:
        _remove_file_on_s3(bucket, f'{key}{filename}')


def _copy_info_from_api_to_s3(api_url: str, s3_prefix: str, what_info: str, filename: str, offset: int = 0, saved_info: "list[dict(str, str)]" = []):
    bucket, key = __get_bucket_and_key(f'{s3_prefix}Kudlakov/')

    info = info_collector.copy_info_from_api(api_url, what_info, offset)
    info_in_dict = info_collector.convert_list_object_to_dict(info)
    full_info_in_dict = saved_info + info_in_dict
    info_json_str = info_collector.convert_to_json_str(full_info_in_dict)
   
    _remove_files_on_s3(bucket, key, [filename])

    s3_key = f"s3://{bucket}/{key}{filename}"
    _load_string_on_s3(info_json_str, s3_key)


def __get_info_from_json_file_on_s3(bucket: str, key: str, keys: "list[str]", filename: str) -> "list[dict(str, str)]":
    s3hook = S3Hook()
    if f'{key}{filename}' in keys:
        json_str = s3hook.read_key(key=f'{key}{filename}', bucket_name=bucket)
        return json.loads(json_str)

    return []


def _load_new_info_from_api_to_s3(api_url: str, s3_prefix: str, what_info: str, filename: str):
    bucket, key = __get_bucket_and_key(f'{s3_prefix}Kudlakov/')
    s3hook = S3Hook()
    keys = s3hook.list_keys(bucket_name=bucket, prefix=key, delimiter='/')

    info_in_dict = __get_info_from_json_file_on_s3(bucket, key, keys, filename)
    
    amount = len(info_in_dict)
    _copy_info_from_api_to_s3(api_url, s3_prefix, what_info, filename, offset=amount, saved_info=info_in_dict)


def _clean_directory(s3_prefix) -> None:
    bucket, key = __get_bucket_and_key(f'{s3_prefix}Kudlakov/')
    s3hook = S3Hook()
    keys = s3hook.list_keys(bucket_name=bucket, prefix=key, delimiter='/')

    s3hook.delete_objects(bucket=bucket, keys=keys)


with DAG(
    dag_id='load_dag_Kudlakov',
    schedule_interval=None,
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
        task_id="branch",
        python_callable=_choose_branch,
        op_args=[['load_new_generation_info', 'load_new_type_info', 'load_new_pokemon_info']]
    )

    start_task >> branch_task

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

    for filename in [GENERATION_FILENAME, TYPE_FILENAME, POKEMON_FILENAME]:
        what_info = filename[:filename.rfind(".")]
        copy_info_task = PythonOperator(
            task_id=f'copy_{what_info}_info',
            python_callable=_copy_info_from_api_to_s3,
            op_args=[POKEMON_API, SNOWPIPE_FILES_PREFIX, what_info, filename]
        )
        branch_task >> copy_info_task >> [success_task, failed_task]


    for filename in [GENERATION_FILENAME, TYPE_FILENAME, POKEMON_FILENAME]:
        what_info = filename[:filename.rfind(".")]
        load_new_info_task = PythonOperator(
            task_id=f'load_new_{what_info}_info',
            python_callable=_load_new_info_from_api_to_s3,
            op_args=[POKEMON_API, SNOWPIPE_FILES_PREFIX, what_info, filename]
        )
        branch_task >> load_new_info_task >> [success_task, failed_task]
