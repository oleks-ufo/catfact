import requests
from pendulum import datetime

from airflow import Dataset
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator
)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "Headway", "retries": 0},
    tags=["catfact"],
)
def catfact_dag():

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        gcp_conn_id='gcp_conn',
        dataset_id='ae_sandbox',
        table_id="catfact",
        schema_fields=[
            {"name": "fact", "type": "STRING", "mode": "NULLABLE"},
            {"name": "length", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    for worker in range(1, 3):

        @task(task_id=f"pull_fact_from_api_{worker}")
        def pull_fact_from_api(**context) -> dict:

            response = requests.get("https://catfact.ninja/fact").json()

            print('-' * 100)
            print(response)
            print('-' * 100)

            return response

        insert_data = BigQueryInsertJobOperator(
            task_id=f"insert_data_{worker}",
            configuration={
                "query": {
                    "query": f"""
                        INSERT INTO `books-us.ae_sandbox.catfact` (fact, length)
                        VALUES (
                            '{{{{ ti.xcom_pull(task_ids="pull_fact_from_api_{worker}")["fact"] }}}}', 
                            '{{{{ ti.xcom_pull(task_ids="pull_fact_from_api_{worker}")["length"] }}}}'
                        )
                    """,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
            location='us-central1',
            gcp_conn_id='gcp_conn'
        )

        create_table >> pull_fact_from_api() >> insert_data


catfact_dag()
