import os


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


args = {
    "owner": "airflow",
}

default_env = {
    "GRETEL_API_KEY": Variable.get("GRETEL_API_KEY"),
    **os.environ,
}

config = "transforms/redact-sensitive-pii"
in_data = "https://gretel-public-website.s3.us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv"


with DAG(
    dag_id="gretel_cli_pipeline_example",
    schedule_interval="@once",
    start_date=days_ago(2),
    catchup=False,
    default_args=args,
) as dag:

    project_create = BashOperator(
        task_id="project_create",
        bash_command="gretel projects create --display-name airflow-test | jq -r .project._id",
        env=default_env,
    )

    model_create = BashOperator(
        task_id="model_create",
        bash_command=(
            "gretel models create "
            "--config {{params.config}} "
            "--in-data {{params.in_data}} "
            "--project {{ti.xcom_pull(task_ids='project_create')}} "
            "| jq -r .uid"
        ),
        env=default_env,
        params={"config": config, "in_data": in_data},
    )

    model_run = BashOperator(
        task_id="model_run",
        bash_command=("gretel records transform "
            "--project {{ti.xcom_pull(task_ids='project_create')}} "
            "--model-id {{ ti.xcom_pull(task_ids='model_create') }} "
            "--in-data {{params.in_data}}"
        ),
        params={"in_data": in_data},
        env=default_env
    )

    project_create >> model_create >> model_run
