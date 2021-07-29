import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from smart_open import open

from gretel_client.helpers import poll

from hooks.gretel import GretelHook


default_args = {
    "owner": "airflow",
}

gretel_api = GretelHook()
logger = logging.getLogger(__name__)

config = "transforms/redact-sensitive-pii"
in_data = "https://gretel-public-website.s3.us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv"

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["gretel"],
)
def gretel_task_flow_example():
    @task()
    def create_model() -> dict:
        project = gretel_api.create_project(display_name="airflow-taskapi-eg")
        model = project.create_model_obj(model_config=config, data_source=in_data)
        model.submit_cloud()
        poll(model)
        return {"model_id": model.model_id, "project_id": project.project_id}


    @task()
    def run_model(model_def: dict) -> str:
        project = gretel_api.get_project(name=model_def["project_id"])
        model = project.get_model(model_id=model_def["model_id"])
        record_handler = model.create_record_handler_obj()
        record_handler.submit(action="transform", data_source=in_data)
        poll(record_handler)
        return record_handler.get_artifact_link("data")

    @task()
    def upload_data_set(data_set: str):
        with open(data_set, "rb") as data:
            logger.info(data.read()[:1000])


    model_def = create_model()
    data_set = run_model(model_def)
    upload_data_set(data_set)


pipeline_dag = gretel_task_flow_example()
