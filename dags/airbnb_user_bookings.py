import logging
from pathlib import Path
from tempfile import NamedTemporaryFile

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from gretel_client.helpers import poll
from hooks.gretel import GretelHook
from smart_open import open

gretel = GretelHook("gretel_default")
postgres = PostgresHook("booking_db")
s3 = S3Hook("s3_default")

logger = logging.getLogger(__name__)


@dag(
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["gretel"],
)
def gretel_synthetics_airbnb_bookings():
    @task()
    def extract_features(sql_file: str) -> str:
        context = get_current_context()
        sql_query = Path(sql_file).read_text()
        key = f"{context['dag_run'].run_id}_booking_features.csv"
        with NamedTemporaryFile(mode="r+", suffix=".csv") as tmp_csv:
            postgres.copy_expert(
                f"copy ({sql_query}) to stdout with csv header", tmp_csv.name
            )
            s3.load_file(
                filename=tmp_csv.name,
                key=key,
            )
        return key

    @task()
    def generate_synthetic_features(data_source: str) -> str:
        project = gretel.get_project()
        model = project.create_model_obj(
            model_config="synthetics/default", data_source=s3.download_file(data_source)
        )
        model.submit_cloud()
        poll(model)
        return model.get_artifact_link("data_preview")

    @task()
    def upload_synthetic_features(data_set: str):
        context = get_current_context()
        with open(data_set, "rb") as synth_features:
            s3.load_file_obj(
                file_obj=synth_features,
                key=f"{context['dag_run'].run_id}_booking_features_synthetic.csv",
            )

    feature_path = extract_features("/opt/airflow/dags/sql/session_rollups__by_user.sql")
    synthetic_data = generate_synthetic_features(feature_path)
    upload_synthetic_features(synthetic_data)


pipeline_dag = gretel_synthetics_airbnb_bookings()
