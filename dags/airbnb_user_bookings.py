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

# configure external connections
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

        # read in the sql query to run
        sql_query = Path(sql_file).read_text()

        # we'll write the result to the configured destination bucket
        # eg manual__2021-09-13T06:12:00.105099+00:00_booking_features.csv
        key = f"{context['dag_run'].run_id}_booking_features.csv"
        with NamedTemporaryFile(mode="r+", suffix=".csv") as tmp_csv:

            # this queries the databases and streams the output
            # results to a csv file
            # docs: psycopg.org/docs/cursor.html#cursor.copy_expert
            postgres.copy_expert(
                f"copy ({sql_query}) to stdout with csv header", tmp_csv.name
            )

            # the csv file is then uploaded to s3
            s3.load_file(
                filename=tmp_csv.name,
                key=key,
            )

        # a key to the file is then returned
        # eg manual__2021-09-13T06:12:00.105099+00:00_booking_features.csv
        return key

    @task()
    def generate_synthetic_features(data_source: str) -> str:


        # using the gretel hook, we will grab the project
        project = gretel.get_project()

        # create the synthetic model using the default configuration
        model = project.create_model_obj(
            model_config="synthetics/default", data_source=s3.download_file(data_source)
        )

        # submit the model for training to Gretel Cloud
        model.submit_cloud()

        # this method will block until the model is done training
        # and generating records.
        poll(model)
        assert model.status == "completed"

        # returns a signed, authenticated link to the generated
        # synthetic file
        return model.get_artifact_link("data_preview")

    @task()
    def upload_synthetic_features(data_set: str):
        context = get_current_context()


        with open(data_set, "rb") as synth_features:
            s3.load_file_obj(
                file_obj=synth_features,
                key=f"{context['dag_run'].run_id}_booking_features_synthetic.csv",
            )

    # extract
    feature_path = extract_features("/opt/airflow/dags/sql/session_rollups__by_user.sql")
    # synthesize
    synthetic_data = generate_synthetic_features(feature_path)
    # load
    upload_synthetic_features(synthetic_data)


pipeline_dag = gretel_synthetics_airbnb_bookings()
