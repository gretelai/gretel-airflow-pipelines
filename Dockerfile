FROM apache/airflow:2.1.2-python3.9

USER root
RUN apt-get update && apt-get install -y awscli jq git

USER airflow
RUN pip install --no-cache-dir git+https://github.com/gretelai/gretel-python-client@main
