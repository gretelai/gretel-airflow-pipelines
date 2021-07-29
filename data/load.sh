curl -O https://gretel-public-website.s3.us-west-2.amazonaws.com/gretel-airflow-pipelines/sessions.csv.zip
curl -O https://gretel-public-website.s3.us-west-2.amazonaws.com/gretel-airflow-pipelines/users.csv.zip
psql postgres://airflow:airflow@postgres -a -f /data/seed.sql
