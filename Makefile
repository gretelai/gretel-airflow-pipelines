configure:
	@cp .env.sample .env
	@echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >> .env
	mkdir -p logs/

setup:
	docker-compose up airflow-init

start:
	docker-compose up

stop:
	docker-compose down

clean:
	docker-compose down --volumes --rmi all

build:
	docker-compose build --no-cache

seed:
	docker-compose run --entrypoint bash -w /data airflow-worker load.sh
