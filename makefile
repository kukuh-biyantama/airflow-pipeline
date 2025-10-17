help:
	@echo "## build			- Build Docker Images (amd64) including its inter-container network."
	@echo "## spinup		- Spinup airflow, postgres, and metabase."
	@echo "## init			- Initialize .env file with default values."

init:
	@echo '__________________________________________________________'
	@echo 'Creating .env file ...'
	@echo '__________________________________________________________'
	@if [ -f .env ]; then \
		echo '.env already exists. Skipping.'; \
	else \
		echo "AIRFLOW_CONTAINER_NAME=airflow-local" > .env; \
		echo "AIRFLOW_HOST_NAME=airflow" >> .env; \
		echo "AIRFLOW_WEBSERVER_PORT=8080" >> .env; \
	fi
	@echo '==========================================================='

build:
	@echo '__________________________________________________________'
	@echo 'Building Docker Images ...'
	@echo '__________________________________________________________'
	@docker build -t dataeng-dibimbing/airflow -f ./docker/Dockerfile.airflow .
	@echo '==========================================================='

spinup:
	@echo '__________________________________________________________'
	@echo 'Creating Instances ...'
	@echo '__________________________________________________________'
	@chmod +x scripts/airflow_entrypoint.sh
	@docker compose -f ./docker/docker-compose.yml --env-file .env up
	@echo '==========================================================='

clean:
	@bash ./scripts/goodnight.sh