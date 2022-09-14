# task6
Snowflake pipeline with airflow

You must create separate directory for airflow
$ export AIRFLOW_HOME=path/to/your/project/airflow
Then run terminal in this directory and create new venv
$ conda create -n yourenvname python=x.x anaconda

$ AIRFLOW_VERSION=2.2.0
$ PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
$ CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
$ pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

Then run this comands:
$ airflow db init
$ airflow scheduler
$ airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
$ airflow webserver
$ pip3 install snowflake-connector-python
$ pip3 install snowflake-sqlalchemy
$ pip3 install apache-airflow-providers-snowflake

Create snowflake connection in airflow API
