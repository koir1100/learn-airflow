import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError

from http import HTTPStatus
import logging

url = "http://localhost:8080/api/v1/dags"

try:
    dags = requests.get(url, auth=HTTPBasicAuth("airflow", "airflow"))
    if dags.status_code != HTTPStatus.OK:
        raise dags.raise_for_status()
except HTTPError as e:
    logging.error(e)
    raise

print(dags.text)
