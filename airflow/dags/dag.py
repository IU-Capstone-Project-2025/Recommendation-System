import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.variable import Variable

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 0 * * *',
    start_date=datetime(2025, 6, 11),
    catchup=False,
    tags=['stg', 'origin', 'my', 'postgres'],
    is_paused_upon_creation=True
)
def stg_dag():
    return

stg_dag = stg_dag()
