import logging
from datetime import datetime

from airflow.decorators import dag, task

from lib.pg_connect import ConnectionBuilder
from lib.csv_book_loader import CSVBookLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 * * * *',
    start_date=datetime(2025, 6, 11),
    catchup=False,
    tags=['books', 'postgres', 'csv'],
    is_paused_upon_creation=True
)
def book_to_origin_dag():
    origin_pg_connect = ConnectionBuilder.pg_conn("POSTGRES_DEFAULT")

    @task(task_id="books_load")
    def load_book():
        rest_loader = CSVBookLoader("/opt/airflow/data/books.csv", origin_pg_connect, log)
        rest_loader.load_book()
    
    book_to_origin = load_book()

    book_to_origin

book_to_origin_dag = book_to_origin_dag()
