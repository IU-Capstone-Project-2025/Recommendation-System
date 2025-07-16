import logging
from datetime import datetime

from airflow.decorators import dag, task
from lib.pg_connect import PgConnectionBuilder
from lib.csv_book_loader import CSVBookLoader

# Initialize logger for the DAG
log = logging.getLogger(__name__)


@dag(
    schedule_interval="@once",  # Run this DAG only once
    start_date=datetime(2025, 6, 11),  # Initial execution date
    catchup=False,  # Disable catchup to prevent backfilling
    tags=["books", "postgres", "csv"],  # Organizational tags for filtering
    is_paused_upon_creation=False,  # Start DAG immediately upon creation
)
def book_to_origin_dag():
    """
    Airflow DAG for loading book data from CSV file into PostgreSQL database.

    This DAG performs a one-time load of book data including:
    - Book metadata (title, author, publication year, etc.)
    - Book types
    - Genres and genre relationships
    - Tags and tag relationships

    The CSV file is expected to be located at /opt/airflow/data/books.csv
    and should contain all required fields for complete book data loading.
    """

    # Initialize PostgreSQL connection using Airflow connection ID
    origin_pg_connect = PgConnectionBuilder.pg_conn("POSTGRES_DEFAULT")

    @task(task_id="books_load")
    def load_book():
        """
        Task that loads book data from CSV file into PostgreSQL database.

        Uses CSVBookLoader to:
        1. Parse the CSV file
        2. Transform data as needed
        3. Load books and related entities into PostgreSQL
        4. Establish all necessary relationships

        The CSV path is hardcoded to /opt/airflow/data/books.csv which is
        the standard location for data files in Airflow containers.
        """
        rest_loader = CSVBookLoader(
            csv_file="/opt/airflow/data/books.csv", pg_dest=origin_pg_connect, log=log
        )
        rest_loader.load_book()

    # Execute the book loading task
    book_to_origin = load_book()

    # Return the task reference to establish the DAG structure
    book_to_origin


# Instantiate the DAG
book_to_origin_dag = book_to_origin_dag()
