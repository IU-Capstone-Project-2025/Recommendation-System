import logging
from datetime import datetime

from airflow.decorators import dag, task
from lib.pg_connect import PgConnectionBuilder
from lib.ch_connect import CHConnectionBuilder
from tables.user_loader import UserLoader
from tables.book_loader import BookLoader
from tables.type_loader import BookTypeLoader
from tables.genre_loader import GenreLoader
from tables.tag_loader import BookTagLoader
from tables.score_loader import BookRatingLoader
from tables.planned_loader import PlannedBooksLoader
from tables.reading_loader import ReadingStatusLoader
from tables.completed_loader import CompletedBooksLoader
from tables.message_loader import MessageLoader
from tables.bookgenre_loader import BookGenreLoader
from tables.booktag_loader import BookTagLoader
from tables.booktype_loader import BookTypeLoader

# Initialize logger for the DAG
log = logging.getLogger(__name__)


@dag(
    schedule_interval="* * * * *",  # Run every minute
    start_date=datetime(2025, 6, 11),  # Initial execution date
    catchup=False,  # Disable catchup to prevent backfilling
    tags=["stg", "postgres", "clickhouse"],  # Organizational tags
    is_paused_upon_creation=False,  # Start DAG immediately upon creation
)
def stg_dag():
    """
    Airflow DAG for loading data from PostgreSQL to ClickHouse (staging layer).

    This DAG orchestrates the parallel loading of multiple data entities:
    - Core entities: Users, Books, Types, Genres, Tags
    - User activity: Scores, Planned books, Reading status, Completed books
    - Relationships: Book-Genre, Book-Tag, Book-Type mappings
    - User messages

    The DAG uses separate tasks for each entity to enable parallel loading.
    """

    # Initialize database connections
    origin_pg_connect = PgConnectionBuilder.pg_conn(
        "POSTGRES_DEFAULT"
    )  # Source PostgreSQL connection
    dwh_clickhouse_connect = CHConnectionBuilder.ch_conn(
        "CLICKHOUSE_DEFAULT"
    )  # Target ClickHouse connection

    @task(task_id="users_load")
    def load_user():
        """Load user data from PostgreSQL to ClickHouse."""
        rest_loader = UserLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_users()

    @task(task_id="book_load")
    def load_book():
        """Load book data from PostgreSQL to ClickHouse."""
        rest_loader = BookLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book()

    @task(task_id="type_load")
    def load_type():
        """Load book type data from PostgreSQL to ClickHouse."""
        rest_loader = BookTypeLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_types()

    @task(task_id="genre_load")
    def load_genre():
        """Load genre data from PostgreSQL to ClickHouse."""
        rest_loader = GenreLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_genres()

    @task(task_id="tag_load")
    def load_tag():
        """Load tag data from PostgreSQL to ClickHouse."""
        rest_loader = BookTagLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_tags()

    @task(task_id="score_load")
    def load_score():
        """Load user score data from PostgreSQL to ClickHouse."""
        rest_loader = BookRatingLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_ratings()

    @task(task_id="planned_load")
    def load_planned():
        """Load planned book data from PostgreSQL to ClickHouse."""
        rest_loader = PlannedBooksLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_planned_books()

    @task(task_id="reading_load")
    def load_reading():
        """Load reading status data from PostgreSQL to ClickHouse."""
        rest_loader = ReadingStatusLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_reading_statuses()

    @task(task_id="completed_load")
    def load_completed():
        """Load completed book data from PostgreSQL to ClickHouse."""
        rest_loader = CompletedBooksLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_completed_books()

    @task(task_id="message_load")
    def load_message():
        """Load user message data from PostgreSQL to ClickHouse."""
        rest_loader = MessageLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_messages()

    @task(task_id="bookgenre_load")
    def load_bookgenre():
        """Load book-genre relationship data from PostgreSQL to ClickHouse."""
        rest_loader = BookGenreLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_genre()

    @task(task_id="booktag_load")
    def load_booktag():
        """Load book-tag relationship data from PostgreSQL to ClickHouse."""
        rest_loader = BookTagLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_tags()

    @task(task_id="booktype_load")
    def load_booktype():
        """Load book-type relationship data from PostgreSQL to ClickHouse."""
        rest_loader = BookTypeLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book_types()

    # Define task dependencies by creating references to task outputs
    user_dict = load_user()
    book_dict = load_book()
    type_dict = load_type()
    genre_dict = load_genre()
    tag_dict = load_tag()
    score_dict = load_score()
    planned_dict = load_planned()
    reading_dict = load_reading()
    completed_dict = load_completed()
    message_dict = load_message()
    bookgenre_dict = load_bookgenre()
    booktag_dict = load_booktag()
    booktype_dict = load_booktype()

    # Return all task references to establish implicit dependencies
    # (Airflow will run all tasks in parallel where possible)
    [
        user_dict,
        book_dict,
        type_dict,
        genre_dict,
        tag_dict,
        score_dict,
        planned_dict,
        reading_dict,
        completed_dict,
        message_dict,
        bookgenre_dict,
        booktag_dict,
        booktype_dict,
    ]


# Instantiate the DAG
stg_dag = stg_dag()
