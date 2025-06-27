import logging
from datetime import datetime

from airflow.decorators import dag, task

from lib.pg_connect import PgConnectionBuilder
from lib.ch_connect import CHConnectionBuilder
from tables.user_loader import UserLoader
from tables.book_loader import BookLoader
from tables.type_loader import TypeLoader
from tables.genre_loader import GenreLoader
from tables.tag_loader import TagLoader
from tables.score_loader import ScoreLoader
from tables.planned_loader import PlannedLoader
from tables.reading_loader import ReadingLoader
from tables.completed_loader import CompletedLoader
from tables.message_loader import MessageLoader
from tables.bookgenre_loader import BookgenreLoader
from tables.booktag_loader import BooktagLoader
from tables.booktype_loader import BooktypeLoader

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 * * * *',
    start_date=datetime(2025, 6, 11),
    catchup=False,
    tags=['stg', 'postgres', 'clickhouse'],
    is_paused_upon_creation=True
)
def stg_dag():
    origin_pg_connect = PgConnectionBuilder.pg_conn("POSTGRES_DEFAULT")

    dwh_clickhouse_connect = CHConnectionBuilder.ch_conn("CLICKHOUSE_DEFAULT")

    @task(task_id="users_load")
    def load_user():
        rest_loader = UserLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_user()

    @task(task_id="book_load")
    def load_book():
        rest_loader = BookLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_book()
        
    @task(task_id="type_load")
    def load_type():
        rest_loader = TypeLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_type()
        
    @task(task_id="genre_load")
    def load_genre():
        rest_loader = GenreLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_genre()
        
    @task(task_id="tag_load")
    def load_tag():
        rest_loader = TagLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_tag()
        
    @task(task_id="score_load")
    def load_score():
        rest_loader = ScoreLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_score()
        
    @task(task_id="planned_load")
    def load_planned():
        rest_loader = PlannedLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_planned()
        
    @task(task_id="reading_load")
    def load_reading():
        rest_loader = ReadingLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_reading()
        
    @task(task_id="completed_load")
    def load_completed():
        rest_loader = CompletedLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_completed()
    
    @task(task_id="message_load")
    def load_message():
        rest_loader = MessageLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_message()
        
    @task(task_id="bookgenre_load")
    def load_bookgenre():
        rest_loader = BookgenreLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_bookgenre()
        
    @task(task_id="booktag_load")
    def load_booktag():
        rest_loader = BooktagLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_booktag()
        
    @task(task_id="booktype_load")
    def load_booktype():
        rest_loader = BooktypeLoader(origin_pg_connect, dwh_clickhouse_connect, log)
        rest_loader.load_booktype()

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

    [user_dict, book_dict, type_dict, genre_dict, tag_dict, score_dict, planned_dict, reading_dict, completed_dict, message_dict, bookgenre_dict, booktag_dict, booktype_dict]

stg_dag = stg_dag()
