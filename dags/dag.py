import logging
from datetime import date

from airflow.decorators import dag, task
from airflow.models.variable import Variable

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 0 * * *',
    start_date=date(2025, 6, 11),
    catchup=False,
    tags=['stg', 'origin', 'my', 'postgres'],
    is_paused_upon_creation=True
)
def stg_dag():
    @task(task_id="users_load")
    def load_users():

    @task(task_id="books_load")
    def load_books():

    @task(task_id="scores_load")
    def load_scores():

    # Инициализируем объявленные таски.
    users_dict = load_users()
    books_dict = load_books()
    scores_dict = load_scores()

    # Далее задаем последовательность выполнения тасков.
    [users_dict, books_dict, scores_dict]

stg_dag = stg_dag()
