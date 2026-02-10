import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    start_date=pendulum.now().subtract(days=1),
    schedule='*/5 * * * *',
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    description='DAG that sums the total of generated count values using PostgresHook.'
)
def sum_dag():  # ⚠️ Đổi tên tránh conflict với hàm built-in `sum`

    @task
    def create_sums_table():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run('''
            CREATE TABLE IF NOT EXISTS sums (
                value INTEGER
            );
        ''')

    @task
    def insert_sum_of_counts():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        hook.run('''
            INSERT INTO sums (value)
            SELECT SUM(value) FROM counts;
        ''')

    create_sums_table() >> insert_sum_of_counts()

sum_dag()
