import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_start_date = pendulum.now().subtract(days=1)

@dag(
    schedule='*/1 * * * *',
    start_date=default_start_date,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    description='DAG that creates table and inserts count using PostgresHook.'
)
def counter():

    @task
    def create_table_if_not_exists():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = '''
        CREATE TABLE IF NOT EXISTS counts (
            value INTEGER
        );
        '''
        hook.run(sql)

    @task
    def insert_count():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = '''
        INSERT INTO counts (value) VALUES (1);
        '''
        hook.run(sql)

    create_table_if_not_exists() >> insert_count()

counter()
