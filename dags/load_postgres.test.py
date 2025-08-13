from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from psycopg2.extras import execute_batch
import pandas as pd
import time

with DAG(
    dag_id='csv_to_postgres_dag',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    @task
    def remove_table():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn')
        remove_table_sql = """
            DROP TABLE IF EXISTS passengers;
        """
        pg_hook.run(remove_table_sql)

    @task
    def read_csv_and_insert():
        start = time.perf_counter()
        csv_file_path = '/opt/airflow/data/Titanic-Dataset.csv'
        df = pd.read_csv(csv_file_path)

        pg_hook = PostgresHook(postgres_conn_id='pg_conn')

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS passengers (
            PassengerId INTEGER PRIMARY KEY,
            Survived INTEGER,
            Pclass INTEGER,
            Name TEXT,
            Sex TEXT,
            Age NUMERIC,
            SibSp INTEGER,
            Parch INTEGER,
            Ticket TEXT,
            Fare NUMERIC,
            Cabin TEXT,
            Embarked TEXT );
        """
        pg_hook.run(create_table_sql)

        insert_sql = """
        INSERT INTO passengers (PassengerId, Survived, Pclass, Name, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (PassengerId) DO NOTHING;
        """

        records = df.values.tolist()

        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        execute_batch(cursor, insert_sql, records)

        conn.commit()
        cursor.close()
        conn.close()

        end = time.perf_counter()
        print(f"총 실행 시간: {end - start}초")

    remove_table() >> read_csv_and_insert()