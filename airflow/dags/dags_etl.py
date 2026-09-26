from airflow import DAG
from datetime import datetime, timedelta
import requests
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_and_cleaning_data(ti):
    query = "Data Engineering"
    url = f"https://openlibrary.org/search.json?q={query.replace(' ', '+')}"

    response = requests.get(url)
    data = response.json()

    books = []

    for book in data['docs'][:10]:
        books.append({
            'title':book.get('title'),
            'author_name':book.get('author_name')[0],
            'first_publish_year':book.get('first_publish_year')
        })
    
    ti.xcom_push(key='book_data', value=books)

def insert_data(ti):
    books = ti.xcom_pull(key='book_data', task_ids= 'extract_cleaning_data')

    postgres_hook = PostgresHook(postgres_conn_id = 'books_connection')

    # {'title': 'Fundamentals of Data Engineering', 'author_name': 'Joe Reis', 'first_publish_year': 2022}
    
    insert_query = """
        INSERT INTO books (title, author_name, first_publish_year)
        VALUES (%s, %s, %s)
    """

    for book in books:
        parameter_book = (book['title'], book['author_name'], book['first_publish_year'])
        postgres_hook.run(insert_query, parameters = parameter_book)

default_args = {
    'owner':'risdan',
    'start_date':datetime(2025,5,4),
    'retries':0
}

dag = DAG(
    'dag_project_de_etl_v04',
    default_args = default_args,
    schedule_interval = None
)

task_1 = PythonOperator(
    task_id = 'extract_cleaning_data',
    python_callable = extract_and_cleaning_data,
    dag=dag
)

task_2 = PostgresOperator(
    task_id ='create_table',
    postgres_conn_id = 'books_connection',
    sql = "./files/create_table.sql",
    dag = dag
)

task_3 = PythonOperator(
    task_id='insert_data',
    python_callable= insert_data,
    dag=dag
)

task_1 >> task_2 >> task_3