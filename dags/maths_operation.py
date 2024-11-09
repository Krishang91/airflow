from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def start_number(**context):
    context['ti'].xcom_push(key='current_value', value=10)
    print("starting number 10")

def add_five_func(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='start_number')
    new_value = current_value + 5
    context['ti'].xcom_push(key='current_value', value=new_value)
    print("add 5: {current_value}".format(current_value=new_value))

def multiply_by_two_func(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='add_five')
    new_value = current_value * 2
    context['ti'].xcom_push(key='current_value', value=new_value)
    print("multiply by 2: {current_value}".format(current_value=new_value))

def subtract_three_func(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='multiply_by_two')
    new_value = current_value - 3
    context['ti'].xcom_push(key='current_value', value=new_value)
    print("subtract 3: {current_value}".format(current_value=new_value))

def square_number_func(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='subtract_three')
    new_value = current_value ** 2
    context['ti'].xcom_push(key='current_value', value=new_value)
    print("square: {current_value}".format(current_value=new_value))

with DAG(
    dag_id='maths_sequence_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',
    catchup=False   
) as dag:
    start_task = PythonOperator(
        task_id='start_number',
        python_callable=start_number,
        provide_context=True
    )

    add_five_task = PythonOperator(
        task_id='add_five',
        python_callable=add_five_func,
        provide_context=True
    )

    multiply_by_two_task = PythonOperator(
        task_id='multiply_by_two',
        python_callable=multiply_by_two_func,
        provide_context=True
    )

    subtract_three_task = PythonOperator(
        task_id='subtract_three',
        python_callable=subtract_three_func,
        provide_context=True
    )

    square_number_task = PythonOperator(
        task_id='square_number',
        python_callable=square_number_func,
        provide_context=True
    )

    start_task >> add_five_task >> multiply_by_two_task >> subtract_three_task >> square_number_task
