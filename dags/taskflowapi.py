from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id='maths_sequence_dag_with_taskflowapi',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',
    catchup=False
)as dag:
    @task
    def start_number() -> int:
        print('start_number: 10')
        return 10

    @task
    def add_five(current_value: int) -> int:
        print('add_five: {current_value}'.format(current_value=current_value + 5))
        return current_value + 5

    @task
    def multiply_by_two(current_value: int) -> int:
        print('multiply_by_two: {current_value}'.format(current_value=current_value * 2))
        return current_value * 2

    @task
    def subtract_three(current_value: int) -> int:
        print('subtract_three: {current_value}'.format(current_value=current_value - 3))
        return current_value - 3

    @task
    def square_number(current_value: int) -> int:
        print('square_number: {current_value}'.format(current_value=current_value ** 2))
        return current_value ** 2

    start_task = start_number()
    add_five_task = add_five(start_task)
    multiply_by_two_task = multiply_by_two(add_five_task)
    subtract_three_task = subtract_three(multiply_by_two_task)
    square_number_task = square_number(subtract_three_task)