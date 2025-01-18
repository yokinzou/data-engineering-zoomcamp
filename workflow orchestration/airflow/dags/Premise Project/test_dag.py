from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

test_dag = DAG(
    'simple_dag_2',
    description = 'A Simple DAG',
    schedule_interval = timedelta(days = 1),
    start_date = days_ago(1),
)

t1 = BashOperator(
    task_id = 'print_date',
    bash_command = 'date',
    dag = test_dag,
)

t2 = BashOperator(
    task_id = 'sleep',
    depends_on_past = False,
    bash_command = 'sleep 3',
    dag = test_dag,
)

t3 = BashOperator(task_id = 'print_end', depends_on_past = False, bash_command = 'echo \'end\'', dag = test_dag)

t1 >> t2
t2 >> t3