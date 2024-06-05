from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id='TargetDag',
    schedule='@once',
    start_date=datetime(2024, 6, 1),
)

task1 = BashOperator(
    task_id='task1',
    bash_command="""echo '{{ ds }}, {{ dag_run.conf.get("path", "none") }}' """,
    dag=dag
)
