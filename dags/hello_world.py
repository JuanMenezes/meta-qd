from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define o nome da DAG e a descrição
dag = DAG(
    'hello_world_example',
    description='Exemplo de DAG que imprime "Hello, World!"',
    schedule_interval=None,  # Define o agendamento da DAG (None para execução manual)
    start_date=datetime(2023, 10, 8),  # Define a data de início da DAG
    catchup=False,  # Evita a execução em lote de tarefas antigas
    tags=["metaqd", "tcc-ufrpe"]
)

# Função que imprime "Hello, World!"
def print_hello_world():
    print("Hello, World!")

# Tarefa para imprimir "Hello, World!"
hello_world_task = PythonOperator(
    task_id='hello_world_task',
    python_callable=print_hello_world,
    dag=dag,
)
