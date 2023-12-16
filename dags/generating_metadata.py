from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

def analisar_metadados(csv_path, output_path):
    df = pd.read_csv(csv_path)
    info = df.info()
    descricao = df.describe()
    colunas = df.columns.tolist()
    nulos = df.isnull().sum()

    # Salvar as informações em um arquivo de metadados
    with open(output_path, 'w') as f:
        f.write("Informações básicas:\n")
        f.write(str(info) + '\n\n')
        f.write("Estatísticas descritivas:\n")
        f.write(str(descricao) + '\n\n')
        f.write("Nome das colunas:\n")
        f.write(str(colunas) + '\n\n')
        f.write("Valores nulos por coluna:\n")
        f.write(str(nulos) + '\n')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'metadados_dag',
    default_args=default_args,
    description='Uma DAG para analisar metadados de um arquivo CSV',
    schedule_interval=None,  # Define a frequência de execução da DAG (None para executar manualmente)
    tags=["metaqd", "tcc-ufrpe"]
)

csv_path = 'data/ufrpe/Ensino de Graduacao/cursos.csv'  # Substitua pelo caminho do seu arquivo CSV
output_path = 'data/ufrpe/Ensino de Graduacao/cursos_metadados.txt'  # Substitua pelo caminho desejado para o arquivo de metadados

analisar_metadados_task = PythonOperator(
    task_id='analisar_metadados',
    python_callable=analisar_metadados,
    op_kwargs={'csv_path': csv_path, 'output_path': output_path},
    dag=dag,
)

# Define a ordem de execução das tarefas
analisar_metadados_task

if __name__ == "__main__":
    dag.cli()

