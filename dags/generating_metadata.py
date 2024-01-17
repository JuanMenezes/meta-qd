import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

def analisar_metadados(table_name, csv_path, output_path):
    df = pd.read_csv(csv_path)
    # Coletar metadados
    tipagem_das_colunas = df.dtypes
    # Converter a série de tipos de dados para um dicionário
    tipos_de_dados_dict = tipagem_das_colunas.astype(str).to_dict()
    descricao = df.describe()
    colunas = df.columns.tolist()
    nulos = df.isnull().sum()
    contagem_valores_distintos = df.nunique()
    num_linhas, num_colunas = df.shape
    """
    TODO
    Na verdade dyype é muito importante porque eu preciso saber se a tabela tem muitas colunas do tipo object porque isso pode fazer
    com que ela perca mais pontos na minha formula, ja que é um tipo que pode ser qualquer coisa ele também é mais fácil de não ser confiavel
    """
    # Criar um dicionário para armazenar os metadados
    metadados = {
        "nome_da_tabela": table_name,
        "n_linhas": num_linhas,
        "n_colunas": num_colunas,
        "tipagem_das_colunas": tipos_de_dados_dict,
        "estats_descritivas": descricao.to_dict(),  # Convertendo DataFrame para dicionário
        "nome_das_colunas": colunas,
        "valores_nulos_por_coluna": nulos.to_dict(),  # Convertendo Series para dicionário
        "valores_distintos_por_coluna": contagem_valores_distintos.to_dict()
        #"tipos_de_dados_contagem": tipos_de_dados_contagem.to_dict()
    }

    # Salvar as informações em um arquivo JSON
    with open(output_path, 'w') as json_file:
        json.dump(metadados, json_file, indent=4)

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

csv_path = 'data/ufrpe/Liquidações/liq.csv'  # Substitua pelo caminho do seu arquivo CSV
output_path = 'data/ufrpe/Liquidações/liq_metadados.json'  # Substitua pelo caminho desejado para o arquivo de metadados
table_name = 'liquidacoes'


analisar_metadados_task = PythonOperator(
    task_id='analisar_metadados',
    python_callable=analisar_metadados,
    op_kwargs={'table_name': table_name, 'csv_path': csv_path, 'output_path': output_path},
    dag=dag,
)

# Define a ordem de execução das tarefas
analisar_metadados_task

if __name__ == "__main__":
    dag.cli()

