import json
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from collections import Counter

def gerar_metadados(**op_kwargs):
    df = pd.read_csv(op_kwargs['input_path'])
    # Coletar metadados
    tipagem_das_colunas = df.dtypes
    # Converter a série de tipos de dados para um dicionário
    tipos_de_dados_dict = tipagem_das_colunas.astype(str).to_dict()
    # Extrair os tipos de dados
    print(list(tipos_de_dados_dict.values()))
    # Contar a quantidade de tipos de dados
    tipos_de_dados = list(tipos_de_dados_dict.values())
    contagem_tipos = Counter(tipos_de_dados)

    descricao = df.describe()
    colunas = df.columns.tolist()
    nulos = df.isnull().sum()
    contagem_valores_distintos = df.nunique()
    num_linhas, num_colunas = df.shape
    # Criar um dicionário para armazenar os metadados
    metadados = {
        "nome_da_tabela": op_kwargs['table_name'],
        "n_linhas": num_linhas,
        "n_colunas": num_colunas,
        "tipagem_das_colunas": tipos_de_dados_dict,
        "contador_tipagem": contagem_tipos,
        "estats_descritivas": descricao.to_dict(),  # Convertendo DataFrame para dicionário
        "nome_das_colunas": colunas,
        "valores_nulos_por_coluna": nulos.to_dict(),  # Convertendo Series para dicionário
        "valores_distintos_por_coluna": contagem_valores_distintos.to_dict()
    }

    # Salvar as informações em um arquivo JSON
    with open(op_kwargs['output_path'], 'w') as json_file:
        json.dump(metadados, json_file, indent=4)

def avaliacoes_criterios(metadados_files):
    """
    Credibilidade Total (considerando menos 'object'):
    Proposta: credibilidade_total = (Total Colunas - Colunas do Tipo 'object') * 100 / (Total Colunas)
    Quanto maior a porcentagem, mais colunas têm tipos de dados mais previsíveis, o que pode indicar maior confiabilidade.
    Essa métrica reflete a ideia de que quanto mais colunas têm tipos de dados previsíveis (por exemplo, inteiros), 
    mais confiável pode ser a tabela como um todo. As colunas do tipo 'object' são tratadas como menos confiáveis, 
    pois podem ter uma variedade maior de valores e podem necessitar de uma análise mais cuidadosa.
    """
    #TODO preciso gerar mais avaliações de criterios escolhendo logo pelo menos umas 5 dimensões para formular meus gráficos
    print('A Porcentagem de credibilidade na tabela de liquidações foi ')
    

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

parent_dir = 'data/ufrpe/'

# get folders
folders_names = [folder for folder in os.listdir(parent_dir) if os.path.isdir(os.path.join(parent_dir, folder))]

for folder_name in folders_names:
    input_path = f'data/ufrpe/{folder_name}/{folder_name}.csv'
    output_path = f'data/ufrpe/{folder_name}/{folder_name}_metadados.json' 
    table_name = folder_name

    gerar_metadados_task = PythonOperator(
        task_id=f'gerar_metadados_{folder_name}',
        python_callable=gerar_metadados,
        op_kwargs={'table_name': table_name, 'input_path': input_path, 'output_path': output_path},
        dag=dag,
    )

avaliacoes_criterios_task = PythonOperator(
    task_id='avaliacoes_criterios',
    python_callable=avaliacoes_criterios,
    op_kwargs={'metadados_files': output_path},
    dag=dag,
)
# Define a ordem de execução das tarefas
gerar_metadados_task >> avaliacoes_criterios_task

if __name__ == "__main__":
    dag.cli()

