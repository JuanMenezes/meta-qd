import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from collections import Counter

def gerar_metadados(table_name, csv_path, output_path):
    df = pd.read_csv(csv_path)
    # Coletar metadados
    tipagem_das_colunas = df.dtypes
    # Converter a série de tipos de dados para um dicionário
    tipos_de_dados_dict = tipagem_das_colunas.astype(str).to_dict()
    # Extrair os tipos de dados
    tipos_de_dados = list(tipagem_das_colunas.values())
    #TODO preciso pegar a quantidade dos tipos para poder jogar nessa metricas=
    # Contar a quantidade de cada tipo de dado
    contagem_tipos = Counter(tipos_de_dados)
    descricao = df.describe()
    colunas = df.columns.tolist()
    nulos = df.isnull().sum()
    contagem_valores_distintos = df.nunique()
    num_linhas, num_colunas = df.shape
    # Criar um dicionário para armazenar os metadados
    metadados = {
        "nome_da_tabela": table_name,
        "n_linhas": num_linhas,
        "n_colunas": num_colunas,
        "tipagem_das_colunas": tipos_de_dados_dict,
        "contador_tipagem": contagem_tipos,
        "estats_descritivas": descricao.to_dict(),  # Convertendo DataFrame para dicionário
        "nome_das_colunas": colunas,
        "valores_nulos_por_coluna": nulos.to_dict(),  # Convertendo Series para dicionário
        "valores_distintos_por_coluna": contagem_valores_distintos.to_dict()
        #"tipos_de_dados_contagem": tipos_de_dados_contagem.to_dict()
    }

    # Salvar as informações em um arquivo JSON
    with open(output_path, 'w') as json_file:
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

# TODO preciso deixar mais modular para que os parametros não sejam passados hardcoded, isso em outras partes do código também
csv_path = 'data/ufrpe/Liquidações/liq.csv'  # Substitua pelo caminho do seu arquivo CSV
output_path = 'data/ufrpe/Liquidações/liq_metadados.json'  # Substitua pelo caminho desejado para o arquivo de metadados
table_name = 'liquidacoes'


gerar_metadados_task = PythonOperator(
    task_id='gerar_metadados',
    python_callable=gerar_metadados,
    op_kwargs={'table_name': table_name, 'csv_path': csv_path, 'output_path': output_path},
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

