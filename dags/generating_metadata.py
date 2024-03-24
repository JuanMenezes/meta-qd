import csv
import json
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from collections import Counter

def gerar_metadados(**op_kwargs):
    df = pd.read_csv(op_kwargs['input_path'], delimiter=';')
    # ! quando for para rodar os dados da ufrpe o delimiter que ser modificado para ; e quando for ufrpe para ,
    # Coleta metadados
    tipagem_das_colunas = df.dtypes
    # Converte a série de tipos de dados para um dicionário
    tipos_de_dados_dict = tipagem_das_colunas.astype(str).to_dict()
    # Extrai os tipos de dados
    print(list(tipos_de_dados_dict.values()))
    # Conta a quantidade de tipos de dados
    tipos_de_dados = list(tipos_de_dados_dict.values())
    contagem_tipos = Counter(tipos_de_dados)

    descricao = df.describe()
    colunas = df.columns.tolist()
    nulos = df.isnull().sum()
    cont_valores_dist = df.nunique()
    num_linhas, num_colunas = df.shape
    # Cria um dicionário para armazenar os metadados
    metadados = {
        "nome_da_tabela": op_kwargs['table_name'],
        "n_linhas": num_linhas,
        "n_colunas": num_colunas,
        "tipagem_das_colunas": tipos_de_dados_dict,
        "contador_tipagem": contagem_tipos,
        "estats_descritivas": descricao.to_dict(),  # Convertendo DataFrame para dicionário
        "nome_das_colunas": colunas,
        "valores_nulos_por_coluna": nulos.to_dict(),  # Convertendo Series para dicionário
        "valores_distintos_por_coluna": cont_valores_dist.to_dict()
    }

    # Salva as informações em um arquivo JSON
    with open(op_kwargs['output_path'], 'w') as json_file:
        json.dump(metadados, json_file, indent=4)

def avaliacoes_criterios(**op_kwargs):
    print(f'Arquivos de metadados atuais:{op_kwargs["metadados_files_path_list"]}')
    for file_path in op_kwargs["metadados_files_path_list"]:
        with open(file_path, 'r') as arquivo:
            json_metadado = json.load(arquivo)
        
        # Cálculo da credibilidade, precisao, completude, acessibilidade, consistencia
        n_colunas = json_metadado['n_colunas']
        quantidade_object = json_metadado['contador_tipagem'].get("object",0)
        credibilidade = (n_colunas - quantidade_object) / n_colunas
        
        # Cálculo da completude
        n_colunas = len(json_metadado["valores_nulos_por_coluna"])
        n_valores_nulos_coluna = sum(value != 0 for value in json_metadado["valores_nulos_por_coluna"].values())
        print(n_valores_nulos_coluna)
        print(n_colunas)
        completude = (1 - (n_valores_nulos_coluna / n_colunas))

        # Cálculo da consistência
        valores_distintos_por_coluna = json_metadado["valores_distintos_por_coluna"]
        inconsistencias = sum(value >= 1000 for value in valores_distintos_por_coluna.values())
        # Aqui, o 1 é subtraído para enfatizar a consistência. Se não houver inconsistências (inconsistencias=0inconsistencias=0), a fórmula retornará 100100, indicando consistência completa. Se todas as colunas forem inconsistentes, a fórmula retornará 00.
        consistencia = (1 - (inconsistencias / n_colunas))

        # Cálculo da precisão
        precisao = (inconsistencias / n_colunas)
        # Aqui, não há subtração de 11, pois a precisão é medida diretamente pela proporção de valores distintos em relação ao total de colunas. Essa métrica indica a "precisão" dos dados em termos de diferentes valores presentes.


        # ! Criando o processo de salvar essas métricas em um CSV para plot posterior
        # Definir o nome do arquivo CSV
        nome_arquivo_csv = 'data/analysis/metricas_ufrn.csv'

        # Verificar se o arquivo CSV já existe
        arquivo_existente = False
        try:
            with open(nome_arquivo_csv, 'r'):
                arquivo_existente = True
        except FileNotFoundError:
            pass
        
        header = ['nome_da_tabela','credibilidade', 'completude', 'consistencia', 'precisao']
        # Abrir o arquivo CSV no modo de adição ('a' para append) ou escrita ('w' para escrever) se o arquivo não existir
        modo_abertura = 'a' if arquivo_existente else 'w'

        # Abrir o arquivo CSV
        with open(nome_arquivo_csv, mode=modo_abertura, newline='') as file:
            writer = csv.writer(file)

            # Se o arquivo não existir, escrever o cabeçalho
            if not arquivo_existente:
                writer.writerow(header)
            
            # Adicionar a linha ao arquivo CSV
            parte_especifica, nome_arquivo_sem_extensao = os.path.split(file_path)
            nome_da_tabela = str(nome_arquivo_sem_extensao.split(".")[0])

            writer.writerow([f"ufrn_{nome_da_tabela}", credibilidade, completude, consistencia, precisao])
            print(f'Dados adicionados ao arquivo CSV "{nome_arquivo_csv}" com sucesso.')

            # lembrando que os dados gerados vou montar como json para plotar isso em outro código
            # Salvar as informações em um arquivo JSON

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
    tags=["metaqd", "tcc"]
)

parent_dir_source = 'data/source/ufrn/'

# Para cada diretorio com dados eu gero os seus respectivos metadados
folders_names = [folder for folder in os.listdir(parent_dir_source) if os.path.isdir(os.path.join(parent_dir_source, folder))]

for folder_name in folders_names:
    input_path = f'data/source/ufrn/{folder_name}/{folder_name}.csv'
    output_path = f'data/metadata/ufrn/{folder_name}_metadados.json' 
    table_name = folder_name

    gerar_metadados_task = PythonOperator(
        task_id=f'gerar_metadados_{folder_name}',
        python_callable=gerar_metadados,
        op_kwargs={'table_name': table_name, 'input_path': input_path, 'output_path': output_path},
        dag=dag,
    )

parent_dir_metadata = 'data/metadata/ufrn/'

metadados_files_path = [os.path.join(parent_dir_metadata, file) for file in os.listdir(parent_dir_metadata) if os.path.isfile(os.path.join(parent_dir_metadata, file))]

avaliacoes_criterios_task = PythonOperator(
    task_id=f'avaliacoes_criterios',
    python_callable=avaliacoes_criterios,
    op_kwargs={'metadados_files_path_list': metadados_files_path},
    dag=dag,
)
# Define a ordem de execução das tarefas
gerar_metadados_task >> avaliacoes_criterios_task

if __name__ == "__main__":
    dag.cli()