from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Caminho para o arquivo CSV onde os resultados serão salvos
csv_output_path = 'data/analysis/precision_results.csv'

# Metainformações dos arquivos
file_checks = {
    'ensino_de_graduacao_ufrn': {
        'path': 'data/source/ufrn/ensino_de_graduacao/ensino_de_graduacao.csv',
        'checks': {
            'id_curso': {'unique': True, 'nulls': False},
            'nome': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'componentes_por_curriculo_ufrn': {
        'path': 'data/source/ufrn/componentes_por_curriculo/componentes_por_curriculo.csv',
        'checks': {
            'id_curriculo': {'unique': True, 'nulls': False}
        }
    },
    'liquidacoes_ufrn': {
        'path': 'data/source/ufrn/liquidacoes/liquidacoes.csv',
        'checks': {
            'id_curriculo': {'unique': True, 'nulls': False}
        }
    },
    'matriculados_turma_graduacao_ufrn': {
        'path': 'data/source/ufrn/matriculados_turma_graduacao/matriculados_turma_graduacao.csv',
        'checks': {
            'id_curriculo': {'unique': True, 'nulls': False}
        }
    },
    # Adicione entradas semelhantes para outros arquivos conforme necessário
}


def calcular_precisao_e_salvar(file_config):
    path = file_config['path']
    checks = file_config['checks']
    df = pd.read_csv(path, delimiter=';')

    resultados = {}
    total_records = len(df)

    for coluna, crits in checks.items():
        if 'unique' in crits:
            unique_pass_rate = (total_records - df[coluna].duplicated().sum()) / total_records
            resultados[f'{coluna}_unique'] = unique_pass_rate

        if 'nulls' in crits:
            non_nulls_rate = df[coluna].notnull().mean()
            resultados[f'{coluna}_nulls'] = non_nulls_rate

        if 'regex' in crits:
            regex_pass_rate = df[coluna].str.match(crits['regex']).mean()
            resultados[f'{coluna}_regex'] = regex_pass_rate

    # Calcula a média de precisão para o arquivo
    average_precision = sum(resultados.values()) / len(resultados)

    # Salvar resultado no arquivo CSV
    with open(csv_output_path, 'a') as file:
        file.write(f"{os.path.basename(path).replace('.csv', '')},{average_precision:.4f}\n")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'verificacao_precisao_dados_generico',
    default_args=default_args,
    description='DAG genérico para verificar a precisão em múltiplos arquivos CSV e salvar em um arquivo CSV',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Assegurar que o arquivo CSV seja criado com o cabeçalho antes de executar as tarefas
if not os.path.exists(csv_output_path):
    with open(csv_output_path, 'w') as file:
        file.write('nome_da_tabela,precisao\n')

for file_name, config in file_checks.items():
    task = PythonOperator(
        task_id=f'verificar_{file_name}',
        python_callable=calcular_precisao_e_salvar,
        op_kwargs={'file_config': config},
        dag=dag
    )
