from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# Metainformações dos arquivos
file_checks = {
    'ensino_de_graduacao.csv': {
        'path': '/path/to/ensino_de_graduacao.csv',
        'checks': {
            'id_curso': {'unique': True, 'nulls': False},
            'nome': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'componentes_por_curriculo.csv': {
        'path': '/path/to/componentes_por_curriculo.csv',
        'checks': {
            'id_componente': {'unique': True, 'nulls': False}
        }
    },
    # Adicione entradas semelhantes para outros arquivos conforme necessário
}

def calcular_precisao(file_config, ti):
    path = file_config['path']
    checks = file_config['checks']
    df = pd.read_csv(path, delimiter=';')
    
    resultados = {}
    total_records = len(df)
    
    for coluna, crits in checks.items():
        if 'unique' in crits:
            unique_pass_rate = (total_records - df[coluna].duplicated().sum()) / total_records
            resultados[f'{coluna}_unique'] = f'{unique_pass_rate:.2%} precisão de unicidade'
        
        if 'nulls' in crits:
            non_nulls_rate = df[coluna].notnull().mean()
            resultados[f'{coluna}_nulls'] = f'{non_nulls_rate:.2%} completude (sem nulos)'
        
        if 'regex' in crits:
            regex_pass_rate = df[coluna].str.match(crits['regex']).mean()
            resultados[f'{coluna}_regex'] = f'{regex_pass_rate:.2%} precisão de formato'

    ti.xcom_push(key='resultados_precisao', value=resultados)

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
    description='DAG genérico para verificar a precisão em múltiplos arquivos CSV',
    schedule_interval=timedelta(days=1),
    catchup=False
)

for file_name, config in file_checks.items():
    task = PythonOperator(
        task_id=f'verificar_{file_name}',
        python_callable=calcular_precisao,
        op_kwargs={'file_config': config},
        provide_context=True,
        dag=dag
    )
