from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import great_expectations as ge
from great_expectations.datasource import PandasDatasource

# Define o nome da DAG e a descrição
dag = DAG(
    'great_expectations_example',
    description='Exemplo de DAG com Great Expectations',
    schedule_interval=None,  # Define o agendamento da DAG (None para execução manual)
    start_date=datetime(2023, 10, 8),  # Define a data de início da DAG
    catchup=False,  # Evita a execução em lote de tarefas antigas
)

# Função que cria um DataFrame de exemplo
def create_dataframe():
    data = {'col1': [1, 2, 3, 4, 5], 'col2': [10, 20, 30, 40, 50]}
    df = pd.DataFrame(data)
    df.to_csv('/tmp/sample_data.csv', index=False)

# Função que define e executa as validações com Great Expectations
def run_great_expectations():
    context = ge.data_context.DataContext('/path/to/your/great_expectations_directory')  # Substitua pelo caminho do seu diretório Great Expectations

    # Cria um DataSource com o DataFrame
    datasource = PandasDatasource('/tmp/sample_data.csv')

    # Define uma suíte de expectativas
    suite = context.create_expectation_suite(suite_name='my_suite', overwrite=True)

    # Define expectativas
    suite.expect_column_to_exist(column='col1')
    suite.expect_column_to_exist(column='col2')
    suite.expect_column_mean_to_be_between(column='col1', min_value=1, max_value=5)

    # Valida o DataFrame com base na suíte de expectativas
    batch = context.get_batch(batch_kwargs={'data_asset_name': '/tmp/sample_data.csv'}, datasource=datasource)
    results = context.run_validation_operator(
        "action_list_operator", 
        assets_to_validate=[batch], 
        run_id='my_run_id'
    )
    for result in results:
        if result["success"]:
            print(f"Expectation {result['expectation_config']['expectation_type']} succeeded for {result['expectation_config']['kwargs']['column']} column.")
        else:
            print(f"Expectation {result['expectation_config']['expectation_type']} failed for {result['expectation_config']['kwargs']['column']} column.")

# Tarefa para criar o DataFrame de exemplo
create_dataframe_task = PythonOperator(
    task_id='create_dataframe',
    python_callable=create_dataframe,
    dag=dag,
)

# Tarefa para executar as validações com Great Expectations
run_great_expectations_task = PythonOperator(
    task_id='run_great_expectations',
    python_callable=run_great_expectations,
    dag=dag,
)

# Define a ordem das tarefas
create_dataframe_task >> run_great_expectations_task
