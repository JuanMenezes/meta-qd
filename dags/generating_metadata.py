import csv
import json
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from collections import Counter

# ? ____________ VARIAVEIS GLOBAIS ___________________

ies = 'ufrpe'


# ? ____________________ INICIO CRIAÇÃO DOS METADADOS ___________________ OK
def verificar_e_consertar_tipos(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':  # Verifica se o tipo da coluna é 'object'
            # Checa se todos os itens na coluna são do tipo string (str)
            if all(isinstance(x, str) or pd.isna(x) for x in df[coluna]):
                df[coluna] = df[coluna].astype('string')  # Converte para 'string' se todos forem strings
            else:
                # Tenta converter para numérico ou data, adiciona mais regras conforme necessário
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
                if df[coluna].isnull().any():  # Checa se a conversão para numérico falhou (produziu NaNs)
                    df[coluna] = pd.to_datetime(df[coluna], errors='coerce')
    return df


def gerar_metadados(**op_kwargs):
    df = pd.read_csv(op_kwargs['input_path'], delimiter=';')
    # ! quando for para rodar os dados da ufrn o delimiter que ser modificado para ; e quando for ufrn para ,

    df = verificar_e_consertar_tipos(df)  # Verifica e ajusta os tipos de dados
    # Coletar metadados após a verificação de tipo
    metadados = {
        "nome_da_tabela": op_kwargs['table_name'],
        "n_linhas": df.shape[0],
        "n_colunas": df.shape[1],
        "tipagem_das_colunas": df.dtypes.astype(str).to_dict(),
        "contador_tipagem": Counter(df.dtypes.astype(str).values),
        "estats_descritivas": df.describe(include='all').to_dict(),
        "nome_das_colunas": df.columns.tolist(),
        "valores_nulos_por_coluna": df.isnull().sum().to_dict(),
        "valores_distintos_por_coluna": df.nunique().to_dict()
    }
    # Salvar as informações em um arquivo JSON
    with open(op_kwargs['output_path'], 'w') as json_file:
        json.dump(metadados, json_file, indent=4)

# ? ____________________ FIM DE CRIAÇÃO DOS METADADOS _________________________
# !
# !
# ? ____________________ INICIO CRIAÇÃO DOS CRITERIOS ________________ ON GOING

file_checks_ufrn = {
    'ensino_de_graduacao_dic_ufrn': {
        'path': 'data/source/ufrn/ensino_de_graduacao/ensino_de_graduacao.csv',
        'checks': {
            'id_curso': {'unique': True, 'nulls': False},
            'nome': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'componentes_por_curriculo_dic_ufrn': {
        'path': 'data/source/ufrn/componentes_por_curriculo/componentes_por_curriculo.csv',
        'checks': {
            'id_curriculo': {'unique': True, 'nulls': False}
        }
    },
    'liquidacoes_dic_ufrn': {
        'path': 'data/source/ufrn/liquidacoes/liquidacoes.csv',
        'checks': {
            'cod_empenho': {'unique': True, 'nulls': False}
        }
    },
    'matriculados_turma_graduacao_dic_ufrn': {
        'path': 'data/source/ufrn/matriculados_turma_graduacao/matriculados_turma_graduacao.csv',
        'checks': {
            'matricula': {'unique': True, 'nulls': False}
        }
    },
}

file_checks_ufrpe = {
    'ensino_de_graduacao_dic_ufrn': {
        'path': 'data/source/ufrn/ensino_de_graduacao/ensino_de_graduacao.csv',
        'checks': {
            'id_curso': {'unique': True, 'nulls': False},
            'nome': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'componentes_por_curriculo_dic_ufrn': {
        'path': 'data/source/ufrn/componentes_por_curriculo/componentes_por_curriculo.csv',
        'checks': {
            'id_curriculo': {'unique': True, 'nulls': False}
        }
    },
    'liquidacoes_dic_ufrn': {
        'path': 'data/source/ufrn/liquidacoes/liquidacoes.csv',
        'checks': {
            'cod_empenho': {'unique': True, 'nulls': False}
        }
    },
    'matriculados_turma_graduacao_dic_ufrn': {
        'path': 'data/source/ufrn/matriculados_turma_graduacao/matriculados_turma_graduacao.csv',
        'checks': {
            'matricula': {'unique': True, 'nulls': False}
        }
    },
}


def calcular_precisao(file_checks):
    path = file_checks['path']
    checks = file_checks['checks']
    df = pd.read_csv(path, delimiter=';')

    resultados = {}
    total_records = len(df)

    for coluna, crits in checks.items():
        if 'unique' in crits:
            unique_pass_rate = (total_records - df[coluna].duplicated().sum()) / total_records
            print(f'unique_pass_rate {unique_pass_rate}')
            resultados[f'{coluna}_unique'] = unique_pass_rate

        if 'nulls' in crits:
            non_nulls_rate = df[coluna].notnull().mean()
            resultados[f'{coluna}_nulls'] = non_nulls_rate

        if 'regex' in crits:
            regex_pass_rate = df[coluna].str.match(crits['regex']).mean()
            resultados[f'{coluna}_regex'] = regex_pass_rate

    # Calcula a média de precisão para o arquivo
    average_precision = sum(resultados.values()) / len(resultados)
    return average_precision


def verificar_tipos(metadados, dic_dados):
    print(dic_dados)
    inconsistencias = 0
    for coluna, tipo in dic_dados['tipagem_das_colunas_disponibilizadas'].items():
        tipo_real = metadados['tipagem_das_colunas'].get(coluna)
        if tipo_real and tipo_real != tipo:
            inconsistencias += 1
    return inconsistencias


def avaliacoes_criterios(**op_kwargs):
    print(f'Arquivos de metadados atuais:{op_kwargs["metadados_files_path_list"]}')
    print(f'Arquivos de dicionarios atuais:{op_kwargs["dicionario_dados_path_list"]}')
    for file_path_metadados, file_path_dicionario in zip(op_kwargs["metadados_files_path_list"], op_kwargs["dicionario_dados_path_list"]):

        parte_especificada, file_name_sem_extensao = os.path.split(file_path_dicionario)
        table_name = str(file_name_sem_extensao.split(".")[0])
        
        # ? _________ Carga dos arquivos em json _________
        with open(file_path_metadados, 'r') as arquivo:
            json_metadado = json.load(arquivo)

        with open(file_path_dicionario, 'r') as arquivo_dic:
            json_dic = json.load(arquivo_dic)

        inconsistencias_retornadas = verificar_tipos(json_metadado, json_dic)

        # !_____________Cálculo da confiabilidade ________________
        n_colunas = json_metadado['n_colunas']
        quantidade_object = json_metadado['contador_tipagem'].get("object",0)
        confiabilidade = (n_colunas - quantidade_object) / n_colunas

        # _____________ Cálculo da completude ________________ OK
        n_colunas = len(json_metadado["valores_nulos_por_coluna"])
        n_valores_nulos_coluna = sum(value == 0 for value in json_metadado["valores_nulos_por_coluna"].values())
        completude = (1 - (n_valores_nulos_coluna / n_colunas))

        # ________________Cálculo da consistência _______________ OK
        consistencia = (1 - (inconsistencias_retornadas / n_colunas))
        '''
            Considerações para Refinamento
                Granularidade: Como a função atual não considera quantas linhas em cada coluna têm tipos inconsistentes, essa métrica pode subestimar o impacto real das inconsistências isso poderia ser um ponto interessante de evolução
                Ponderação por Importância da Coluna: Se algumas colunas são mais críticas para a integridade dos  dados do que outras, isso pode ser feito dando mais peso para as colunas obrigatorias
                Extensão para Outras Inconsistências: Trabalhos futuros expandir a definição de consistência para incluir outros tipos de inconsistências (como inconsistências lógicas entre colunas, por exemplo), a fórmula pode ser adaptada para incorporar esses diferentes tipos com seus respectivos pesos.
        '''

        # ________________Cálculo da precisão________________ OK
        precisao = calcular_precisao(f'{table_name}_{ies}')

        # _________INICIO_____ SAVING ON CSV ______________
        nome_arquivo_csv = f'data/analysis/metricas_{ies}.csv'

        # Verificar se o arquivo CSV já existe
        arquivo_existente = False
        try:
            with open(nome_arquivo_csv, 'r'):
                arquivo_existente = True
        except FileNotFoundError:
            pass

        header = ['nome_da_tabela','confiabilidade', 'completude', 'consistencia', 'precisao']
        # Abrir o arquivo CSV no modo de adição ('a' para append) ou escrita ('w' para escrever) se o arquivo não existir
        modo_abertura = 'a' if arquivo_existente else 'w'

        # Abrir o arquivo CSV
        with open(nome_arquivo_csv, mode=modo_abertura, newline='') as file:
            writer = csv.writer(file)

            # Se o arquivo não existir, escrever o cabeçalho
            if not arquivo_existente:
                writer.writerow(header)

            # Adicionar a linha ao arquivo CSV
            parte_especifica, nome_arquivo_sem_extensao = os.path.split(file_path_metadados)
            nome_da_tabela = str(nome_arquivo_sem_extensao.split(".")[0])

            writer.writerow([f"{ies}_{nome_da_tabela}", confiabilidade, completude, consistencia, precisao])
            print(f'Dados adicionados ao arquivo CSV "{nome_arquivo_csv}" com sucesso.')

            # lembrando que os dados gerados vou montar como json para plotar isso em outro código
            # _________FIM_____ SAVING ON CSV ______________
# ? ____________________ FIM DE CRIAÇÃO DOS CRITERIOS _________________________
# !
# !
# ? ____________________ INICIO DEFINICOES AIRFLOW ____________________________


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

parent_dir_source = f'data/source/{ies}/'

# Para cada diretorio com dados eu gero os seus respectivos metadados
folders_names = [folder for folder in os.listdir(parent_dir_source) if os.path.isdir(os.path.join(parent_dir_source, folder))]

for folder_name in folders_names:
    input_path = f'data/source/{ies}/{folder_name}/{folder_name}.csv'
    output_path = f'data/metadata/{ies}/{folder_name}_metadados.json'
    print()
    table_name = folder_name

    gerar_metadados_task = PythonOperator(
        task_id=f'gerar_metadados_{folder_name}',
        python_callable=gerar_metadados,
        op_kwargs={'table_name': table_name, 'input_path': input_path, 'output_path': output_path},
        dag=dag,
    )

parent_dir_metadata = f'data/metadata/{ies}/'
metadados_files_path = [os.path.join(parent_dir_metadata, file) for file in os.listdir(parent_dir_metadata) if os.path.isfile(os.path.join(parent_dir_metadata, file))]
metadados_files_path = sorted(metadados_files_path)


parent_dir_dicionario = f'data/dicionario_dados/{ies}/'
dicionario_dados_files_path = [os.path.join(parent_dir_dicionario, file) for file in os.listdir(parent_dir_dicionario) if os.path.isfile(os.path.join(parent_dir_dicionario, file))]
dicionario_dados_files_path = sorted(dicionario_dados_files_path)


avaliacoes_criterios_task = PythonOperator(
    task_id='avaliacoes_criterios',
    python_callable=avaliacoes_criterios,
    op_kwargs={
        'metadados_files_path_list': metadados_files_path,
        'dicionario_dados_path_list': dicionario_dados_files_path
        },
    dag=dag,
)
# ? ____________________ FIM DEFINICOES AIRFLOW ____________________________


gerar_metadados_task >> avaliacoes_criterios_task


if __name__ == "__main__":
    dag.cli()
