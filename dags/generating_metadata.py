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
    df = pd.read_csv(op_kwargs['input_path'], delimiter=',')
    # ! quando for para rodar os dados da ufrn o delimiter que ser modificado para ; e quando for ufrpe para ,

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
    'censo_dic_ufrpe': {
        'path': 'data/source/ufrpe/censo_cursos/censo_cursos.csv',
        'checks': {
            'NO_MUNICIPIO': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'ensino_de_graduacao_dic_ufrpe': {
        'path': 'data/source/ufrpe/ensino_de_graduacao/ensino_de_graduacao.csv',
        'checks': {
            'CODIGO_INEP': {'unique': True, 'nulls': False},
            'NOME_CURSO': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'componentes_por_curriculo_dic_ufrpe': {
        'path': 'data/source/ufrpe/componentes_por_curriculo/componentes_por_curriculo.csv',
        'checks': {
            'CODIGO_COMPONENTE': {'unique': True, 'nulls': False},
            'NOME_COMPONENTE': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'liquidacoes_dic_ufrpe': {
        'path': 'data/source/ufrpe/liquidacoes/liquidacoes.csv',
        'checks': {
            'Unidade Or\u00e7ament\u00e1ria': {'regex': r'^[a-zA-Z ]+$'}
        }
    },
    'matriculados_turma_graduacao_dic_ufrpe': {
        'path': 'data/source/ufrpe/matriculados_turma_graduacao/matriculados_turma_graduacao.csv',
        'checks': {
            'CODIGO_COMPONENTE': {'nulls': False}
        }
    },
    'qtd_alunos_graduacao_dic_ufrpe': {
        'path': 'data/source/ufrpe/qtd_alunos_graduacao/qtd_alunos_graduacao.csv',
        'checks': {
            'CURSO': {'regex': r'^[a-zA-Z ]+$', 'nulls': False}
        }
    }
}


def detectar_outliers(tipagem_real, coluna_nome, estats):
    # Se a coluna for numérica e o desvio padrão for muito alto, consideramos que há imprecisão
    if tipagem_real[coluna_nome] in ['int64', 'float64'] and estats['std'] > (estats['mean'] * 2):  # Exemplo de threshold arbitrário
        return 1
    return 0


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
        
        tipagem_real = json_metadado['tipagem_das_colunas']
        tipagem_esperada = json_dic['tipagem_das_colunas_disponibilizadas']
        n_colunas = json_metadado['n_colunas']
        n_linhas = json_metadado['n_linhas']
        valores_nulos = json_metadado['valores_nulos_por_coluna']
        valores_distintos = json_metadado['valores_distintos_por_coluna']

        # Calcular discrepância de tipos
        discrepancia_tipos = sum(1 for col in tipagem_real if tipagem_real[col] != tipagem_esperada[col])

        # _____________Cálculo da confiabilidade ________________
        confiabilidade = (n_colunas - discrepancia_tipos) / n_colunas

        # _____________ Cálculo da completude ________________ OK
        total_valores = n_linhas * n_colunas
        total_nulos = sum(valores_nulos.values())
        completude = (total_valores - total_nulos) / total_valores

        # ________________Cálculo da consistência _______________ OK
        inconsistencias = sum(1 for col, valores in valores_distintos.items() if tipagem_real[col] == 'int64' and valores > 100)
        consistencia = 1 - (inconsistencias / n_colunas)
        '''
            Considerações para Refinamento
                Granularidade: Como a função atual não considera quantas linhas em cada coluna têm tipos inconsistentes, essa métrica pode subestimar o impacto real das inconsistências isso poderia ser um ponto interessante de evolução
                Ponderação por Importância da Coluna: Se algumas colunas são mais críticas para a integridade dos  dados do que outras, isso pode ser feito dando mais peso para as colunas obrigatorias
                Extensão para Outras Inconsistências: Trabalhos futuros expandir a definição de consistência para incluir outros tipos de inconsistências (como inconsistências lógicas entre colunas, por exemplo), a fórmula pode ser adaptada para incorporar esses diferentes tipos com seus respectivos pesos.
        '''
        # Aplicar a função de detecção de outliers
        inconsistencias_precisao = sum(detectar_outliers(tipagem_real, col, json_metadado['estats_descritivas'][col]) for col in json_metadado['estats_descritivas'])

        # ________________Cálculo da precisão________________ OK
        precisao = 1 - (inconsistencias_precisao / n_colunas)

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
