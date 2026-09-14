# Importa o módulo 'os' para acessar variáveis do ambiente e funções do sistema operacional.
# Isso é útil para ler credenciais e manipular arquivos locais.
import os

# Importa a biblioteca do projeto BasedosDados para consultar tabelas do BigQuery.
# Ela permite executar consultas SQL diretamente em datasets públicos.
import basedosdados as bd

# Importa o pandas, que é usado para trabalhar com tabelas (DataFrames).
# Aqui ele é usado principalmente para receber o resultado da consulta.
import pandas as pd

# Importa a função 'load_dotenv' para carregar variáveis armazenadas em um arquivo .env.
# Isso evita expor segredos diretamente no código.
from dotenv import load_dotenv

# Importa a classe que autentica no Azure usando client ID, tenant ID e secret.
from azure.identity import ClientSecretCredential

# Importa a classe para conectar ao Azure Data Lake Storage Gen2.
from azure.storage.filedatalake import DataLakeServiceClient

# Carrega as variáveis do arquivo .env para o ambiente atual do Python.
# Se o arquivo .env não existir, a função simplesmente ignora.
load_dotenv()

# Lê a variável de ambiente AZURE_TENANT_ID e remove espaços extras.
# Caso não exista, retorna uma string vazia.
tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()

# Lê o ID do cliente do Azure (Application ID) para autenticação.
client_id = os.getenv("AZURE_CLIENT_ID", "").strip()

# Lê o segredo do cliente do Azure, que é usado para autenticação segura.
client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()

# Lê o nome da conta de armazenamento do Azure Data Lake.
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()

# Lê o ID do projeto no BasedosDados, necessário para executar consultas no billing correto.
bd_project_id = os.getenv("BASEDOSDADOS_PROJECT_ID", "").strip()

# Define uma função que cria e retorna o cliente do Azure Data Lake.
def get_datalake_client():
    # Cria o objeto de autenticação com as credenciais do Azure.
    # Esse objeto será usado para validar a conexão com os serviços Azure.
    credential = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )

    # Monta a URL do endpoint do Data Lake usando o nome da conta.
    # Exemplo: https://meuaccount.dfs.core.windows.net
    account_url = f"https://dltech2tutorsdev.dfs.core.windows.net"

    # Cria e retorna um cliente do Azure Data Lake, conectado com a autenticação do Azure.
    return DataLakeServiceClient(account_url=account_url, credential=credential)

# Define a função principal da ingestão da camada Bronze.
# Ela acessa diversas tabelas do BasedosDados e salva os dados em parquet no Azure.
def ingestao_multipla_bronze():
    # Cria um dicionário em que a chave é o nome da tabela e o valor é a SQL de consulta.
    # Cada consulta pega todos os registros de uma tabela pública do Base dos Dados.
    # O dicionário ajuda a automatizar a extração de várias tabelas em sequência.
    tabelas_extracao = {
        # Consulta da tabela de UFs do Brasil.
        "uf": "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.uf`",

        # Consulta da tabela de municípios do Brasil.
        "municipio": "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`",

        # Consulta da tabela de indicadores de alfabetização por UF.
        "meta_uf": "SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`",

        # Consulta de indicadores de alfabetização por município.
        "meta_municipio": "SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`"
    }

    # Chama a função que cria o cliente do Azure Data Lake.
    client = get_datalake_client()

    # Obtém o cliente do filesystem chamado 'bronze' dentro do Data Lake.
    # Esse filesystem é o container onde os arquivos brutos serão armazenados.
    file_system_client = client.get_file_system_client(file_system="bronze")

    # Percorre cada item do dicionário: nome da tabela e SQL da consulta.
    # Isso permite repetir o mesmo processo para todas as tabelas listadas.
    for nome_tabela, query in tabelas_extracao.items():
        # Imprime uma mensagem indicando que a extração dessa tabela começou.
        print(f"\n⏳ Extraindo: {nome_tabela}...")

        # Tenta executar o bloco de código e, se acontecer erro, captura e imprime.
        try:
            # 1. Extração: executa a consulta SQL no BasedosDados e salva o resultado em um DataFrame.
            # O DataFrame é uma tabela em memória, muito usada em Python para manipular dados.
            df = bd.read_sql(query=query, billing_project_id=bd_project_id)

            # Imprime a quantidade de linhas extraídas da tabela atual.
            print(f"✅ {len(df)} linhas extraídas para {nome_tabela}.")

            # 2. Conversão para Parquet: salva o DataFrame em um arquivo local no formato Parquet.
            # Parquet é eficiente para arquivos de dados porque comprime bem e mantém tipos.
            # O parâmetro index=False evita salvar a coluna de índice do DataFrame no arquivo.
            caminho_local = f"{nome_tabela}_bruto.parquet"
            df.to_parquet(caminho_local, index=False)

            # 3. Upload para o Azure: define o caminho remoto do arquivo dentro do container 'bronze'.
            # O padrão da camada Bronze é guardar os dados em formato bruto, sem transformação.
            file_path = f"{nome_tabela}/{nome_tabela}_bruto.parquet"

            # Cria um cliente para o arquivo específico dentro do filesystem Bronze.
            file_client = file_system_client.get_file_client(file_path)

            # Abre o arquivo parquet local em modo binário para leitura.
            # Em seguida, faz o upload para o Azure Data Lake.
            with open(caminho_local, "rb") as data:
                file_client.upload_data(data, overwrite=True)

            # Imprime confirmação de que o arquivo foi enviado com sucesso.
            print(f"🚀 SUCESSO! {nome_tabela} salvo no Azure: bronze/{file_path}")

            # Remove o arquivo local após o upload, para não deixar arquivos temporários no computador.
            os.remove(caminho_local)

        # Caso qualquer erro ocorra no processamento da tablea atual, entra no except.
        except Exception as e:
            # Exibe uma mensagem específica do erro para facilitar o debug.
            print(f"❌ Erro ao processar {nome_tabela}: {e}")

# Verifica se este script está sendo executado diretamente.
# Se for importado por outro arquivo, esse bloco não será executado.
if __name__ == "__main__":
    # Mensagem inicial para indicar que a ingestão começou.
    print("Iniciando Ingestão Batch - Camada Bronze")

    # Chama a função principal da ingestão.
    ingestao_multipla_bronze()