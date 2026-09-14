# Importa o módulo 'os' para trabalhar com variáveis do sistema e do ambiente.
import os

# Importa o módulo 'io' para trabalhar com dados em memória.
# Isso será usado para ler e gravar arquivos Parquet sem salvar no disco local.
import io

# Importa o pandas, biblioteca usada para manipular tabelas de dados em memória.
import pandas as pd

# Importa a função que carrega as variáveis do arquivo .env para o ambiente.
from dotenv import load_dotenv

# Importa a classe para autenticar no Azure usando tenant, client id e secret.
from azure.identity import ClientSecretCredential

# Importa a classe para conectar ao Azure Data Lake Storage Gen2.
from azure.storage.filedatalake import DataLakeServiceClient

# Carrega as variáveis de ambiente que estiverem no arquivo .env.
load_dotenv()

# Lê as variáveis do Azure e remove espaços extras no começo e no fim da string.
tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()
client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()

# Define a função que cria e retorna um cliente autenticado do Azure Data Lake.
def get_datalake_client():
    # Cria o objeto que é usado para autenticar no Azure.
    credential = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )

    # Monta a URL do storage do Azure Data Lake.
    account_url = f"https://dltech2tutorsdev.dfs.core.windows.net"

    # Retorna o cliente do Data Lake, já conectado e autenticado.
    return DataLakeServiceClient(account_url=account_url, credential=credential)

# Define a função que lê um arquivo parquet da camada Bronze direto para memória.
def ler_parquet_bronze(file_system_client, caminho_arquivo):
    """Lê um arquivo Parquet do Azure direto para a memória (BytesIO)"""

    # Cria um cliente para o arquivo específico dentro do container Bronze.
    file_client = file_system_client.get_file_client(caminho_arquivo)

    # Faz o download do arquivo e lê todos os bytes em memória.
    downloaded_bytes = file_client.download_file().readall()

    # Converte os bytes em um DataFrame do pandas.
    # BytesIO faz com que o pandas trate esses bytes como se fossem um arquivo real.
    return pd.read_parquet(io.BytesIO(downloaded_bytes))

# Define a função que salva um DataFrame em parquet na camada Silver.
def salvar_parquet_silver(file_system_client, df, caminho_arquivo):
    """Salva um DataFrame como Parquet no Azure usando apenas a memória"""

    # Cria um "arquivo em memória" para receber o parquet.
    parquet_buffer = io.BytesIO()

    # Converte o DataFrame para parquet e guarda no buffer.
    # index=False significa: não salvar o índice do DataFrame como coluna.
    df.to_parquet(parquet_buffer, index=False)

    # Cria um cliente para o caminho de destino dentro do container Silver.
    file_client = file_system_client.get_file_client(caminho_arquivo)

    # Faz upload dos bytes para o Azure.
    # overwrite=True garante que o arquivo será substituído se já existia.
    file_client.upload_data(parquet_buffer.getvalue(), overwrite=True)

# Função principal: executa a transformação da camada Silver.
def processar_camada_silver():
    # Cria o cliente do Azure Data Lake.
    client = get_datalake_client()

    # Conecta ao container 'bronze', onde os arquivos brutos estão armazenados.
    bronze_client = client.get_file_system_client("bronze")

    # Conecta ao container 'silver', onde os dados tratados serão armazenados.
    silver_client = client.get_file_system_client("silver")

    # Mensagem para mostrar que a leitura da camada Bronze começou.
    print("📥 Lendo dados da Camada Bronze...")

    # Lê os arquivos Parquet da Bronze para DataFrames em memória.
    df_uf = ler_parquet_bronze(bronze_client, "uf/uf_bruto.parquet")
    df_mun = ler_parquet_bronze(bronze_client, "municipio/municipio_bruto.parquet")
    df_meta_uf = ler_parquet_bronze(bronze_client, "meta_uf/meta_uf_bruto.parquet")
    df_meta_mun = ler_parquet_bronze(bronze_client, "meta_municipio/meta_municipio_bruto.parquet")

    # Mensagem mostrando que a etapa de limpeza e transformação começou.
    print("⚙️ Iniciando transformações (Limpeza, Tratamento e Integração)...")

    # 1. TRATAMENTO DE VALORES AUSENTES E NORMALIZAÇÃO DE CHAVES
    # Remove linhas que têm o campo 'id_municipio' vazio.
    # Isso evita que junções e análises quebram por causa de chaves faltando.
    df_meta_mun = df_meta_mun.dropna(subset=['id_municipio'])

    # Remove linhas que têm a sigla do estado vazia.
    # Isso é importante porque a sigla será usada em joins e análises.
    df_meta_uf = df_meta_uf.dropna(subset=['sigla_uf'])

    # 2. INTEGRAÇÃO DAS BASES (Joins)
    # Junta a tabela de metas municipais com a tabela de municípios.
    # A ideia é trazer o nome do município e a sigla do estado para cada registro de meta.
    df_silver_mun = pd.merge(
        df_meta_mun,
        df_mun[['id_municipio', 'nome', 'sigla_uf']],
        on='id_municipio',
        how='left'
    )

    # Renomeia a coluna 'nome' para 'nome_municipio' para deixar o nome mais claro.
    # Isso ajuda a entender que a coluna representa o município e não outra coisa.
    df_silver_mun.rename(columns={'nome': 'nome_municipio'}, inplace=True)

    # 3. DERIVAÇÃO DE DADOS (Criação da Meta Brasil)
    # Agrupa os dados por ano e calcula a média da taxa de alfabetização.
    # Assim, cria-se uma visão agregada nacional para o Brasil.
    print("📈 Calculando indicador agregado nacional (Meta Brasil)...")

    # Agrupa por ano e calcula a média da coluna 'taxa_alfabetizacao'.
    # .mean() retorna a média para cada ano.
    # .reset_index() transforma o resultado em colunas normais do DataFrame.
    df_meta_brasil = df_meta_uf.groupby('ano')[['taxa_alfabetizacao']].mean().reset_index()

    # Cria a coluna 'escala' e preenche com 'Brasil'.
    # Isso indica que esse dado representa a agregação nacional.
    df_meta_brasil['escala'] = 'Brasil'

    # Mensagem informando que os dados estão prontos para serem salvos.
    print("📤 Salvando dados tratados na Camada Silver...")

    # 4. UPLOAD PARA A SILVER
    # Salva o DataFrame enriquecido com nome do município.
    salvar_parquet_silver(silver_client, df_silver_mun, "meta_municipio/meta_municipio_enriquecido.parquet")

    # Salva a tabela tratada por UF.
    salvar_parquet_silver(silver_client, df_meta_uf, "meta_uf/meta_uf_tratado.parquet")

    # Salva o indicador agregado do Brasil por ano.
    salvar_parquet_silver(silver_client, df_meta_brasil, "meta_brasil/meta_brasil_agregado.parquet")

    # Mensagem final confirmando sucesso.
    print("🚀 SUCESSO! Transformações da Camada Silver concluídas.")

# Se esse arquivo for executado diretamente, chama a função principal.
if __name__ == "__main__":
    processar_camada_silver()