import os
import basedosdados as bd
import pandas as pd
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()
client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "").strip()
bd_project_id = os.getenv("BASEDOSDADOS_PROJECT_ID", "").strip()

def get_datalake_client():
    credential = ClientSecretCredential(
        tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
    )
    account_url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=credential)

def ingestao_multipla_bronze():
    # Dicionário corrigido com os nomes exatos do dataset no BigQuery
    tabelas_extracao = {
        "uf": "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        "municipio": "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`",
        "meta_uf": "SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf`",
        "meta_municipio": "SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio`"
    }

    client = get_datalake_client()
    file_system_client = client.get_file_system_client(file_system="bronze")

    for nome_tabela, query in tabelas_extracao.items():
        print(f"\n⏳ Extraindo: {nome_tabela}...")
        try:
            # 1. Extração
            df = bd.read_sql(query=query, billing_project_id=bd_project_id)
            print(f"✅ {len(df)} linhas extraídas para {nome_tabela}.")

            # 2. Conversão para Parquet (FinOps e otimização exigida)
            caminho_local = f"{nome_tabela}_bruto.parquet"
            df.to_parquet(caminho_local, index=False)

            # 3. Upload preservando formato bruto (regra da Camada Bronze)
            file_path = f"{nome_tabela}/{nome_tabela}_bruto.parquet"
            file_client = file_system_client.get_file_client(file_path)
            
            with open(caminho_local, "rb") as data:
                file_client.upload_data(data, overwrite=True)
            
            print(f"🚀 SUCESSO! {nome_tabela} salvo no Azure: bronze/{file_path}")
            
            # Limpeza local
            os.remove(caminho_local)

        except Exception as e:
            print(f"❌ Erro ao processar {nome_tabela}: {e}")

if __name__ == "__main__":
    print("Iniciando Ingestão Batch - Camada Bronze")
    ingestao_multipla_bronze()