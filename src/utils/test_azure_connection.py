import os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

tenant_id = os.getenv("AZURE_TENANT_ID")
client_id = os.getenv("AZURE_CLIENT_ID")
client_secret = os.getenv("AZURE_CLIENT_SECRET")
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

def get_datalake_client():
    try:
        #autentica usando o service principal
        credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
        )
        #Url do serviço de Data Lake
        datalake_url = f"https://dltech2tutorsdev.dfs.core.windows.net"

        # "Abre a conexão": cria o objeto que efetivamente envia comandos de leitura/escrita para a nuvem
        service_client = DataLakeServiceClient(account_url=datalake_url, credential=credential)
        # Retorna a conexão pronta para ser usada no restante do código
        return service_client
    
    except Exception as e:
        print(f"Erro na autenticação: {e}")
        return None

    # Verifica se o script está sendo executado diretamente no terminal (e não importado por outro arquivo)
if __name__ == "__main__":
    print("Conectando ao Azure Data Lake...")
    
    # Chama a função que criamos acima para tentar conectar
    client = get_datalake_client()
    
    # Se a conexão 'client' não for vazia (ou seja, conectou com sucesso)
    if client:
        print("\nSucesso! Estrutura de camadas encontrada:")
        try:
            # Pede ao Azure a lista de todos os contêineres/sistemas de arquivos (nossas camadas)
            file_systems = client.list_file_systems()
            
            # Faz um loop (repetição) passando por cada contêiner encontrado e imprime o nome dele
            for fs in file_systems:
                print(f" 🏅 {fs.name}")
        except Exception as e:
            # Se o app logou mas falhar ao listar, avisa que faltou dar a permissão IAM correta
            print(f"Erro de permissão: {e}")
