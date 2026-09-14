import os
import basedosdados as bd
from dotenv import load_dotenv

load_dotenv()
bd_project_id = os.getenv("BASEDOSDADOS_PROJECT_ID", "").strip()

print("🔐 Solicitando autorização do Google...")
# Fazemos uma consulta "falsa" e minúscula apenas para forçar a tela de login
bd.read_sql("SELECT 1", billing_project_id=bd_project_id, reauth=True)
print("✅ Autenticação concluída e salva no seu computador!")