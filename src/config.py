import os
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

class Config:
    
    # Centraliza todas as variáveis de ambiente usadas no projeto. 
    # Segue o modelo 12-Factor App.

    @property
    def DUCKDB_PATH(self):
        path = os.getenv("DUCKDB_PATH")
        if not path:
            raise ValueError("❌ Variável de ambiente DUCKDB_PATH não definida.")
        return path

    @property
    def PRF_CSV_DIR(self):
        path = os.getenv("PRF_CSV_DIR")
        if not path:
            raise ValueError("❌ Variável de ambiente PRF_CSV_DIR não definida.")
        return path

    @property
    def SCHEMAS(self):
        raw = os.getenv("SCHEMAS", "")
        return [s.strip() for s in raw.split(",") if s.strip()]

    @property
    def PRF_TARGET_TABLE(self):
        return os.getenv("PRF_TARGET_TABLE", "prf.ocorrencias")
