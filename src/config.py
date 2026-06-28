import os
from dotenv import load_dotenv

# Garante que as variáveis do .env sejam carregadas
load_dotenv()

class Config:

    @property
    def DUCKDB_PATH(self) -> str:
        path = os.getenv("DUCKDB_PATH")
        if not path:
            raise ValueError("❌ DUCKDB_PATH não definido no .env")
        return path

    @property
    def DB_PATH(self) -> str:
        # Alias para DUCKDB_PATH — utilizado pelo api.py
        return self.DUCKDB_PATH

    @property
    def CSV_BASE_PATH(self) -> str:
        path = os.getenv("CSV_BASE_PATH")
        if not path:
            raise ValueError("❌ CSV_BASE_PATH não definido no .env")
        return path

    @property
    def MAX_ROWS(self) -> int:
        return int(os.getenv("MAX_ROWS", "20000"))

    @property
    def QUERY_TIMEOUT_MS(self) -> int:
        return int(os.getenv("QUERY_TIMEOUT_MS", "2000"))

    # Configurações JWT
    @property
    def JWT_SECRET_KEY(self) -> str:
        key = os.getenv("JWT_SECRET_KEY")
        if not key:
            raise ValueError("❌ JWT_SECRET_KEY não definido no .env")
        return key

    @property
    def JWT_ALGORITHM(self) -> str:
        return os.getenv("JWT_ALGORITHM", "HS256")

    @property
    def JWT_EXPIRE_MINUTES(self) -> int:
        return int(os.getenv("JWT_EXPIRE_MINUTES", "60"))

    # Credenciais da API
    @property
    def API_USERNAME(self) -> str:
        user = os.getenv("API_USERNAME")
        if not user:
            raise ValueError("❌ API_USERNAME não definido no .env")
        return user

    @property
    def API_PASSWORD(self) -> str:
        pwd = os.getenv("API_PASSWORD")
        if not pwd:
            raise ValueError("❌ API_PASSWORD não definido no .env")
        return pwd