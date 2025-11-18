import logging
import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from config import Config
from transfer.duckdb_load import DuckDB_load
import duckdb

# Configuração de logging (Registra data/hora, nível (INFO/ERRO) e mensagem.
# Usada para acompanhar a execução das consultas e detectar erros.)
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# Modelo Pydantic para receber o corpo JSON no formato {"sql": "..."}
class SQLRequest(BaseModel):
    sql: str

# Inicialização do Microserviço
# Ativa o sistema de log/ Lê o caminho do banco a partir do .env (via classe Config).
# Conecta ao banco DuckDB no modo somente leitura.
# Define limite de memória e ativa profiling (monitoramento de desempenho).    
def create_app() -> FastAPI:
    setup_logging()
    cfg = Config()

    # DuckDB_load também registra automaticamente os CSVs anuais
    csv_path = (
        "C:/Faculdade/2º Semestre - 2025/Trabalho de Conclusão de Curso I - Projeto e Pesquisa/Base de dados"
    )

    duck = DuckDB_load(
        db_path=cfg.duckdb_path,
        csv_base_path=csv_path  # <--- integração automática dos arquivos datatranYYYY.csv
    )

    # Aqui é criada a instância da API, com nome, descrição e versão que aparecem automaticamente na documentação Swagger (/docs).
    app = FastAPI(
        title="Consultas Analíticas com DuckDB",
        description="Microserviço para execução de consultas SQL read-only usando DuckDB",
        version="1.0.0"
    )

    # Endpoint Principal - aceita SQL em formato JSON {"sql": "..."}
    @app.post("/query", summary="Executa uma consulta SQL read-only")
    async def execute_query(body: SQLRequest):
        try:
            # Extrai o campo 'sql' do corpo JSON
            query = body.sql.strip()

            if not query:
                raise HTTPException(status_code=400, detail="Campo 'sql' vazio.")

            # Loga a consulta executada (útil para debug)
            logging.info(f"Executando consulta SQL:\n{query}")

            # Executa a query (consulta) no DuckDB
            result = duck.query(query)

            # Retorna o resultado formatado como lista JSON
            return JSONResponse(content=result.to_dict(orient="records"))

        except duckdb.Error as e:
            logging.error(f"Erro DuckDB: {e}")
            raise HTTPException(status_code=500, detail=f"Erro DuckDB: {str(e)}")
        except Exception as e:
            logging.exception("Erro inesperado ao executar query.")
            raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")

    @app.get("/", summary="Status do microserviço")
    async def root():
        return {"status": "ok", "message": "Microserviço DuckDB ativo e pronto para consultas."}

    logging.info("Microserviço iniciado. O sistema está pronto para executar consultas SQL.")
    return app

# Inicialização do Servidor

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)
