from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import os, tempfile, logging

from config import Config
from transfer.duckdb_load import DuckDB_load
from service.query_service import QueryService

from fastapi.responses import JSONResponse

# LOGS
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

setup_logging()


# Modelo Pydantic para requisições de consulta
class QueryRequest(BaseModel):
    sql: str
    params: List[Any] = []
    limit: Optional[int] = None
    offset: Optional[int] = None   # ajustado

    class Config:
        schema_extra = {
            "example": {
                "sql": "SELECT * FROM datatran2020 WHERE uf='PR'",
                "limit": 100,
                "offset": 0
            }
        }

# API
app = FastAPI(
    title="Microserviço Analítico PRF com DuckDB",
    description="API para consultas analíticas sobre dados da PRF.",
    version="1.0.0",
)


# Conexão global DuckDB
cfg = Config()
duck = DuckDB_load(db_path=cfg.DUCKDB_PATH, csv_base_path=cfg.PRF_CSV_DIR)
service = QueryService(duck)


# Startup Event
@app.on_event("startup")
def startup_event():
    try:
        df = duck.query("SELECT table_name FROM information_schema.tables;")
        logging.info(f"Tabelas carregadas: {df}")
    except Exception as e:
        logging.error(f"Erro na inicialização: {e}")


# ENDPOINTS
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/query", tags=["Consultas"])
async def execute_query(req: QueryRequest):
    try:
        df = service.run_query(
            sql=req.sql,
            params=req.params,
            limit=req.limit,
            offset=req.offset
        )
        return df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/schemas")
async def list_schemas():
    df = duck.query("SELECT schema_name FROM information_schema.schemata;")
    return JSONResponse(content=df["schema_name"].tolist())


@app.get("/tables/{schema}/{table}/rows")
async def read_table(schema: str, table: str, limit: int = 10):
    try:
        ident = f"{schema}.{table}"
        df = duck.query(f"SELECT * FROM {ident} LIMIT {limit}")
        return df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/query/parquet/", summary="Executa SQL e retorna Parquet")
async def execute_query_parquet(req: QueryRequest):

    try:
        tmp_dir = tempfile.gettempdir()
        file_path = duck.query_to_temp_parquet(req.sql, dir_path=tmp_dir)

        return {
            "file_path": file_path,
            "file_name": os.path.basename(file_path),
            "file_size": os.path.getsize(file_path)
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
