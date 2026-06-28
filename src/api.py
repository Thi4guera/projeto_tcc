from fastapi import FastAPI, HTTPException, Depends, status, Request, Response
from pydantic import BaseModel
from typing import Optional
import time
import uuid
import json
import logging
from datetime import datetime, timezone

from src.config import Config
from src.duckdb_load import DuckDBLoad
from src.service.query_service import QueryService
from src.utils.auth import authenticate_user, create_access_token, get_current_user

# LOGGING ESTRUTURADO
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)
logger = logging.getLogger("duckdb_microservice")

# APP
app = FastAPI(
    title="Microsserviço DuckDB Analítico",
    version="1.0.0",
    description=(
        "Serviço analítico para execução de consultas SQL avançadas "
        "sobre dados da PRF utilizando DuckDB em modo somente leitura."
    )
)

config = Config()

# INFRA
db_loader = DuckDBLoad(
    csv_base_path=config.CSV_BASE_PATH,
    db_path=config.DUCKDB_PATH,
    max_rows=config.MAX_ROWS,
)

service = QueryService(db_loader)

# MÉTRICAS SIMPLES EM MEMÓRIA
metrics_data = {
    "http_requests_total": 0,
    "http_query_requests_total": 0,
    "http_errors_total": 0,
    "query_time_ms_total": 0.0,
}

# MODELOS
class SQLRequest(BaseModel):
    sql: str
    limit: Optional[int] = None
    offset: Optional[int] = None


class LoginRequest(BaseModel):
    username: str
    password: str

# MIDDLEWARE DE OBSERVABILIDADE
@app.middleware("http")
async def add_request_id_log_and_metrics(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    start = time.time()

    response = None
    status_code = 500

    try:
        response = await call_next(request)
        status_code = response.status_code
        return response

    except Exception as e:
        status_code = 500

        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "ERROR",
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "error": str(e),
        }
        logger.error(json.dumps(log_data, ensure_ascii=False))

        error_response = {
            "detail": "Erro interno no processamento da requisição.",
            "request_id": request_id
        }
        return Response(
            content=json.dumps(error_response, ensure_ascii=False),
            media_type="application/json",
            status_code=500
        )

    finally:
        elapsed_ms = round((time.time() - start) * 1000, 2)

        metrics_data["http_requests_total"] += 1
        if status_code >= 400:
            metrics_data["http_errors_total"] += 1

        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": "INFO" if status_code < 400 else "ERROR",
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "tempo_ms": elapsed_ms,
        }
        logger.info(json.dumps(log_data, ensure_ascii=False))

        if response is not None:
            response.headers["X-Request-ID"] = request_id

# AUTENTICAÇÃO
@app.post("/token", tags=["Autenticação"])
async def token(data: LoginRequest):
    if not authenticate_user(data.username, data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário ou senha inválidos"
        )

    access_token = create_access_token({"sub": data.username})

    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

# ENDPOINT PRINCIPAL
@app.post("/query", tags=["Consultas"])
async def query(
    req: SQLRequest,
    request: Request,
    user: dict = Depends(get_current_user)
):
    try:
        start = time.time()

        result = await service.execute(
            sql=req.sql,
            limit=req.limit,
            offset=req.offset,
            max_rows=config.MAX_ROWS,
            timeout=max(1, int(config.QUERY_TIMEOUT_MS / 1000)),
        )

        elapsed = round((time.time() - start) * 1000, 2)

        metrics_data["http_query_requests_total"] += 1
        metrics_data["query_time_ms_total"] += elapsed

        return {
            "request_id": request.state.request_id,
            "tempo_ms": elapsed,
            "resultado": result
        }

    except HTTPException as e:
        raise HTTPException(
            status_code=e.status_code,
            detail={
                "mensagem": e.detail,
                "request_id": request.state.request_id
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "mensagem": f"Erro interno ao executar consulta: {str(e)}",
                "request_id": request.state.request_id
            }
        )

# HEALTH CHECK
@app.get("/health", tags=["Sistema"])
async def health():
    return {
        "status": "ok",
        "service": "duckdb-microservice"
    }

# MÉTRICAS PROMETHEUS
@app.get("/metrics", tags=["Observabilidade"])
async def metrics():
    total_queries = metrics_data["http_query_requests_total"]

    avg_query_time = (
        metrics_data["query_time_ms_total"] / total_queries
        if total_queries > 0 else 0.0
    )

    content = "\n".join([
        "# HELP http_requests_total Total de requisições HTTP recebidas pela API",
        "# TYPE http_requests_total counter",
        f"http_requests_total {metrics_data['http_requests_total']}",

        "# HELP http_query_requests_total Total de requisições realizadas ao endpoint /query",
        "# TYPE http_query_requests_total counter",
        f"http_query_requests_total {metrics_data['http_query_requests_total']}",

        "# HELP http_errors_total Total de respostas HTTP com status de erro",
        "# TYPE http_errors_total counter",
        f"http_errors_total {metrics_data['http_errors_total']}",

        "# HELP query_time_ms_total Tempo total acumulado de execução das consultas SQL em milissegundos",
        "# TYPE query_time_ms_total counter",
        f"query_time_ms_total {metrics_data['query_time_ms_total']:.2f}",

        "# HELP query_time_ms_avg Tempo médio de execução das consultas SQL em milissegundos",
        "# TYPE query_time_ms_avg gauge",
        f"query_time_ms_avg {avg_query_time:.2f}",
    ]) + "\n"

    return Response(
        content=content,
        media_type="text/plain; version=0.0.4; charset=utf-8"
    )