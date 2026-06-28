import asyncio
from fastapi import HTTPException
from typing import Optional
from src.duckdb_load import DuckDBLoad
import re

FALLBACK_LIMIT = 1000  # limite automático para SELECT sem LIMIT


class QueryService:

    def __init__(self, db_loader: DuckDBLoad):
        self.db_loader = db_loader

    # Validação segura sem bloquear consultas válidas
    def _validate_sql(self, sql: str):

        if not sql or not sql.strip():
            raise HTTPException(400, "Consulta SQL vazia.")

        sql_clean = sql.strip().lower()

        # Apenas SELECT e WITH
        primeira = sql_clean.split()[0]
        if primeira not in ("select", "with"):
            raise HTTPException(400, "❌ Apenas SELECT ou WITH são permitidos.")

        # Comandos proibidos
        proibidos = [
            "insert", "update", "delete", "drop",
            "create", "alter", "truncate", "attach",
            "detach", "grant", "revoke", "copy",
            "replace", "merge", "pragma", "set", "call"
        ]

        # Bloqueia comandos proibidos como palavras inteiras
        for cmd in proibidos:
            if re.search(rf"\b{cmd}\b", sql_clean):
                raise HTTPException(400, f"❌ Comando SQL proibido: {cmd.upper()}.")

        # Múltiplos statements
        if ";" in sql_clean[:-1]:
            raise HTTPException(400, "❌ Múltiplos comandos SQL não são permitidos.")

        # CROSS JOIN proibido
        if "cross join" in sql_clean:
            raise HTTPException(400, "❌ CROSS JOIN proibido por performance.")

        # JOIN sem ON
        if " join " in sql_clean and " on " not in sql_clean:
            raise HTTPException(400, "❌ JOIN sem ON é proibido.")

        # Detecção de JOIN implícito com vírgula
        sql_main = sql_clean

        # Se começar com WITH, tenta localizar o SELECT principal
        if sql_main.startswith("with "):
            pos_select = sql_main.rfind("select")
            if pos_select != -1:
                sql_main = sql_main[pos_select:]

        regex = r"from\s+([^\(\)]*?)(where|group by|order by|limit|having|offset|$)"
        match = re.search(regex, sql_main, re.DOTALL)

        if match:
            trecho = match.group(1)

            # Remove subqueries entre parênteses
            trecho = re.sub(r"\([^\)]*\)", "", trecho)

            # Se após limpar ainda houver vírgula → join implícito real
            if "," in trecho:
                raise HTTPException(400, "❌ JOIN implícito com vírgula é proibido.")

    # Execução principal com LIMIT automático e controle de timeout
    async def execute(
        self,
        sql: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        max_rows: int = 50000,
        timeout: int = 10  # Timeout em segundos
    ):
        self._validate_sql(sql)

        try:
            sql_final = sql.strip()

            # Remove ; final
            if sql_final.endswith(";"):
                sql_final = sql_final[:-1].strip()

            # Remove vírgula perdida no final
            while sql_final.endswith(","):
                sql_final = sql_final[:-1].strip()

            sql_lower = sql_final.lower()

            has_limit = " limit " in sql_lower
            has_offset = " offset " in sql_lower

            # LIMIT automático
            if not has_limit and limit is None:
                sql_final += f" LIMIT {FALLBACK_LIMIT}"

            # LIMIT informado pelo usuário
            if limit is not None and not has_limit:
                sql_final += f" LIMIT {int(limit)}"

            # OFFSET informado pelo usuário
            if offset is not None and not has_offset:
                sql_final += f" OFFSET {int(offset)}"

            # Executando a consulta com timeout
            df = await asyncio.wait_for(self.db_loader.execute_raw_async(
                sql=sql_final,
                max_rows=max_rows
            ), timeout=timeout)

            if df is None or df.empty:
                return []

            return df.to_dict(orient="records")

        except asyncio.TimeoutError:
            raise HTTPException(
                408,  # Status de Timeout
                detail="❌ Tempo limite de execução excedido."
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                500,
                f"❌ Erro interno ao executar consulta: {str(e)}"
            )