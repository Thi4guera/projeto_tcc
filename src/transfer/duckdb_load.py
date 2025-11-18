import duckdb
import pandas as pd
import os
import re
from pathlib import Path
from typing import Optional, List, Any


class DuckDB_load:
    """
    Classe utilitária para consultas analíticas no DuckDB.
    Mantém o modo SOMENTE LEITURA e permite SQL apenas SELECT/WITH.
    Faz substituição automática de datatranYYYY → read_csv_auto().
    """

    def __init__(self, db_path: str = None, csv_base_path: str = "/data/prf"):
        """
        Inicializa a conexão com o banco DuckDB (read-only).
        """

        self.db_path = db_path or os.getenv("DUCKDB_PATH")

        if not self.db_path:
            raise ValueError("❌ DUCKDB_PATH não foi definido.")

        self.db_path = Path(self.db_path)

        if not self.db_path.exists():
            raise FileNotFoundError(f"❌ Arquivo DuckDB não encontrado: {self.db_path}")

        print(f"[DuckDB_load] Banco carregado de: {self.db_path}")

        # SOMENTE LEITURA — conforme orientação do professor
        self._con = duckdb.connect(str(self.db_path), read_only=True)

        # Diretório dos CSVs
        self._csv_base_path = csv_base_path
        if not os.path.exists(self._csv_base_path):
            print(f"[DuckDB_load] ⚠ Diretório CSV não encontrado: {self._csv_base_path}")
        else:
            print(f"[DuckDB_load] CSV base path registrado: {self._csv_base_path}")


    # ============================================================================
    # VALIDAÇÃO SOMENTE LEITURA (leve, rápida e segura)
    # ============================================================================
    def validate_sql(self, sql: str):
        proibidos = ["insert", "update", "delete", "create", "alter", "drop", "truncate"]
        lower = sql.lower()

        for cmd in proibidos:
            if cmd in lower:
                raise ValueError(f"❌ Operação '{cmd.upper()}' não permitida.")

        if not lower.strip().startswith(("select", "with")):
            raise ValueError("❌ Somente SELECT e WITH são permitidos.")


    # ============================================================================
    # MÉTODO PRINCIPAL — FOCADO EM VELOCIDADE E SOMENTE LEITURA
    # ============================================================================
    def query(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> pd.DataFrame:

        # Validação mínima (leve e rápida)
        self.validate_sql(sql)

        # Limpa o SQL
        sql_clean = sql.strip().rstrip(";")

        # Detecta uso de datatranYYYY e substitui por CSV (rápido)
        if "datatran" in sql_clean.lower():
            anos = re.findall(r"datatran(\d{4})", sql_clean, flags=re.IGNORECASE)

            for ano in anos:
                csv_path = os.path.join(self._csv_base_path, f"datatran{ano}.csv")

                if not os.path.exists(csv_path):
                    raise FileNotFoundError(f"❌ CSV não encontrado: {csv_path}")

                # Substituição direta (rápida)
                sql_clean = sql_clean.replace(
                    f"datatran{ano}",
                    f"read_csv_auto('{csv_path}', sep=';', header=True, ignore_errors=True)"
                )

        # Adiciona LIMIT e OFFSET apenas se informado (rápido)
        if limit is not None:
            sql_clean += f" LIMIT {int(limit)}"

        if offset is not None:
            sql_clean += f" OFFSET {int(offset)}"

        try:
            # Execução direta (rápida)
            return self._con.execute(sql_clean, params or []).fetchdf()

        except duckdb.CatalogException as e:
            raise ValueError(f"❌ Erro de catálogo/arquivo: {str(e)}")

        except Exception as e:
            raise ValueError(f"❌ Erro ao executar consulta: {str(e)}")
