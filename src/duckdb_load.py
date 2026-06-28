import duckdb
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os

class DuckDBLoad:

    def __init__(self, csv_base_path: str, db_path: str, max_rows: int = 20000):
        self.csv_base_path = csv_base_path
        self.db_path = db_path
        self.max_rows = max_rows

        # ThreadPool para execução async
        self.executor = ThreadPoolExecutor(max_workers=4)

    # Substitui datatranXXXX por read_csv_auto('caminho')
    def _inject_csv_reads(self, sql: str) -> str:
        arquivos = os.listdir(self.csv_base_path)

        for nome in arquivos:
            if nome.endswith(".csv") and nome.startswith("datatran"):
                ano = nome.replace(".csv", "")
                caminho = os.path.join(self.csv_base_path, nome).replace("\\", "/")
                sql = sql.replace(ano, f"read_csv_auto('{caminho}')")

        return sql

    # Execução síncrona com conexão isolada por consulta
    def execute_raw(self, sql: str, limit=None, offset=None, max_rows=None):
        sql_final = self._inject_csv_reads(sql)

        if limit is not None:
            sql_final += f" LIMIT {limit}"

        if offset is not None:
            sql_final += f" OFFSET {offset}"

        con = duckdb.connect(database=self.db_path, read_only=True)
        try:
            df = con.execute(sql_final).fetch_df()

            if max_rows and len(df) > max_rows:
                df = df.iloc[:max_rows]

            return df
        finally:
            con.close()

    # Execução assíncrona
    async def execute_raw_async(self, sql: str, limit=None, offset=None, max_rows=None):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self.executor,
            self.execute_raw,
            sql,
            limit,
            offset,
            max_rows or self.max_rows
        )