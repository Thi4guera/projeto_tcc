from transfer.duckdb_load import DuckDB_load
from typing import Any, List, Optional


class QueryService:
    
    # Camada de serviço responsável por abstrair o acesso ao DuckDB.
    # Apenas encaminha consultas para o adaptador DuckDB_load.

    def __init__(self, duck: DuckDB_load):
        self.duck = duck

    def execute(self, sql: str, params: Optional[List[Any]] = None):
        
        #Método compatível com versões antigas — executa SQL direto.
        # Ideal para uso básico.
        
        return self.duck.query(sql, params=params)

    def run_query(
        self,
        sql: str,
        params: Optional[List[Any]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ):
        # Método principal usado pela API /query.
        # Aceita paginação e parâmetros opcionais.
        
        return self.duck.query(
            sql=sql,
            params=params,
            limit=limit,
            offset=offset
        )
