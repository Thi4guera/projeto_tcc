# Microsserviço DuckDB Analítico

Este repositório contém o código-fonte do projeto desenvolvido para o Trabalho de Conclusão de Curso, com foco na implementação de um microsserviço de consultas analíticas em modo somente leitura utilizando FastAPI e DuckDB.

## Descrição do projeto

O projeto implementa uma API para execução de consultas SQL analíticas sobre dados públicos da Polícia Rodoviária Federal.

A solução foi desenvolvida com foco em:

- consultas somente leitura;
- validação de comandos SQL;
- controle de acesso;
- limitação de volume de resposta;
- timeout de execução;
- exposição de métricas operacionais.

## Tecnologias utilizadas

- Python
- FastAPI
- DuckDB
- Docker
- Docker Compose
- JWT
- Swagger / OpenAPI

## Estrutura principal

```txt
Microservice-DuckD/
├── src/
│   ├── api.py
│   ├── config.py
│   ├── duckdb_load.py
│   ├── service/
│   └── utils/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Endpoints principais

- `POST /token` — geração de token de autenticação.
- `POST /query` — execução de consultas SQL analíticas em modo somente leitura.
- `GET /health` — verificação de funcionamento da aplicação.
- `GET /metrics` — exposição de métricas operacionais.

## Regras de segurança e controle

A API permite apenas consultas do tipo `SELECT` e `WITH`.

Comandos de alteração ou manipulação de dados são bloqueados, como:

- `INSERT`
- `UPDATE`
- `DELETE`
- `DROP`
- `CREATE`
- `ALTER`

Também foram implementados controles de limite máximo de linhas retornadas e timeout de execução, com o objetivo de evitar consultas excessivamente pesadas.

## Execução com Docker

Para executar o projeto, é necessário ter Docker e Docker Compose instalados.

```bash
docker compose up --build
```

Após a inicialização, a documentação interativa da API pode ser acessada em:

```txt
http://localhost:8000/docs
```

## Configuração do ambiente

O arquivo `.env` não é versionado no repositório por conter configurações locais.

Um modelo está disponível em:

```txt
.env.example
```

Antes da execução, crie um arquivo `.env` com base no `.env.example` e ajuste os caminhos conforme a máquina utilizada.

## Observação sobre os dados

Os arquivos CSV da PRF e o banco `database.duckdb` não foram incluídos no repositório devido ao tamanho dos arquivos e à separação entre código-fonte e dados locais.

Para execução completa, os dados devem ser mantidos localmente e montados no container conforme definido no `docker-compose.yml`.

## Finalidade acadêmica

Este projeto foi desenvolvido como parte de um Trabalho de Conclusão de Curso, com o objetivo de avaliar a aplicação do DuckDB em um microsserviço analítico read-only, comparando aspectos funcionais, operacionais e de desempenho em relação a uma arquitetura tradicional com PostgreSQL.