# Imagem base mínima
FROM python:3.9-slim

# Variáveis de ambiente (12-Factor)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Diretório principal da aplicação
WORKDIR /app

# Instalar dependências com cache otimizado
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copiar apenas o código necessário
COPY api.py /app/
COPY config.py /app/
COPY transfer /app/transfer
COPY service /app/service
COPY scripts /app/scripts

# Expor porta da API
EXPOSE 8000

# Comando de produção (sem reload)
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
