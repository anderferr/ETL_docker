FROM python:3.11-slim

WORKDIR /app

# Instalar TexLive mínimo
RUN apt-get update && \
    apt-get install -y texlive-latex-base && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY app.py .
COPY src/ ./src/

# Expor porta Flask
EXPOSE 5000

# Comando padrão
CMD ["python", "app.py"]
