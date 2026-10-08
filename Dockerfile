FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip && pip install .
RUN useradd --create-home mdp && mkdir -p /data && chown mdp /data
USER mdp
VOLUME ["/data"]
ENV MDP_DATA_DIR=/data
EXPOSE 8000 9100
HEALTHCHECK --interval=30s --timeout=3s CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health').status==200 else 1)" || exit 1
CMD ["mdp", "serve", "--host", "0.0.0.0", "--port", "8000"]
