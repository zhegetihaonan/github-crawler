FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Keep dependency installation in its own layer so source-only changes rebuild quickly.
COPY ./app/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY ./app/ /app/
COPY ./common/ /app/common/
RUN mkdir -p /app/logs

EXPOSE 8008
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8008"]
