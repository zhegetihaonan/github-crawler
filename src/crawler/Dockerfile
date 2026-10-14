FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /home/surface_crawler

RUN apt-get update \
    && apt-get install -y --no-install-recommends supervisor \
    && rm -rf /var/lib/apt/lists/*

# Copy dependencies first so crawler source edits can reuse this layer.
COPY ./crawler/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY ./crawler/ /home/surface_crawler/
COPY ./common/ /home/surface_crawler/common/
COPY ./crawler/supervisord.conf /etc/supervisord.conf
RUN mkdir -p /home/surface_crawler/logs

CMD ["supervisord", "-n", "-c", "/etc/supervisord.conf"]
