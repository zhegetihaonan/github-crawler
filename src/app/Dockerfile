FROM surface_web_base:v1.2

WORKDIR /app



COPY ./app /app
COPY ./common /app/common

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8008"]
#CMD ["supervisord", "-n", "-c", "/etc/supervisord.conf"]