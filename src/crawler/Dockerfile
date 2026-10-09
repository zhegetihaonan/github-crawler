FROM surface_web_base:v1.2

WORKDIR /home/surface_crawler

#COPY requirements.txt .
#
#RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/
#
## 👉 安装 playwright 浏览器 + 依赖
#RUN playwright install --with-deps chromium

RUN mkdir -p /home/surface_crawler/common /home/surface_crawler/logs
COPY ./crawler /home/surface_crawler
COPY ./common /home/surface_crawler/common
COPY ./crawler/supervisord.conf /etc/supervisord.conf

CMD ["supervisord", "-n", "-c", "/etc/supervisord.conf"]
