FROM apache/spark:3.4.2

USER root     # нужно, чтобы pip смог установить Python-библиотеки при желании

# Если нужны дополнительные python-пакеты — ставьте здесь
# RUN pip install --no-cache-dir pandas==2.2.1

WORKDIR /opt/spark/jobs

# Копируем сами джобы в образ
COPY jobs/ /opt/spark/jobs/
