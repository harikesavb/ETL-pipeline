FROM kestra/kestra:v1.1.12

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

USER root

WORKDIR /opt/project

COPY requirements.txt ./
RUN python -m pip install --upgrade pip && python -m pip install -r requirements.txt

COPY . .
RUN mkdir -p /opt/project/data/warehouse

CMD ["server", "local", "--flow-path=/opt/project/kestra"]
