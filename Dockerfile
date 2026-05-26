FROM prefecthq/prefect:3.4.2-python3.13

WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir .
