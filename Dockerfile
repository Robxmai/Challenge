FROM nedbank-de-challenge/base:1.0

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark

ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

COPY pipeline/ /app/pipeline/
COPY config/ /app/config/

COPY delta-spark_2.12-3.1.0.jar /usr/local/lib/python3.11/site-packages/pyspark/jars/
COPY delta-storage-3.1.0.jar /usr/local/lib/python3.11/site-packages/pyspark/jars/

COPY log4j2.properties /app/
ENV SPARK_SUBMIT_OPTS="-Dlog4j2.configurationFile=file:///app/log4j2.properties"

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt 2>/dev/null || echo "No extra deps — offline build safe"

CMD ["python", "pipeline/run_all.py"]
