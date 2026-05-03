import os
import yaml
from pyspark.sql import SparkSession


def get_spark_session(app_name="nedbank-de-pipeline", master="local[2]"):
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)

        .config("spark.driver.host", "localhost")

        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.pyspark.memory", "512m")
        .config("spark.memory.fraction", "0.5")
        .config("spark.memory.storageFraction", "0.3")

        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "2")

        .config("spark.shuffle.file.buffer", "64k")
        .config("spark.reducer.maxSizeInFlight", "96m")
        .config("spark.shuffle.sort.bypassMergeThreshold", "0")
        .config("spark.local.dir", "/data/output/spark-temp")

        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")

        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
        .config("spark.sql.adaptive.maxNumPostShufflePartitions", "4")
        .config("spark.sql.files.maxPartitionBytes", "128m")

        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
        .config("spark.sql.broadcastTimeout", "600")

        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        .config("spark.sql.parquet.compression.codec", "gzip")
        .config("spark.io.compression.codec", "lz4")

        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "ERROR")
    )

    return builder.getOrCreate()


def get_config():
    config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
    if not os.path.exists(config_path):
        config_path = "config/pipeline_config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
