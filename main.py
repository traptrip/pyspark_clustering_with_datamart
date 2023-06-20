import logging
from pathlib import Path
from pyspark.sql import SparkSession, SQLContext, DataFrame

from src.utils import read_config
from src.clusterizer import Clusterizer

DEFAULT_CONFIG_PATH = Path(__file__).parent / "configs/default.yml"


def main(config):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("OpenFoodClusterizer")
        .config("spark.num.executors", "2")
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.jars", "datamart/out/artifacts/datamart_jar/datamart.jar")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sqlContext = SQLContext(spark)

    logging.info("Prepare dataset")
    jvm_df = sc._jvm.datamart.DataMart.getTrainData()

    data = DataFrame(jvm_df, sqlContext)

    model = Clusterizer(config, spark)
    metric, prediction = model.fit(data)
    print(f"Metric: {metric}")

    logging.info("Push prediction to DataMart")
    sc._jvm.datamart.DataMart.write(prediction._jdf)

    model.save(config.model.save_path)
    logging.info(f"Model saved to {config.model.save_path}")


if __name__ == "__main__":
    config = read_config(DEFAULT_CONFIG_PATH)
    main(config)
