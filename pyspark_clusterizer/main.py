import os
import json
import pickle
from pathlib import Path

import requests
from pyspark.sql import SparkSession, SQLContext, DataFrame

from src.utils import read_config
from src.clusterizer import Clusterizer
from src.logger import LOGGER

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config.yml"
API_HOST = os.environ.get("API_HOST", "data_mart")
API_PORT = os.environ.get("API_PORT", 8000)
API_URL = f"http://{API_HOST}:{API_PORT}"


def get_data(spark):
    bytes_data = requests.get(f"{API_URL}/get_data").content
    data_list = pickle.loads(bytes_data)
    df = spark.createDataFrame(data_list)
    return df


def write_prediction(data: DataFrame):
    data = data.toJSON().collect()
    data = [json.loads(d) for d in data]
    requests.post(f"{API_URL}/write_prediction", json=data)


def main(config):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("OpenFoodClusterizer")
        .config("spark.num.executors", "2")
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    LOGGER.info("Get dataset from DataMart")
    data = get_data(spark)

    model = Clusterizer(config, spark)
    metric, prediction = model.fit(data)
    LOGGER.info(f"Metric: {metric}")

    LOGGER.info("Push prediction to DataMart")
    write_prediction(prediction)

    model.save(config.model.save_path)
    LOGGER.info(f"Model saved to {config.model.save_path}")


if __name__ == "__main__":
    config = read_config(DEFAULT_CONFIG_PATH)
    main(config)
    # while True:
    #     ...
