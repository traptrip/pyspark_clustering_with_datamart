import json
import os
import pickle

import uvicorn
import redis
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


from config import Config

RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", "data/openfood_sample.csv")
DB_HOST = os.environ.get("DB_HOST", "redis")
DB_PORT = os.environ.get("DB_PORT", "6379")
REDIS_CLIENT = redis.Redis(host=DB_HOST, port=DB_PORT)
API_HOST = os.environ.get("API_HOST", "data_mart")
API_PORT = os.environ.get("API_PORT", 8000)


class DataMartAPI:
    def __init__(self, cfg: Config):
        self.app = FastAPI()

        self.app.post("/write_prediction")(self.write_prediction)
        self.app.get("/get_data")(self.get_data)

        self.spark = (
            SparkSession.builder.master("local[*]")
            .appName("OpenFoodClusterizer")
            .config("spark.num.executors", "2")
            .config("spark.executor.cores", "4")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "4g")
            .getOrCreate()
        )
        self.cfg = cfg

    def _process(self, data):
        # keep only selected columns
        data = data.select([col(c).cast("float") for c in self.cfg.features])

        # process NaNs
        data = data.na.fill(0.0).na.fill("unk")

        # Scale data
        data = (
            VectorAssembler(inputCols=data.columns, outputCol="raw_features")
            .setHandleInvalid("error")
            .transform(data)
        )
        scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
        return scaler.fit(data).transform(data).select("features")

    def get_data(self):
        raw_data_bytes = REDIS_CLIENT.get("RawData")
        raw_data = pickle.loads(raw_data_bytes)
        with open("tmp.csv", "wb") as f:
            f.write(raw_data)

        data = self.spark.read.csv("tmp.csv", header=True, sep="\t")
        data = self._process(data)

        # TODO: replace pickle with json
        # data = data.toJSON().collect()

        bytes_data = pickle.dumps(data.collect())
        return StreamingResponse(
            iter([bytes_data]), media_type="application/octet-stream"
        )

    async def write_prediction(self, request: Request):
        data = json.dumps(await request.json()).encode("utf-8")
        REDIS_CLIENT.set("prediction", data)
        return {"message": "Data received"}

    def run(self):
        uvicorn.run(self.app, host=API_HOST, port=API_PORT)


if __name__ == "__main__":
    api = DataMartAPI(Config())
    api.run()


"""
import requests; import json; url = "http://data_mart:8000/get_data"; r = requests.get(url).content
from pyspark.sql import SparkSession, SQLContext, DataFrame
spark = (
    SparkSession.builder.master("local[*]")
    .appName("OpenFoodClusterizer")
    .config("spark.num.executors", "2")
    .config("spark.executor.cores", "4")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
sqlContext = SQLContext(spark)

"""
