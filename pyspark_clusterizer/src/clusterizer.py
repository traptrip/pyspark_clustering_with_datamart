import os
import shutil

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from src.logger import LOGGER


class Clusterizer:
    def __init__(self, config, spark):
        self.spark = spark
        self.model = KMeans(k=config.model.k, seed=config.model.seed)

    def fit(self, data):
        LOGGER.info("Training")
        metric = ClusteringEvaluator()
        self.model = self.model.fit(data)
        pred = self.model.transform(data)
        LOGGER.info("Evaluating")
        m = metric.evaluate(pred)
        return m, pred.select("prediction")

    def save(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)
        self.model.save(path)
