import logging

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


class Clusterizer:
    def __init__(self, config, spark):
        self.spark = spark
        self.model = KMeans(k=config.model.k, seed=config.model.seed)

    def fit(self, data):
        logging.info("Training")
        metric = ClusteringEvaluator()
        self.model = self.model.fit(data)
        pred = self.model.transform(data)
        logging.info("Evaluating")
        m = metric.evaluate(pred)
        return m, pred.select("prediction")

    def save(self, path):
        self.model.save(path)
