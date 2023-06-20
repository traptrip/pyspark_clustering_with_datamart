import logging
from pathlib import Path

from src.utils import read_config
from src.clusterizer import Clusterizer
from src.redis import RedisBroker

DEFAULT_CONFIG_PATH = Path(__file__).parent / "configs/default.yml"


def main(config):
    redis_broker = RedisBroker(config.redis)

    logging.info("Reading raw dataset")
    while True:
        raw_data = redis_broker.get(config.data.raw_dataset_name)
        if raw_data is not None:
            break

    model = Clusterizer(config, raw_data)
    metric = model.fit()
    print(f"Metric: {metric}")

    redis_broker.set("metric", metric)
    logging.info("Metric value sended to redis")

    model.save(config.model.save_path)
    logging.info(f"Model saved to {config.model.save_path}")


if __name__ == "__main__":
    config = read_config(DEFAULT_CONFIG_PATH)
    main(config)
