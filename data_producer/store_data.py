import os
import pickle
import redis

RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", "data/openfood_sample.csv")
DB_HOST = os.environ.get("DB_HOST", "redis")
DB_PORT = os.environ.get("DB_PORT", "6379")
REDIS_CLIENT = redis.Redis(host=DB_HOST, port=DB_PORT)


def store_dataset():
    with open(RAW_DATA_PATH, "rb") as f:
        raw_dataset = f.read()
        raw_dataset = pickle.dumps(raw_dataset)
    REDIS_CLIENT.set("RawData", raw_dataset)


if __name__ == "__main__":
    store_dataset()
