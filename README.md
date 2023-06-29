# Pyspark Clustering

Dataset: [openfoodfacts](https://world.openfoodfacts.org/data)

# Prepare dataset
## Download
```bash
sh data/download_data.sh
```

## Create a sample
```bash
python data_producer/prepare_dataset.py
```

# Run
```bash
docker compose up --build
```

# Run PySpark example 
```bash
python pyspark_clusterizer/src/word_count.py
```

# Project structure
```
├── data                              <- Dir where dataset will be placed
│   └── download_data.sh                <- Download raw data
│
├── data_producer/                    <- Data producer/preparer container
├── pyspark_clusterizer/              <- Clusterization model container
├── data_mart/                        <- DataMart container
└── README.md                         <- Project documentation
```
