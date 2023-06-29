import pandas as pd

df = pd.read_csv("data/openfood.csv.gz", compression="gzip", sep="\t")
df.sample(100).to_csv("data/openfood_sample.csv", sep="\t", index=False)
