from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkByExamples").getOrCreate()
data = [
    "Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s",
]
rdd = spark.sparkContext.parallelize(data)

print("\nAll elements:")
for element in rdd.collect():
    print(element)

# Flatmap
print("\nFlatmap:")
rdd2 = rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

# map
print("\nMap:")
rdd3 = rdd2.map(lambda x: (x, 1))
for element in rdd3.collect():
    print(element)

print("\nreduceByKey:")
# reduceByKey
rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
for element in rdd4.collect():
    print(element)

print("\nMap:")
# map
rdd5 = rdd4.map(lambda x: (x[1], x[0])).sortByKey()
for element in rdd5.collect():
    print(element)

# filter
print("\nFilter:")
rdd6 = rdd5.filter(lambda x: "a" in x[1])
for element in rdd6.collect():
    print(element)
