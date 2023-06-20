import org.apache.spark.sql.SparkSession

object StoreDataToRedis {
  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val redisHost = "localhost"
    val redisPort = "6379"
    val dataKey = "RawTable"
    val dataPath = "./data/openfood_sample.csv"

    val spark = SparkSession.builder()
      .appName("Redis DataFrame Example")
      .master("local[*]") // Set the master URL as per your environment
      .config("spark.redis.host", redisHost)
      .config("spark.redis.port", redisPort)
      .getOrCreate()

    // Read DataFrame from CSV
    val df = spark.read.option("header", "true").option("delimiter", "\t").csv(dataPath)

    // Put DataFrame in Redis
    df.write
      .mode("overwrite")
      .format("org.apache.spark.sql.redis")
      .option("table", dataKey)
      .save()

    spark.stop()
  }
}
