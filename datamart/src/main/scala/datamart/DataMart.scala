package datamart

import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataMart {
  private val redisHost = "redis"
  private val redisPort = "6379"
  private val rawDataKey = "RawTable"
  private val predictsKey = "PredsTable"
  private val spark: SparkSession = SparkSession.builder()
    .appName("Redis DataFrame Example")
    .master("local[*]") // Set the master URL as per your environment
    .config("spark.redis.host", redisHost)
    .config("spark.redis.port", redisPort)
    .getOrCreate()

  def getTrainData(): DataFrame = {
    val redisDF = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", rawDataKey)
      .load()

    // Cast to float
    val castedDF1 = redisDF.select(Features.num_features.map(x => col(x).cast("float")): _*)
    val castedDF = castedDF1.na.fill(0.0).na.fill("unk")

    // Transform to vector features
    val vectorData = new VectorAssembler()
      .setInputCols(castedDF.columns)
      .setOutputCol("raw_features")
      .setHandleInvalid("error")
      .transform(castedDF)

    // Scale features
    val scaler = new MinMaxScaler().setInputCol("raw_features").setOutputCol("features")
    val scaledData = scaler.fit(vectorData).transform(vectorData)
    scaledData.show(4)
    scaledData
  }

  def write(data: DataFrame): Unit = {
    data.write
      .mode("overwrite")
      .format("org.apache.spark.sql.redis")
      .option("table", predictsKey)
      .save()
  }
}
