package org.pintu

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

/**
 * OrderBookPipelines object contains all the necessary methods to
 * handle order book data pipeline processing using Spark structured streaming.
 */
object OrderBookPipelines {
  def main(args: Array[String]): Unit = {
    val spark = initializeSparkSession()
    import spark.implicits._

    // Configuration variables
    val schemaRegistryUrl = "http://localhost:8085"
    val topic = "technical_assessment"
    val subject = s"${topic}-schema"

    // Fetch the schema string from Schema Registry
    val schema = fetchSchemaFromRegistry(subject, schemaRegistryUrl)

    // Read from Kafka topic and transform the DataFrame
    val transformedDf = readFromKafka(spark, topic, schema)

    // Process the data through ETL, deduplication and stateful transformations
    val processedDf = transformData(spark, transformedDf)

    // Write the final result to the output sink
    writeToOutput(spark, processedDf)

    spark.streams.awaitAnyTermination()
  }

  /**
   * Initializes a SparkSession for the application.
   *
   * @return SparkSession instance
   */
  def initializeSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("OrderBookPipelines")
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * Reads data from a Kafka topic and creates a DataFrame with the desired schema.
   *
   * @param spark SparkSession instance
   * @param topic Kafka topic to read from
   * @param schema Schema to apply to the Kafka data
   * @return DataFrame with data from Kafka and applied schema
   */
  def readFromKafka(spark: SparkSession, topic: String, schema: StructType): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("string"), schema).alias("data"), to_date(col("timestamp")).alias("date_partition"))
      .select("data.*", "date_partition")
  }

  /**
   * Transforms the data by applying various ETL operations, deduplication, and stateful transformations.
   *
   * @param df DataFrame with the raw data
   * @return DataFrame with transformed data
   */
  def transformData(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    val etlDf = df
      .withColumn("order_id", col("order_id").cast("integer"))
      .withColumn("size", col("size").cast("double"))
      .withColumn("price", col("price").cast("double"))
      .withColumn("created_at", to_timestamp(col("created_at")))
      .withColumn("total", col("size") * col("price"))

    etlDf
  }

  /**
   * Writes the transformed DataFrame to the output in parquet format.
   *
   * @param df DataFrame to write
   */
  def writeToOutput(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._

    val outputPath = "/Applications/Works/pintu-de-assesment/src/main/scala/org/pintu/output/" // Replace with your desired output path
    val checkpointsPath = "/Applications/Works/pintu-de-assesment/src/main/scala/org/pintu/checkpoints/" // Replace with your desired output path

    df.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        // Define the window specification
        val windowSpecDedup = Window.partitionBy("order_id").orderBy("created_at")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        // Add a new column 'isClosed' which will be true if the last status in the window is 'CLOSED'
        val dedupDF = batchDF.withColumn("isClosed", when(last(col("status")).over(windowSpecDedup) === "CLOSED", true).otherwise(false)).dropDuplicates("order_id", "created_at")

        val openDedupDF = dedupDF.filter((col("status") === "OPEN") && (col("isClosed") === false))

        // Define the window specification
        val windowSpecCumSum = Window.partitionBy("order_side").orderBy("created_at")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)

        // Calculate the cumulative sum within each partition defined by 'order_side'
        val cumulativeSumDF = openDedupDF
          .withColumn("cum_sum", sum($"total").over(windowSpecCumSum))

        val selectedBatchDF = cumulativeSumDF
          .select(
            col("symbol"),
            col("order_side").alias("side"),
            col("price"),
            col("size").alias("amount"), // Assuming you meant "size" as the original field for "amount"
            col("total"),
            col("cum_sum"),
            col("date_partition")
          )

        // Then write the sorted batch to a sink, for example, to parquet files
        selectedBatchDF
          .write
          .mode("append")
          .format("parquet")
          .option("path", outputPath)
          .option("compression", "snappy")
          .partitionBy("date_partition")
          .save()
      }
      .option("checkpointLocation", checkpointsPath)
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()
  }

  // Function to fetch schema from Schema Registry
  def fetchSchemaFromRegistry(subject: String, schemaRegistryUrl: String): StructType = {
    val client = HttpClients.createDefault()
    val request = new HttpGet(s"$schemaRegistryUrl/subjects/$subject/versions/latest")
    val response = client.execute(request)

    try {
      val entity = response.getEntity
      val content = EntityUtils.toString(entity)

      // Create an ObjectMapper with Scala module
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      // Parse the JSON string to get the schema part
      val parsedJson = mapper.readValue[Map[String, Any]](content)
      val schemaString = parsedJson.getOrElse("schema", throw new Exception("Schema not found")).toString

      // Parse the schema string to get the properties
      val schemaJson = mapper.readValue[Map[String, Any]](schemaString)
      val properties = schemaJson("properties").asInstanceOf[Map[String, Map[String, Any]]]

      // Define a mapping from JSON schema types to Spark data types
      val typeMapping = Map(
        "integer" -> IntegerType,
        "string" -> StringType,
        "number" -> DoubleType // Assuming 'number' should be mapped to DoubleType
      )

      // Function to create a StructField for each property
      def createStructField(name: String, property: Map[String, Any]): StructField = {
        StructField(
          name,
          typeMapping(property("type").toString),
          property.get("nullable").map(!_.asInstanceOf[Boolean]).getOrElse(true)
        )
      }

      // Create a list of StructField objects for each property in the schema
      val fields = properties.map { case (name, prop) => createStructField(name, prop) }.toList

      // Create and return the StructType
      StructType(fields)
    } finally {
      response.close()
      client.close()
    }
  }
}
