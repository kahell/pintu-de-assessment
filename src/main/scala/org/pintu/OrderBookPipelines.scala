package org.pintu

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

import java.sql.{Timestamp, Date}

object OrderBookPipelines {

  case class InputRow(order_id: Long,
                      symbol: Option[String],
                      size: Option[Double],
                      price: Option[Double],
                      order_side: Option[String],
                      status: String,
                      created_at: Timestamp,
                      total: Option[Double],
                      date_partition: Date
                     )

  case class LastStatusState(order_id: Long, lastStatus: String)

  case class OrderUpdate(
                          order_id: Long,
                          symbol: String,
                          size: Double,
                          price: Double,
                          order_side: String,
                          status: String,
                          created_at: Timestamp,
                          total: Double,
                          date_partition: Date,
                          cumulative_sum: Double)
  case class OrderState(order_side: String, var cumulative_sum: Double)

  def main(args: Array[String]): Unit = {
    // Schema Registry configuration
    val schemaRegistryUrl = "http://localhost:8085"
    val topic = "technical_assessment"
    val subject = s"${topic}-schema"

    // Initialize Spark Session
    val spark = SparkSession.builder
      .appName("OrderBookPipelines")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Fetch the schema string from Schema Registry
    val schema = fetchSchemaFromRegistry(subject, schemaRegistryUrl)

    // Read from Kafka topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    // Assuming 'df' is your DataFrame read from Kafka and 'jsonSchema' is already defined
    val transformedDf = df
      .withColumn("data", from_json(col("value").cast("string"), schema))
      .withColumn("date_partition", to_date(col("timestamp")))
      .select("data.*", "date_partition")

    // ETL
    // Cast Column
    val etlDf = transformedDf
      .withColumn("order_id", col("order_id").cast("integer"))
      .withColumn("size", col("size").cast("double"))
      .withColumn("price", col("price").cast("double"))
      .withColumn("created_at", to_timestamp(col("created_at")))
      .withColumn("total", col("size") * col("price"))

    // Dedup
    val dedupDF = etlDf
      .withWatermark("created_at", "10 minutes")
      .dropDuplicates("order_id", "created_at")

    def updateAcrossEvents(
                            order_id: Long,
                            inputs: Iterator[InputRow],
                            oldState: GroupState[LastStatusState]
                          ): Iterator[(Long, String, Double, Double, String, String, Timestamp, Double, Date)] = {
      // Retrieve the last state or initialize if there's none
      var state: LastStatusState = if (oldState.exists) oldState.get else LastStatusState(order_id, "OPEN")

      // Update the state with the latest status
      val events = inputs.toSeq.sortBy(_.created_at.getTime) // Sort the events by timestamp
      val latestInput = events.last
      state = state.copy(lastStatus = latestInput.status)

      // Update the state
      oldState.update(state)

      // If you want to set a timeout after which the state value is removed, set it here
      // oldState.setTimeoutDuration("30 minutes")

      // Return the updated results with the new_status
      events.map(input => (
        input.order_id,
        input.symbol.getOrElse(""),
        input.size.getOrElse(0.0),
        input.price.getOrElse(0.0),
        input.order_side.getOrElse(""),
        state.lastStatus,
        input.created_at,
        input.total.getOrElse(0.0),
        input.date_partition
        )
      ).iterator
    }

    val updateStatus = dedupDF
      .as[InputRow]
      .groupByKey(_.order_id)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout)(updateAcrossEvents)

    val updatedStatusDF = updateStatus.toDF(
      "order_id",
      "symbol",
      "size",
      "price",
      "order_side",
      "status",
      "created_at",
      "total",
      "date_partition"
    )

    // Only Get Open & order_side not empty
    val openOrders = updatedStatusDF.filter(col("status") === "OPEN" && col("order_side") =!= "")

    // Cumulative SUM
    def updateCumulativeSum(
                             order_side: Option[String], // The key is now a tuple of (order_id, order_side)
                             inputs: Iterator[InputRow],
                             oldState: GroupState[OrderState]
                           ): Iterator[OrderUpdate] = {
      // Retrieve the old state if it exists, or initialize a new one
      val state = if (oldState.exists) oldState.get else OrderState(order_side = order_side.getOrElse(""), cumulative_sum = 0.0)

      // Sort the events by created_at timestamp
      val sortedInputs = inputs.toList.sortBy(_.created_at.getTime)

      // Map over the sorted inputs to create the OrderUpdate instances
      val updates = sortedInputs.map { order =>
        // Update the cumulative sum with the current order's total, only if the order_side matches
        state.cumulative_sum += order.total.getOrElse(0.0)
        OrderUpdate(
          order.order_id,
          order.symbol.getOrElse(""),
          order.size.getOrElse(0.0),
          order.price.getOrElse(0.0),
          order.order_side.getOrElse(""),
          order.status,
          order.created_at,
          order.total.getOrElse(0.0),
          order.date_partition,
          state.cumulative_sum
        )
      }

      // Update the state to the latest cumulative sum
      oldState.update(state)

      // Return the iterator of OrderUpdate instances
      updates.iterator
    }

    /// Group the data by the combination of Id and order_state
    val cumulativeSum = openOrders
      .as[InputRow]
      .groupByKey(_.order_side)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout)(updateCumulativeSum)

    val updatedCumulativeSumStreamDF = cumulativeSum.toDF(
      "order_id",
      "symbol",
      "size",
      "price",
      "order_side",
      "status",
      "created_at",
      "total",
      "date_partition",
      "cum_sum"
    )

    // Assuming df is your DataFrame after all transformations
    val outputPath = "/Applications/Works/pintu-de-assesment/src/main/scala/org/pintu/output/" // Replace with your desired output path
    val checkpointsPath = "/Applications/Works/pintu-de-assesment/src/main/scala/org/pintu/checkpoints/" // Replace with your desired output path

    // Selected
    val selectedDf = updatedCumulativeSumStreamDF
      .select(col("order_id"),
        col("symbol"),
        col("order_side"),
        col("size"),
        col("price"),
        col("status"),
        col("created_at"),
        col("total"),
        col("date_partition"),
        col("cum_sum"),
      )

    // Write the results in "update" mode and partition by 'date_partition'
    val query = selectedDf
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointsPath)
      .partitionBy("date_partition")
      .option("compression", "snappy")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()

    query.awaitTermination()
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
