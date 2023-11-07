import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.pintu.OrderBookPipelines
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

class OrderBookPipelinesTest extends AnyFunSuite with BeforeAndAfter {

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("OrderBookPipelinesTest")
    .master("local[*]")
    .getOrCreate()

  after {
    if (spark != null) spark.stop()
  }

  test("transformData should calculate total correctly") {
    import spark.implicits._

    // Create a DataFrame from the provided sample data
    val sampleData = Seq(
      (0, "BTC-USDT", "SELL", 0.75, 34010.0, "OPEN", 1693973108L),
      (0, "BTC-USDT", "", 0.0, 0.0, "CLOSED", 1693979408L),
      (1, "BTC-USDT", "SELL", 0.96, 34050.0, "OPEN", 1693973208L),
      (2, "BTC-USDT", "SELL", 0.47, 34020.0, "OPEN", 1693973308L),
      (3, "BTC-USDT", "SELL", 0.66, 34010.0, "OPEN", 1693973408L)
    ).toDF("order_id", "symbol", "order_side", "size", "price", "status", "created_at")

    // Apply the transformData logic to calculate the "total" column
    val transformedDf = OrderBookPipelines.transformData(spark, sampleData)

    // Define the expected output data, including the "total" column
    // Here, we also need to cast the "created_at" column to timestamp
    val expectedData = Seq(
      (0, "BTC-USDT", "SELL", 0.75, 34010.0, "OPEN", new java.sql.Timestamp(1693973108L * 1000), 25507.5),
      (0, "BTC-USDT", "", 0.0, 0.0, "CLOSED", new java.sql.Timestamp(1693979408L * 1000), 0.0),
      (1, "BTC-USDT", "SELL", 0.96, 34050.0, "OPEN", new java.sql.Timestamp(1693973208L * 1000), 32688.0),
      (2, "BTC-USDT", "SELL", 0.47, 34020.0, "OPEN", new java.sql.Timestamp(1693973308L * 1000), 15989.4),
      (3, "BTC-USDT", "SELL", 0.66, 34010.0, "OPEN", new java.sql.Timestamp(1693973408L * 1000), 22446.600000000002)
    ).toDF("order_id", "symbol", "order_side", "size", "price", "status", "created_at", "total")

    // Compare the expected data with the actual transformed data
    assertColumnEquals(spark, expectedData, transformedDf, "total")
  }

  private def assertColumnEquals(spark: SparkSession, expected: DataFrame, actual: DataFrame, columnName: String): Unit = {
    import spark.implicits._
    val expectedColumn = expected.select(columnName).as[Double]
    val actualColumn = actual.select(columnName).as[Double]

    val expectedArray = expectedColumn.collect()
    val actualArray = actualColumn.collect()

    expectedArray.zip(actualArray).zipWithIndex.foreach { case ((exp, act), index) =>
      assert(exp === act, s"The total at index $index does not match (expected: $exp, actual: $act)")
    }
  }


}
