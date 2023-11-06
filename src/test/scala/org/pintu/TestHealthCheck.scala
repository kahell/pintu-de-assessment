package scala.org.pintu

import org.scalatest.funsuite.AnyFunSuite

class HealthCheckTest extends AnyFunSuite {

  test("HealthCheck.main should print the correct message") {
    // Capture the output of the println statement
    val stream = new java.io.ByteArrayOutputStream()
    Console.withOut(stream) {
      HealthCheck.main(Array.empty)
    }

    // Convert the output stream to a string and remove any trailing new line characters
    val output = stream.toString.trim

    // Assert that the output is what we expect
    assert(output == "Health Check is ok")
  }
}
