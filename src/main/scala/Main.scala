
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("HW2 - MongoDB & Spark SQL")

    val spark = SparkSession.builder()
      .appName("HW2 Student Records")
      .master("local[*]")
      .getOrCreate()

    println(s"Spark Version: ${spark.version}")

    spark.stop()
  }
}
