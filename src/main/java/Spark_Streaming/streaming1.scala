package Spark_Streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object streaming1 {
  def main(args: Array[String]): Unit = {

    /* SparkSession*/
    val spark = SparkSession.builder()
      .appName("Stream1")
      .master("local")
      .getOrCreate()

    val df = spark.readStream
      .option("header", "true")
      .option("sep",",")
      .csv("C:\\Users\\msi\\Desktop\\LifeExpectancyData.csv")
    df.createOrReplaceTempView("df")

    df.printSchema()

    val dfDeveloping = spark.sql("SELECT * FROM df WHERE Status = 'Developing'")
    dfDeveloping.createOrReplaceTempView("dfDeveloping")
    dfDeveloping.show()

    val dfNotNull = spark.sql("SELECT * FROM dfDeveloping WHERE dfDeveloping IS NOT NULL")
    dfNotNull.createOrReplaceTempView("dfNotNull")
    dfNotNull.show()

    dfNotNull.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }
}
