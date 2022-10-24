import org.apache.spark.sql.SparkSession

object Big_Data_P3 {
    def main(args:Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("Big_Data_P3")
            .master("local")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "52.91.98.39:9092")
            .option("subscribe", "prodTopic")
            .load()

        df.selectExpr("CAST(key AS String)", "CAST(value AS String)")
            .writeStream
            .format("console")
            .option("truncate", "false")
            .start()
            .awaitTermination()

        spark.close
    }
}