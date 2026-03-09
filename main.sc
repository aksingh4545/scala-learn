import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GlueApp {
  def main(sysArgs: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    // 1. Read raw CSV from S3
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3://scala-glue-data/raw/sample_dataset.csv")

    // 2. Clean missing values
    val cleanedDF = df.na.fill(Map(
      "Age"           -> 30,
      "City"          -> "Unknown",
      "Salary"        -> 0,
      "Payment_Method"-> "Unknown"
    ))

    // 3. Feature engineering
    val enrichedDF = cleanedDF.withColumn(
      "Salary_Category",
      when(col("Salary") > 80000, "High")
        .when(col("Salary") > 50000, "Medium")
        .otherwise("Low")
    )

    // 4. Aggregation
    val cityAnalytics = enrichedDF.groupBy("City").agg(
      avg("Purchase_Amount").alias("Avg_Purchase"),
      count("Customer_ID").alias("Total_Customers")
    )

    // Optional: preview in logs
    cityAnalytics.show()

    // 5. Write processed data as Parquet
    cityAnalytics.write
      .mode("overwrite")
      .parquet("s3://scala-glue-data/processed/")
  }
}
