from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lit

def main():
    # Create a local Spark session for the ETL demo
    spark = (
        SparkSession.builder
        .appName("pyspark-etl-example")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # Input/output paths
    src_csv = r"d:\Python\pyspark\sample.csv"
    out_parquet = r"d:\Python\pyspark\output_parquet"

    # Read source CSV
    df = (
        spark.read.option("header", True)
                  .option("inferSchema", True)
                  .csv(src_csv)
    )

    # Clean and enrich data
    df_clean = (
        df.withColumn("name", trim(col("name")))
          .withColumn("amount", col("amount").cast("double"))
          .filter(col("amount").isNotNull())
          .withColumn("status", when(col("amount") > 0, lit("ok")).otherwise(lit("bad")))
    )

    # Aggregate per category
    agg = (
        df_clean.groupBy("category")
                .agg({"amount": "sum", "*": "count"})
                .withColumnRenamed("sum(amount)", "total_amount")
                .withColumnRenamed("count(1)", "row_count")
    )

    # Write results and report
    agg.write.mode("overwrite").parquet(out_parquet)

    print("Result rows:", agg.count())
    agg.show(truncate=False)

    # Stop Spark to free resources
    spark.stop()

if __name__ == "__main__":
    main()
