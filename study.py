from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StudyApp").getOrCreate()

# create branch studydatabrciks

textFile = spark.read.text("README.md")

print(textFile.count())
print(textFile.first())
