from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StudyApp").getOrCreate()

textFile = spark.read.text("README.md")

print(textFile.count())
print(textFile.first())

# create branch study huhu