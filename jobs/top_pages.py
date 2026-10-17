from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("pageviews-top-pages").getOrCreate()

raw = spark.read.text("/data/pageviews-*.gz")
parts = F.split("value", " ")
df = raw.select(
    parts[0].alias("domain"),
    parts[1].alias("title"),
    parts[2].cast("long").alias("views"),
)

for i in range(5):
    top = (df.filter(F.col("domain") == "en")
             .groupBy("title")
             .agg(F.sum("views").alias("total_views"))
             .orderBy(F.desc("total_views"))
             .limit(20))
    top.show(truncate=False)

top.show(truncate=False)
print("Input partitions:", raw.rdd.getNumPartitions())
spark.stop()