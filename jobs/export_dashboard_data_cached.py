# Same as export_dashboard_data.py, but caches the two aggregates that are reused,
# so the 1.4 GB input is scanned 2 times instead of 5.
import json
from pyspark.sql import SparkSession, functions as F

# Create a SparkSession
spark = SparkSession.builder.appName("pageviews-dashboard-export-CACHED").getOrCreate() 

# Define DataFrame, Spark is lazy, so no reads yet, just a plan.
raw = spark.read.text("/data/pageviews-*.gz") 

# Split the "value" column into separate fields for domain, title, and views.
p = F.split("value", " ") 
df = raw.select( 
    F.regexp_extract(F.input_file_name(), r"pageviews-\d{8}-(\d{2})", 1).cast("int").alias("hour"), 
    p[0].alias("domain"),
    p[1].alias("title"),
    p[2].cast("long").alias("views"),
).filter(F.col("views").isNotNull()) # Keep only rows where "views" parsed as a number.

# Cached table for the hourly aggregates, so we don't have to scan the input multiple times.
hourly = (df.filter(F.col("domain").isin("en", "en.m"))
            .groupBy("title", "hour").agg(F.sum("views").alias("views"))
            .cache()) # Filters only english desktop/mobile. Merges their views together. 

# Per page: total views for the day, peak = busiest hour's views, ratio = peak / average hour (how spiky it is).
totals = (hourly.groupBy("title")
          .agg(F.sum("views").alias("total"), F.max("views").alias("peak"))
          .withColumn("ratio", F.col("peak") / (F.col("total") / 24))) 

# 300 most-viewed pages of the day. collect() is an action, so this is where Spark actually runs (and fills the cache).
top = totals.orderBy(F.desc("total")).limit(300).collect()

# 100 highest-spike pages: busiest hour above their hourly average.
spikes = (totals.filter(F.col("total") >= 1000)
          .orderBy(F.desc("ratio")).limit(100).collect())

# table of every title we need a chart for (top + spikes, no duplicates).
sel = spark.createDataFrame([(r.title,) for r in top + spikes], ["title"]).distinct()

# Build a 24-slot hourly series per title. broadcast() ships the table above to every executor, so the join avoids a shuffle.
series = {}
for r in hourly.join(F.broadcast(sel), "title").collect():
    series.setdefault(r.title, [0] * 24)[r.hour] = r.views # hours with no rows stay 0

hourly.unpersist()   # done with it: free executor memory before the language scan

peak_hour = {t: max(range(24), key=lambda h: v[h]) for t, v in series.items()} # dictionary that maps each title to the hour with the maximum views in the series.

# Language code = the part of the domain before the first dot ("en.m" -> "en", "de" -> "de").
lang = F.split("domain", r"\.")[0].alias("lang")
# Cached table of views per language per hour, across all projects. This is the last full scan of the input.
lang_hourly = (df.select(lang, "hour", "views").groupBy("lang", "hour")
               .agg(F.sum("views").alias("views"))
               .cache())   

# 8 languages with the most total views for the day.
top_langs = [r.lang for r in lang_hourly.groupBy("lang").agg(F.sum("views").alias("t"))
             .orderBy(F.desc("t")).limit(8).collect()]

# Fill a 24hr series for each of those languages (read from the cache)
langs = {l: [0] * 24 for l in top_langs}
for r in lang_hourly.filter(F.col("lang").isin(top_langs)).collect():
    langs[r.lang][r.hour] = r.views

# Dashboard stuff.
out = {
    "scope": "titles: en + en.m; languages: all projects. Hours are UTC.",
    "top": [{"title": r.title, "views": r.total} for r in top],
    "spikes": [{"title": r.title, "total": r.total, "peak": r.peak,
                "peak_hour": peak_hour[r.title], "ratio": round(r.ratio, 2)} for r in spikes],
    "series": series,
    "langs": langs,
}
with open("/opt/jobs/dashboard_data_cached.json", "w") as f:
    json.dump(out, f) # dump the output dictionary to a JSON file.

print("Wrote dashboard_data_cached.json:", len(out["top"]), "top pages,", len(out["spikes"]), "spikes")
spark.stop()