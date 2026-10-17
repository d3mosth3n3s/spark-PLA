import json
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("pageviews-dashboard-export").getOrCreate()

raw = spark.read.text("/data/pageviews-*.gz")
p = F.split("value", " ")
df = raw.select(
    F.regexp_extract(F.input_file_name(), r"pageviews-\d{8}-(\d{2})", 1).cast("int").alias("hour"),
    p[0].alias("domain"),
    p[1].alias("title"),
    p[2].cast("long").alias("views"),
).filter(F.col("views").isNotNull())

hourly = (df.filter(F.col("domain").isin("en", "en.m"))
            .groupBy("title", "hour").agg(F.sum("views").alias("views")))

totals = (hourly.groupBy("title")
          .agg(F.sum("views").alias("total"), F.max("views").alias("peak"))
          .withColumn("ratio", F.col("peak") / (F.col("total") / 24)))


top = totals.orderBy(F.desc("total")).limit(300).collect()

spikes = (totals.filter(F.col("total") >= 1000)
          .orderBy(F.desc("ratio")).limit(100).collect())

sel = spark.createDataFrame([(r.title,) for r in top + spikes], ["title"]).distinct()
series = {}
for r in hourly.join(F.broadcast(sel), "title").collect():
    series.setdefault(r.title, [0] * 24)[r.hour] = r.views

peak_hour = {t: max(range(24), key=lambda h: v[h]) for t, v in series.items()}

lang = F.split("domain", r"\.")[0].alias("lang")
lang_hourly = df.select(lang, "hour", "views").groupBy("lang", "hour").agg(F.sum("views").alias("views"))
top_langs = [r.lang for r in lang_hourly.groupBy("lang").agg(F.sum("views").alias("t"))
             .orderBy(F.desc("t")).limit(8).collect()]
langs = {l: [0] * 24 for l in top_langs}
for r in lang_hourly.filter(F.col("lang").isin(top_langs)).collect():
    langs[r.lang][r.hour] = r.views

out = {
    "scope": "titles: en + en.m; languages: all projects. Hours are UTC.",
    "top": [{"title": r.title, "views": r.total} for r in top],
    "spikes": [{"title": r.title, "total": r.total, "peak": r.peak,
                "peak_hour": peak_hour[r.title], "ratio": round(r.ratio, 2)} for r in spikes],
    "series": series,
    "langs": langs,
}
with open("/opt/jobs/dashboard_data.json", "w") as f:
    json.dump(out, f)

print("Wrote dashboard_data.json:", len(out["top"]), "top pages,", len(out["spikes"]), "spikes")
spark.stop()