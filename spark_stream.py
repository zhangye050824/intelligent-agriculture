from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg

spark = SparkSession.builder \
    .appName("agri_realtime") \
    .master("local[1]") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 读取kafka原始数据流
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "agri_sensor") \
    .option("startingOffsets", "latest") \
    .load()

# 定义JSON schema，解析传感器数据
schema = "greenhouse_id string, air_temp double, air_humidity double, soil_humidity double, light double, timestamp long"

df_parse = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 转换时间戳为timestamp类型用于窗口计算
df_parse = df_parse.withColumn("ts", col("timestamp") / 1000).selectExpr("to_timestamp(ts) as event_time", "*")

# ====核心：按大棚分组 + 5秒滑动窗口，滑动1秒，聚合求均值====
df_window = df_parse.groupBy(
    col("greenhouse_id"),
    window(col("event_time"), windowDuration="5 seconds", slideDuration="1 second")
).agg(
    avg("air_temp").alias("air_temp_avg"),
    avg("air_humidity").alias("air_humidity_avg"),
    avg("soil_humidity").alias("soil_humidity_avg"),
    avg("light").alias("light_avg")
)

# 整理输出字段，把窗口结束时间作为时间戳输出
df_out = df_window.selectExpr(
    "greenhouse_id",
    "window.end as ts",
    "air_temp_avg",
    "air_humidity_avg",
    "soil_humidity_avg",
    "light_avg"
).selectExpr("to_json(struct(*)) as value")

# 输出结果写回kafka agri_result主题
query = df_out.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("topic", "agri_result") \
    .option("checkpointLocation", "file:///home/hadoop/agri_checkpoint") \
    .outputMode("update") \
    .start()

query.awaitTermination()
