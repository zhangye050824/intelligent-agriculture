# ============================================================
# Spark 双引擎对比作业（处理时间 + 简单算术平均）
#   与 Flink 事件时间 + 截断均值形成双维度差异对比：
#     时间语义：Spark 处理时间 vs Flink 事件时间+Watermark
#     聚合算法：Spark 算术平均 vs Flink 截断均值
#   Sink1 -> MySQL window_stat   （append 模式，每窗口一行）
#   Sink2 -> Kafka agri_result   （update 模式实时推送大屏）
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, current_timestamp
from pyspark.sql.types import StructType, StructField, FloatType, LongType, StringType

spark = SparkSession.builder \
    .appName("GreenhouseStreamAgg") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "300") \
    .load()

schema = StructType([
    StructField("greenhouse_id", StringType()),
    StructField("air_temp", FloatType()),
    StructField("air_humidity", FloatType()),
    StructField("soil_humidity", FloatType()),
    StructField("light", FloatType()),
    StructField("timestamp", LongType())
])

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# ====处理时间：使用 Spark 处理消息的时间戳（Spark 的原生语义）====
# 与 Flink 的事件时间 + Watermark 形成对比：
#   - Spark 处理时间：消息何时被 Spark 读到就归入哪个窗口
#   - Flink 事件时间：消息自身携带的时间戳决定归入哪个窗口
# 当处理有延迟时，两者会把同一条消息归入不同窗口，产生差异
df_with_ts = json_df.withColumn("event_time", current_timestamp())

# ====5 秒滚动窗口 + Watermark 允许 10 秒晚到（与 Flink 窗口边界对齐）====
agg_df = df_with_ts \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(col("greenhouse_id"), window(col("event_time"), "5 seconds")) \
    .agg(
        avg("air_temp").alias("avg_temp"),
        avg("air_humidity").alias("avg_humidity"),
        avg("soil_humidity").alias("avg_soil_humidity"),
        max("light").alias("max_light")
    )

agg_df = agg_df \
    .withColumn("win_start", col("window.start")) \
    .withColumn("win_end", col("window.end")) \
    .drop("window") \
    .select("greenhouse_id", "win_start", "win_end",
            "avg_temp", "avg_humidity", "avg_soil_humidity", "max_light")

# ---------- Sink1：MySQL window_stat（双引擎对比表） ----------
jdbc_url = "jdbc:mariadb://127.0.0.1:3306/agriculture_analysis?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&sessionVariables=sql_mode=ANSI_QUOTES"
connection_properties = {
    "user": "root",
    "password": "123456",
    "driver": "org.mariadb.jdbc.Driver"
}

def write_to_mysql(batch_df, batch_id):
    try:
        batch_df.write.mode("append").jdbc(url=jdbc_url, table="window_stat", properties=connection_properties)
        print(f"[MySQL-Spark] 批次{batch_id}处理时间窗口写入成功，{batch_df.count()}行")
    except Exception as err:
        print(f"[MySQL-Spark] 批次{batch_id}写入失败: {err}")

q_mysql = agg_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_mysql) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "file:///tmp/greenhouse_chk_v3") \
    .queryName("agg_to_mysql") \
    .start()

# ---------- Sink2：Kafka agri_result（大屏 SocketIO 实时推送） ----------
kafka_out = agg_df.selectExpr(
    "greenhouse_id",
    "win_end as ts",
    "avg_temp as air_temp_avg",
    "avg_humidity as air_humidity_avg",
    "avg_soil_humidity as soil_humidity_avg",
    "max_light as light_avg"
).selectExpr("to_json(struct(*)) as value")

q_kafka = kafka_out.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "agri_result") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "file:///tmp/greenhouse_chk_kafka") \
    .queryName("agg_to_kafka") \
    .start()

spark.streams.awaitAnyTermination()
