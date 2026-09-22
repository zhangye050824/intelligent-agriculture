# -*- coding: utf-8 -*-
"""
Flink 创新版：智慧农业大棚 5 秒事件时间滚动窗口实时统计
================================================================
数据流：
  Kafka(sensor_data, JSON)
    -->> FlinkKafkaConsumer 消费 + JSON 解析
    -->> 分配事件时间戳(数据自带 timestamp) / Watermark(允许 10 秒乱序)
    -->> keyBy(greenhouse_id 三大棚分区) + TumblingEventTimeWindows(5 秒事件时间滚动窗口)
    -->> ProcessWindowFunction 窗口触发时计算均值/最大值
    -->> JDBC Sink 写入 MySQL agriculture_analysis.window_stat_flink

与 Spark 版(spark_write_mysql.py) 的双引擎对比：
  1) 时间语义：Spark 用【处理时间】current_timestamp，Flink 用数据自带的【事件时间】，
     并通过 Watermark 正确处理乱序/晚到数据；
  2) 输出模式：Spark complete 模式每个 trigger 把所有历史窗口全量重算并重写 MySQL，
     同一窗口会产生大量重复行；Flink 每个事件时间窗口只在触发时 append 一行，结果干净；
  3) 容错：Flink 开启 10s Checkpoint(EXACTLY_ONCE)，Kafka 位点与窗口状态一起持久化，
     作业失败可从最近一次 Checkpoint 精确恢复；
  4) PyFlink 1.15.4 的 datastream 包未导出 TumblingEventTimeWindows，本作业基于
     WindowAssigner + EventTimeTrigger + TimeWindowSerializer 按官方语义实现该分配器，
     窗口触发时由 ProcessWindowFunction 完成聚合（窗口状态由状态后端按窗口托管）。
"""
import json
import datetime

from pyflink.common import Duration, Row, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors import (
    FlinkKafkaConsumer,
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import (
    TimeWindow,
    TimeWindowSerializer,
    Trigger,
    TriggerResult,
    WindowAssigner,
)


class EventTimeTrigger(Trigger):
    """事件时间触发器（对齐 Flink Java EventTimeTrigger 语义）。

    注意：必须实现 datastream.window.Trigger 的接口
    （on_element/on_event_time/on_processing_time/on_merge/clear），
    不能借用 pyflink.fn_execution.table 下的触发器——那是 Table
    API 窗口使用的，方法签名不同，会在 Beam Worker 中抛 TypeError。

    语义（与 Flink Java EventTimeTrigger 完全一致）：
    窗口收到第一条数据时，在 window.max_timestamp()（即 end-1 毫秒）处
    注册事件时间定时器；当 Watermark 越过该时刻，定时器触发 -> FIRE
    （计算并输出窗口结果）；窗口销毁时删除定时器。
    注意：必须注册在 end-1 而不是 end！PyFlink 窗口算子的状态清理
    定时器也注册在 end-1（allowedLateness=0），若触发时间晚于清理
    时间，窗口状态已被清空，FIRE 时拿不到任何数据，表现为“窗口不输出”。
    """

    def on_element(self, element, timestamp, window, ctx):
        ctx.register_event_time_timer(window.max_timestamp())
        return TriggerResult.CONTINUE

    def on_processing_time(self, time, window, ctx):
        return TriggerResult.CONTINUE

    def on_event_time(self, time, window, ctx):
        return TriggerResult.FIRE if time == window.max_timestamp() else TriggerResult.CONTINUE

    def can_merge(self) -> bool:
        return False

    def on_merge(self, window, ctx):
        pass

    def clear(self, window, ctx):
        ctx.delete_event_time_timer(window.max_timestamp())


class TumblingEventTimeWindows(WindowAssigner):
    """事件时间滚动窗口分配器。

    说明：PyFlink 1.15.4 的 datastream 包未直接导出该类，这里按 Flink
    Java 同名类的语义实现：窗口按事件时间对齐到 epoch 整点、非重叠，
    默认触发器为 EventTimeTrigger（Watermark 越过窗口末尾即触发）。
    """

    def __init__(self, size_ms: int, offset_ms: int = 0):
        self._size = size_ms
        self._offset = offset_ms

    def assign_windows(self, element, timestamp: int, context):
        start = TimeWindow.get_window_start_with_offset(
            timestamp, self._offset, self._size)
        return [TimeWindow(start, start + self._size)]

    def get_default_trigger(self, env):
        return EventTimeTrigger()

    def get_window_serializer(self):
        return TimeWindowSerializer()

    def is_event_time(self) -> bool:
        return True

    @staticmethod
    def of(size: Time, offset: Time = None):
        size_ms = size.to_milliseconds()
        offset_ms = 0 if offset is None else offset.to_milliseconds()
        return TumblingEventTimeWindows(size_ms, offset_ms)

# ============================== 配置 ==============================
KAFKA_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "sensor_data"
KAFKA_GROUP = "flink-window-stat"

WINDOW_SECONDS = 5        # 5 秒滚动窗口
WATERMARK_SECONDS = 10    # 水印：允许 10 秒乱序/晚到数据

MYSQL_URL = (
    "jdbc:mysql://127.0.0.1:3306/agriculture_analysis"
    "?useSSL=false&serverTimezone=Asia/Shanghai"
    "&allowPublicKeyRetrieval=true&characterEncoding=utf8"
)
MYSQL_TABLE = "window_stat_flink"   # Flink 专用对比表（结构与 window_stat 完全一致）
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

CHECKPOINT_DIR = "file:///home/hadoop/flink_checkpoint"

# ============================== 类型声明 ==============================
# Kafka JSON 解析后的传感器数据类型
INPUT_TYPE = Types.ROW_NAMED(
    ["greenhouse_id", "air_temp", "air_humidity", "soil_humidity", "light", "event_time"],
    [Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG()],
)

# 最终写入 MySQL 的类型：greenhouse_id, win_start, win_end, 4 个指标
OUTPUT_TYPE = Types.ROW([
    Types.STRING(),
    Types.SQL_TIMESTAMP(),
    Types.SQL_TIMESTAMP(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
    Types.DOUBLE(),
])


# ============================== 处理函数 ==============================
class ParseSensorMapFunction(MapFunction):
    """Kafka 消息 JSON 字符串 -> Row(大棚编号 + 4 个传感器字段 + 毫秒事件时间)"""

    def map(self, value):
        d = json.loads(value)
        return Row(
            greenhouse_id=str(d.get("greenhouse_id") or "gh_01"),  # 大棚编号，兼容缺失字段的旧消息
            air_temp=float(d["air_temp"]),
            air_humidity=float(d["air_humidity"]),
            soil_humidity=float(d["soil_humidity"]),
            light=float(d["light"]),
            event_time=int(d["timestamp"]),  # 生产者上报的毫秒时间戳（事件时间）
        )


class SensorTimestampAssigner(TimestampAssigner):
    """从数据本身的 timestamp 字段提取事件时间（毫秒）"""

    def extract_timestamp(self, element, record_timestamp):
        return element.event_time


class GreenhouseWindowFunction(ProcessWindowFunction):
    """窗口触发时对窗口内全部传感器记录做截断均值聚合：

    与 Spark 的简单算术平均形成差异对比：
      Spark: AVG(air_temp)                  —— 简单算术平均，对极端值敏感
      Flink: TRIMMED_MEAN(air_temp)         —— 去掉最高最低各1条后取平均，抗噪声
    优势体现：Flink ProcessWindowFunction 可以遍历窗口内全部元素，
    灵活实现排序/截断等自定义聚合；而 Spark SQL DataFrame 的 groupBy.agg
    只能用内置聚合函数（avg/max/min/count），做不了这种自定义逻辑。
    """

    def process(self, key, context, elements):
        temps = []
        hums = []
        soils = []
        max_light = None
        for e in elements:
            temps.append(e.air_temp)
            hums.append(e.air_humidity)
            soils.append(e.soil_humidity)
            max_light = e.light if max_light is None or e.light > max_light else max_light

        n = len(temps)
        if n == 0:
            return

        # 截断均值：排序后去掉首尾各 1 个再取平均（至少留 2 个）
        def trimmed_mean(values):
            if n <= 2:
                return sum(values) / len(values)
            sv = sorted(values)
            return sum(sv[1:-1]) / (n - 2)

        mean_temp = trimmed_mean(temps)
        mean_hum = trimmed_mean(hums)
        mean_soil = trimmed_mean(soils)

        win_start = datetime.datetime.fromtimestamp(context.window().start / 1000.0)
        win_end = datetime.datetime.fromtimestamp(context.window().end / 1000.0)
        print("[Flink窗口] 大棚=%s %s ~ %s 共%d条 截断均温=%.4f 截断均湿=%.4f 截断均土湿=%.4f 最大光照=%.4f"
              % (key, win_start, win_end, n, mean_temp, mean_hum, mean_soil, max_light))

        yield Row(
            key,  # greenhouse_id（keyBy 的分区键）
            win_start,
            win_end,
            round(mean_temp, 4),
            round(mean_hum, 4),
            round(mean_soil, 4),
            round(max_light, 4),
        )

    def clear(self, context):
        pass


# ============================== 主流程 ==============================
if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # 低配虚拟机单并行度

    # 10 秒一次 Checkpoint，EXACTLY_ONCE（Kafka 位点 + 窗口状态一起持久化）
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_checkpoint_storage_dir(CHECKPOINT_DIR)
    # 每 1 秒推进一次 Watermark
    env.get_config().set_auto_watermark_interval(1000)

    # ---------- Source：Kafka sensor_data（从最新位点开始消费） ----------
    kafka_consumer = FlinkKafkaConsumer(
        KAFKA_TOPIC,
        SimpleStringSchema(),
        {
            "bootstrap.servers": KAFKA_SERVERS,
            "group.id": KAFKA_GROUP,
        },
    )
    kafka_consumer.set_start_from_latest()

    # ---------- 事件时间 + Watermark（允许 10 秒乱序） ----------
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_SECONDS))
        .with_timestamp_assigner(SensorTimestampAssigner())
    )

    # ---------- 解析 -> 时间戳/水印 -> 5 秒事件时间滚动窗口聚合 ----------
    result_stream = (
        env.add_source(kafka_consumer, "sensor_data_kafka_source", Types.STRING())
        .map(ParseSensorMapFunction(), output_type=INPUT_TYPE)
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda event: event.greenhouse_id, key_type=Types.STRING())  # 按大棚编号分区窗口聚合
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(GreenhouseWindowFunction(), result_type=OUTPUT_TYPE)
    )

    # ---------- Sink：MySQL window_stat_flink（每窗口 1 行，批量1秒刷新） ----------
    jdbc_sink = JdbcSink.sink(
        "INSERT INTO {table} (greenhouse_id, win_start, win_end, avg_temp, avg_humidity, "
        "avg_soil_humidity, max_light) VALUES (?, ?, ?, ?, ?, ?, ?)".format(table=MYSQL_TABLE),
        OUTPUT_TYPE,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(MYSQL_URL)
        .with_driver_name(MYSQL_DRIVER)
        .with_user_name(MYSQL_USER)
        .with_password(MYSQL_PASSWORD)
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_size(1)
        .with_batch_interval_ms(1000)
        .with_max_retries(3)
        .build(),
    )
    result_stream.add_sink(jdbc_sink)

    print("==== Flink 窗口统计作业启动：sensor_data -> window_stat_flink ====")
    env.execute("greenhouse_flink_window_stat")
