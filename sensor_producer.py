import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 三个大棚，各自独立的指标量程：
# gh_02 温度上限高于 30℃，便于在页面上演示"高温告警"效果
GREENHOUSES = {
    "gh_01": {"temp": (24, 30), "hum": (55, 70), "soil": (40, 60), "light": (3000, 5000)},
    "gh_02": {"temp": (28, 36), "hum": (45, 65), "soil": (35, 55), "light": (3500, 5500)},
    "gh_03": {"temp": (22, 28), "hum": (50, 68), "soil": (38, 58), "light": (2000, 4500)},
}

if __name__ == "__main__":
    while True:
        ts = int(time.time() * 1000)
        for gh_id, rng in GREENHOUSES.items():
            data = {
                "greenhouse_id": gh_id,                          # 大棚编号，窗口聚合按它分组
                "air_temp": round(random.uniform(*rng["temp"]), 2),    # 空气温度
                "air_humidity": round(random.uniform(*rng["hum"]), 2), # 空气湿度
                "soil_humidity": round(random.uniform(*rng["soil"]), 2),  # 土壤湿度
                "light": round(random.uniform(*rng["light"]), 2),      # 光照强度
                "timestamp": ts
            }
            # 同一条消息发两个主题：agri_sensor 给 Spark，sensor_data 给 Flink
            producer.send("agri_sensor", value=data)
            producer.send("sensor_data", value=data)
            print(f"发送传感器数据:{data}")
        time.sleep(2)  # 每 2 秒一轮，每轮 3 条（每棚 1 条）
