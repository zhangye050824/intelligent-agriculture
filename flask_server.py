import json
import io
import re
import datetime
from kafka import KafkaConsumer
from flask import Flask, render_template, request, Response
from flask_socketio import SocketIO
import threading
import pymysql

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret'
socketio = SocketIO(app, cors_allowed_origins="*")


MYSQL_CONF = dict(host='127.0.0.1', port=3306, user='root', password='123456',
                  database='agriculture_analysis', charset='utf8')
DB_NAME = 'agriculture_analysis'
GH_LABELS = ['大棚一', '大棚二', '大棚三']
# 真实环境中大棚编号 -> 中文名称的归一映射
_GH_MAP = {'gh_01': '大棚一', 'gh_02': '大棚二', 'gh_03': '大棚三',
           '1': '大棚一', '2': '大棚二', '3': '大棚三'}


# kafka消费者，单独后台线程运行
def kafka_consumer_task():
    consumer = KafkaConsumer(
        'agri_result',
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for msg in consumer:
        data = msg.value
        print(f"收到计算结果:{data}")
        socketio.emit("agri_update", data)  # 推送消息给前端


@app.route('/')
def index():
    resp = Response(render_template("index.html"))
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    resp.headers['Pragma'] = 'no-cache'
    resp.headers['Expires'] = '0'
    return resp


# ============ 大棚编号 / 时间 工具函数 ============
# 缓存：表 -> (列是否存在, 是否真的含多个大棚)
_TABLE_INFO_CACHE = {}


def _table_info(table):
    """返回 (列存在, 是否多棚真值)。

    - 老表没有 greenhouse_id 列        -> (False, False)：按窗口时间合成三棚
    - 列存在但全是默认值(如全为 gh_01) -> (True, False) ：单路流，仍按时间合成三棚
    - 列存在且确实含 >=2 个大棚        -> (True, True)   ：使用真实大棚编号
    """
    if table in _TABLE_INFO_CACHE:
        return _TABLE_INFO_CACHE[table]
    conn = pymysql.connect(**MYSQL_CONF)
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s AND column_name='greenhouse_id'",
        (DB_NAME, table)
    )
    exists = cur.fetchone()[0] > 0
    real = False
    if exists:
        cur.execute(f"SELECT COUNT(DISTINCT NULLIF(greenhouse_id, '')) FROM {table}")
        real = (cur.fetchone()[0] or 0) >= 2
    cur.close()
    conn.close()
    _TABLE_INFO_CACHE[table] = (exists, real)
    return exists, real


def _norm_gh(v):
    """把真实大棚编号归一为中文名称；无法识别则原样返回"""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.lower() in _GH_MAP:
        return _GH_MAP[s.lower()]
    if s in GH_LABELS:
        return s
    return s


def _synth_gh(ws):
    """老库没有 greenhouse_id 时，按窗口开始时间确定性分配大棚。

    5 秒一个窗口，以 15 秒为一个周期轮流分给 大棚一/二/三；
    Spark 与 Flink 只要窗口开始时间相同，就一定会被分到同一个大棚，
    因此双引擎对齐对比依然成立。
    """
    sod = None
    if hasattr(ws, 'hour'):
        sod = ws.hour * 3600 + ws.minute * 60 + ws.second
    else:
        m = re.search(r'(\d{1,2}):(\d{2}):(\d{2})\s*$', str(ws))
        if m:
            sod = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))
    if sod is None:
        return GH_LABELS[sum(map(ord, str(ws))) % 3]
    return GH_LABELS[(sod // 5) % 3]


def _gh_of(raw_gh, win_start):
    g = _norm_gh(raw_gh)
    return g if g else _synth_gh(win_start)


def _fmt_t(v):
    """时间列统一格式化为字符串（供前端图表，空格分隔）"""
    return v.strftime('%Y-%m-%d %H:%M:%S') if hasattr(v, 'strftime') else str(v)


def _iso_t(v):
    """时间列格式化为文本（供 CSV 导出）。

    用空格分隔日期与时间，Excel/WPS 会按文本原样显示，秒不会被默认隐藏。
    """
    if hasattr(v, 'strftime'):
        return v.strftime('%Y-%m-%d %H:%M:%S')
    return str(v)


def _num(v):
    return round(float(v), 2) if v is not None else ''


# ============ 历史窗口数据导出（CSV，可直接用 Excel 打开） ============
# src=flink 导出 Flink 结果表；src=spark 导出 Spark 结果表
@app.route('/export')
def export_csv():
    src = request.args.get('src', 'flink')
    table = 'window_stat' if src == 'spark' else 'window_stat_flink'

    has_col, real_gh = _table_info(table)
    gh_col = 'greenhouse_id, ' if has_col else 'NULL AS greenhouse_id, '
    sql = (f"SELECT {gh_col}win_start, win_end, avg_temp, avg_humidity, "
           f"avg_soil_humidity, max_light FROM {table} "
           f"ORDER BY win_start DESC LIMIT 2000")

    conn = pymysql.connect(**MYSQL_CONF)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    sio = io.StringIO()
    sio.write('\ufeff')  # UTF-8 BOM，保证 Excel 打开中文不乱码
    sio.write('大棚,窗口开始,窗口结束,平均温度(℃),平均空气湿度(%),平均土壤湿度(%),最大光照(lux)\n')
    for r in rows:
        gh = _norm_gh(r[0]) if real_gh else _synth_gh(r[1])
        sio.write('{},{},{},{},{},{},{}\n'.format(
            gh, _iso_t(r[1]), _iso_t(r[2]),
            _num(r[3]), _num(r[4]), _num(r[5]), _num(r[6])
        ))

    fname = f'greenhouse_{src}_window_data.csv'
    return Response(
        sio.getvalue(),
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename={fname}'}
    )


# ============ 双引擎对比（Spark vs Flink） ============
def _load_engine_windows(table, limit=120):
    """读取某引擎最近 N 个（大棚, 窗口）结果。

    GROUP BY 去重：Spark complete 模式会反复重写同一窗口产生多行，
    对重复行取 AVG 即为该窗口最终值，两种引擎通用。
    老表无 greenhouse_id 列时按窗口时间合成大棚。
    """
    has_col, real_gh = _table_info(table)
    conn = pymysql.connect(**MYSQL_CONF)
    cur = conn.cursor()
    if has_col:
        # 有真实大棚编号时过滤掉历史 NULL 行，避免浪费 LIMIT 配额
        gh_filter = "WHERE greenhouse_id IS NOT NULL AND greenhouse_id != ''" if real_gh else ""
        cur.execute(
            f"SELECT greenhouse_id, win_start, win_end, AVG(avg_temp), AVG(avg_humidity), "
            f"AVG(avg_soil_humidity), AVG(max_light) FROM {table} {gh_filter} "
            f"GROUP BY greenhouse_id, win_start, win_end "
            f"ORDER BY win_start DESC LIMIT {int(limit)}"
        )
    else:
        cur.execute(
            f"SELECT win_start, win_end, AVG(avg_temp), AVG(avg_humidity), "
            f"AVG(avg_soil_humidity), AVG(max_light) FROM {table} "
            f"GROUP BY win_start, win_end "
            f"ORDER BY win_start DESC LIMIT {int(limit)}"
        )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    out = {}
    for r in rows:
        if has_col:
            raw_gh, ws, we, t, h, soil, light = r
        else:
            raw_gh, ws, we = None, r[0], r[1]
            t, h, soil, light = r[2], r[3], r[4], r[5]
        gh = _norm_gh(raw_gh) if real_gh else _synth_gh(ws)
        key = (gh, _fmt_t(ws))
        out[key] = {
            'win_end': _fmt_t(we),
            'temp': round(float(t), 2) if t is not None else None,
            'hum': round(float(h), 2) if h is not None else None,
            'soil': round(float(soil), 2) if soil is not None else None,
            'light': round(float(light), 2) if light is not None else None,
        }
    return out


def _engine_stats(table):
    """引擎运行概况：累计窗口数 / 最近窗口时间 / 是否存活（120 秒内有新窗口）"""
    conn = pymysql.connect(**MYSQL_CONF)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(DISTINCT win_start), MAX(win_end) FROM {table}")
    cnt, last = cur.fetchone()
    cur.close()
    conn.close()
    alive = last is not None and \
        (datetime.datetime.now() - last).total_seconds() < 120
    return {'windows': int(cnt), 'last': _fmt_t(last) if last else '--', 'alive': bool(alive)}


def _aligned_pairs(spark_map, flink_map, max_n):
    """按 (大棚, win_start)（5 秒整点对齐）取两引擎都存在的窗口，
    返回带偏差的对比列表。

    兜底：若 Spark 端 greenhouse_id 缺失（归一化为 None），
    则按同一 win_start 将 Spark 数据与 Flink 的各大棚行配对，
    确保双引擎对比图始终有数据可见。
    """

    def diff(a, b):
        return round(abs(a - b), 2) if (a is not None and b is not None) else None

    def make_pair(gh, ws, s, f):
        return {
            'gh': gh, 'win_start': ws, 'win_end': s['win_end'],
            'spark_temp': s['temp'], 'flink_temp': f['temp'], 'diff_temp': diff(s['temp'], f['temp']),
            'spark_hum': s['hum'], 'flink_hum': f['hum'], 'diff_hum': diff(s['hum'], f['hum']),
            'spark_soil': s['soil'], 'flink_soil': f['soil'], 'diff_soil': diff(s['soil'], f['soil']),
            'spark_light': s['light'], 'flink_light': f['light'], 'diff_light': diff(s['light'], f['light']),
        }

    # ---- 1. 严格对齐：(gh, win_start) 完全相同 ----
    # 注意：必须按 win_start 排序作为主序，否则 Python tuple 会先按 gh_label 字符串排序，
    # 导致所有大棚二堆在最前、大棚一排在最后，[:max_n] 只覆盖一两个大棚
    strict_keys = sorted(set(spark_map) & set(flink_map),
                         reverse=True, key=lambda k: k[1])[:max_n]
    pairs = []
    used_spark = set()
    used_flink = set()
    for k in strict_keys:
        gh, ws = k
        pairs.append(make_pair(gh, ws, spark_map[k], flink_map[k]))
        used_spark.add(k)
        used_flink.add(k)

    # ---- 2. 兜底对齐：Spark 端 gh=None 的行，按 win_start 匹配 Flink 端 ----
    if len(pairs) < max_n:
        # 收集 Spark 未匹配、且 gh 为 None 的行（按 win_start 分组）
        spark_null_by_ws = {}
        for k, v in spark_map.items():
            if k in used_spark:
                continue
            gh, ws = k
            if gh is None:
                spark_null_by_ws.setdefault(ws, []).append(v)

        # 收集 Flink 未匹配的行
        for k in sorted(flink_map.keys() - used_flink, reverse=True):
            if len(pairs) >= max_n:
                break
            gh, ws = k
            bucket = spark_null_by_ws.get(ws)
            if bucket:
                s = bucket.pop(0)  # 取一个 Spark NULL 行
                pairs.append(make_pair(gh, ws, s, flink_map[k]))
                used_flink.add(k)

    # ---- 3. 兜底对齐的反向：Flink 端 gh=None 时匹配 Spark 的各大棚 ----
    if len(pairs) < max_n:
        flink_null_by_ws = {}
        for k, v in flink_map.items():
            if k in used_flink:
                continue
            gh, ws = k
            if gh is None:
                flink_null_by_ws.setdefault(ws, []).append(v)

        for k in sorted(spark_map.keys() - used_spark, reverse=True):
            if len(pairs) >= max_n:
                break
            gh, ws = k
            bucket = flink_null_by_ws.get(ws)
            if bucket:
                f = bucket.pop(0)
                pairs.append(make_pair(gh, ws, spark_map[k], f))
                used_spark.add(k)

    pairs.sort(key=lambda x: x['win_start'], reverse=True)
    pairs = pairs[:max_n]
    pairs.reverse()  # 图表需要时间升序
    return pairs


# 实时对比接口：大屏每 5 秒轮询一次，?gh=大棚一 可按大棚过滤
@app.route('/api/engine_compare')
def api_engine_compare():
    gh_filter = request.args.get('gh', '').strip()
    spark_map = _load_engine_windows('window_stat', 150)
    flink_map = _load_engine_windows('window_stat_flink', 150)
    pairs = _aligned_pairs(spark_map, flink_map, 36)
    if gh_filter:
        pairs = [p for p in pairs if p['gh'] == gh_filter]
    pairs.reverse()  # 时间升序，图表从左往右
    return json.dumps({
        'spark': _engine_stats('window_stat'),
        'flink': _engine_stats('window_stat_flink'),
        'aligned': pairs,
        'current_gh': gh_filter or None,
    })


# 双引擎对比报告导出（CSV，同一窗口两引擎结果 + 偏差列）
@app.route('/export_compare')
def export_compare_csv():
    spark_map = _load_engine_windows('window_stat', 2000)
    flink_map = _load_engine_windows('window_stat_flink', 2000)
    pairs = _aligned_pairs(spark_map, flink_map, 2000)

    def c(x):
        return '' if x is None else x

    sio = io.StringIO()
    sio.write('\ufeff')  # UTF-8 BOM，Excel 打开中文不乱码
    sio.write('双引擎窗口结果对比报告（Spark Structured Streaming vs Flink DataStream）\n')
    sio.write('对齐方式：同一大棚 + 窗口开始时间相同的 5 秒窗口；偏差=|Spark-Flink|\n')
    sio.write('大棚,窗口开始,窗口结束,'
              'Spark均温(℃),Flink均温(℃),温度偏差(℃),'
              'Spark空气湿度(%),Flink空气湿度(%),湿度偏差(%),'
              'Spark土壤湿度(%),Flink土壤湿度(%),土壤湿度偏差(%),'
              'Spark最大光照(lux),Flink最大光照(lux),光照偏差(lux)\n')
    for p in pairs:
        sio.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(
            p['gh'], p['win_start'], p['win_end'],
            c(p['spark_temp']), c(p['flink_temp']), c(p['diff_temp']),
            c(p['spark_hum']), c(p['flink_hum']), c(p['diff_hum']),
            c(p['spark_soil']), c(p['flink_soil']), c(p['diff_soil']),
            c(p['spark_light']), c(p['flink_light']), c(p['diff_light'])))

    return Response(
        sio.getvalue(),
        mimetype='text/csv; charset=utf-8',
        headers={'Content-Disposition': 'attachment; filename=greenhouse_dual_engine_compare.csv'}
    )


if __name__ == "__main__":
    # 启动消费线程
    t = threading.Thread(target=kafka_consumer_task, daemon=True)
    t.start()
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
