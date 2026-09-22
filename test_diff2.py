import pymysql
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456',
                       database='agriculture_analysis', charset='utf8')
cur = conn.cursor()
# 检查 Spark 表是否有多行同一窗口（complete模式重写）
cur.execute("""
    SELECT win_start, COUNT(*) as cnt, greenhouse_id,
           MIN(avg_temp), MAX(avg_temp),
           MIN(avg_humidity), MAX(avg_humidity)
    FROM window_stat
    WHERE win_start >= NOW() - INTERVAL 30 MINUTE
    GROUP BY win_start, greenhouse_id
    HAVING cnt > 1
    ORDER BY win_start DESC LIMIT 5
""")
rows = cur.fetchall()
print(f"Spark complete模式重写的窗口: {len(rows)}")
for r in rows[:5]:
    print(f"  {r[0]} gh={r[2]} cnt={r[1]} temp=[{r[3]:.4f},{r[4]:.4f}] hum=[{r[5]:.4f},{r[6]:.4f}]")

# 对比 Spark vs Flink 原始精度
cur.execute("""
    SELECT w.win_start, w.greenhouse_id as w_gh, f.greenhouse_id as f_gh,
           w.avg_temp, f.avg_temp, ABS(w.avg_temp - f.avg_temp) as diff_t,
           w.avg_humidity, f.avg_humidity, ABS(w.avg_humidity - f.avg_humidity) as diff_h,
           w.avg_soil_humidity, f.avg_soil_humidity, ABS(w.avg_soil_humidity - f.avg_soil_humidity) as diff_s,
           w.max_light, f.max_light, ABS(w.max_light - f.max_light) as diff_l
    FROM window_stat w
    JOIN window_stat_flink f ON w.win_start = f.win_start
    WHERE w.greenhouse_id IS NULL AND f.greenhouse_id = 'gh_01'
    ORDER BY w.win_start DESC LIMIT 10
""")
rows = cur.fetchall()
print(f"\nSpark vs Flink 原始数值对比 (gh_01):")
print(f"{'win_start':22} {'sp_t':>14} {'fl_t':>14} {'d_t':>12} | {'sp_h':>14} {'fl_h':>14} {'d_h':>12}")
for r in rows:
    print(f"{str(r[0]):22} {r[3]:14.10f} {r[4]:14.10f} {r[5]:12.10f} | {r[6]:14.10f} {r[7]:14.10f} {r[8]:12.10f}")
cur.close()
conn.close()
