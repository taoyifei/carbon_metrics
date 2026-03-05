"""检查 load_rate 数据是否存在于 agg_hour 表"""
import os
import pymysql
from datetime import datetime

# 从环境变量读取数据库配置
db_config = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': int(os.getenv('DB_PORT', '3306')),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'cooling_system_v2'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

print(f"连接数据库: {db_config['host']}:{db_config['port']}/{db_config['database']}")

try:
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    
    # 检查1: load_rate 数据总量
    print("\n=== 检查1: load_rate 在 agg_hour 中的总记录数 ===")
    cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            MIN(bucket_time) as earliest,
            MAX(bucket_time) as latest
        FROM agg_hour
        WHERE metric_name = 'load_rate'
    """)
    result = cursor.fetchone()
    print(f"总记录数: {result['total_records']}")
    print(f"时间范围: {result['earliest']} 到 {result['latest']}")
    
    # 检查2: 2025年7月的 load_rate 数据
    print("\n=== 检查2: 2025年7月 load_rate 数据 ===")
    cursor.execute("""
        SELECT 
            COUNT(*) as july_records,
            COUNT(DISTINCT building_id) as buildings,
            COUNT(DISTINCT equipment_id) as equipments,
            MIN(bucket_time) as earliest,
            MAX(bucket_time) as latest
        FROM agg_hour
        WHERE metric_name = 'load_rate'
          AND bucket_time >= '2025-07-01 00:00:00'
          AND bucket_time < '2025-08-01 00:00:00'
    """)
    result = cursor.fetchone()
    print(f"7月记录数: {result['july_records']}")
    print(f"涉及建筑数: {result['buildings']}")
    print(f"涉及设备数: {result['equipments']}")
    print(f"时间范围: {result['earliest']} 到 {result['latest']}")
    
    # 检查3: load_ratio 数据
    print("\n=== 检查3: load_ratio 数据 ===")
    cursor.execute("""
        SELECT COUNT(*) as total_records
        FROM agg_hour
        WHERE metric_name = 'load_ratio'
    """)
    result = cursor.fetchone()
    print(f"load_ratio 总记录数: {result['total_records']}")
    
    # 检查4: 冷机设备的 load_rate 数据（7月）
    print("\n=== 检查4: 冷机设备 7月 load_rate 明细 ===")
    cursor.execute("""
        SELECT 
            building_id,
            equipment_id,
            COUNT(*) as records,
            MIN(bucket_time) as earliest,
            MAX(bucket_time) as latest
        FROM agg_hour
        WHERE metric_name = 'load_rate'
          AND equipment_type = 'chiller'
          AND bucket_time >= '2025-07-01 00:00:00'
          AND bucket_time < '2025-08-01 00:00:00'
        GROUP BY building_id, equipment_id
        ORDER BY building_id, equipment_id
        LIMIT 10
    """)
    results = cursor.fetchall()
    if results:
        print(f"前10个设备:")
        for row in results:
            print(f"  {row['building_id']}/{row['equipment_id']}: {row['records']}条记录 ({row['earliest']} ~ {row['latest']})")
    else:
        print("  ❌ 没有找到冷机的 load_rate 数据！")
    
    # 检查5: 当前 DB_READ_TIMEOUT 设置
    print("\n=== 检查5: MySQL 超时设置 ===")
    cursor.execute("SHOW VARIABLES LIKE '%timeout%'")
    timeouts = cursor.fetchall()
    for row in timeouts:
        if 'read' in row['Variable_name'] or 'wait' in row['Variable_name']:
            print(f"  {row['Variable_name']}: {row['Value']}")
    
    cursor.close()
    conn.close()
    print("\n✅ 检查完成")
    
except Exception as e:
    print(f"\n❌ 错误: {e}")
