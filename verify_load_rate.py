"""验证冷机负载率原始数据 - 检查125%是否来自Excel源文件"""
import pandas as pd
from pathlib import Path
import sys
sys.stdout.reconfigure(encoding='utf-8')

# 读取 G11-1 负载率Excel
excel_path = Path(r"D:\Github\carbon_metrics\data1\冷源设备相关数据反馈20260120\制冷主机\冷机负载率\G11-1冷机负载率历史数据报表_2026-01-20 12-30-49.xlsx")

print(f"读取文件: {excel_path.name}")
print("=" * 80)

# 读取Excel（通常第一行是表头）
df = pd.read_excel(excel_path)

print(f"\n列名: {df.columns.tolist()}")
print(f"总行数: {len(df)}")
print(f"\n前5行数据:")
print(df.head())

# G11-1 Excel列名固定: ['标签', '采集时间', '采集值', '单位']
time_col = '采集时间'
load_col = '采集值'

print(f"\n检测到的列:")
print(f"  时间列: {time_col}")
print(f"  负载率列: {load_col}")

if time_col and load_col:
    # 转换时间列
    df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
    
    # 筛选2025年7月数据
    july_data = df[
        (df[time_col] >= '2025-07-01') & 
        (df[time_col] < '2025-08-01')
    ].copy()
    
    print(f"\n2025年7月数据:")
    print(f"  记录数: {len(july_data)}")
    
    if len(july_data) > 0:
        # 转换负载率为数值
        july_data[load_col] = pd.to_numeric(july_data[load_col], errors='coerce')
        
        # 统计
        max_val = july_data[load_col].max()
        min_val = july_data[load_col].min()
        avg_val = july_data[load_col].mean()
        
        print(f"  最大值: {max_val}")
        print(f"  最小值: {min_val}")
        print(f"  平均值: {avg_val:.2f}")
        
        # 找出>100%的记录
        over_100 = july_data[july_data[load_col] > 100]
        print(f"\n  >100%的记录数: {len(over_100)}")
        
        if len(over_100) > 0:
            print(f"\n  >100%的样本（前10条）:")
            print(over_100[[time_col, load_col]].head(10).to_string(index=False))
            
            # 找出最大值的记录
            max_records = july_data[july_data[load_col] == max_val]
            print(f"\n  最大值({max_val})出现时间:")
            print(max_records[[time_col, load_col]].to_string(index=False))
    else:
        print("  未找到2025年7月的数据")
else:
    print("\n无法自动识别时间列或负载率列，请手动检查")
