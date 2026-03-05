"""批量检查所有G11建筑的负载率数据"""
import pandas as pd
from pathlib import Path
import sys
sys.stdout.reconfigure(encoding='utf-8')

base_dir = Path(r"D:\Github\carbon_metrics\data1\冷源设备相关数据反馈20260120\制冷主机\冷机负载率")
buildings = ['G11-1', 'G11-2', 'G11-3']

print("检查所有G11建筑的2025年7月负载率数据")
print("=" * 80)

for building in buildings:
    files = list(base_dir.glob(f"{building}*.xlsx"))
    if not files:
        print(f"\n{building}: 未找到文件")
        continue
    
    excel_path = files[0]
    print(f"\n{building}: {excel_path.name}")
    
    try:
        df = pd.read_excel(excel_path)
        time_col = '采集时间'
        load_col = '采集值'
        
        df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
        july_data = df[
            (df[time_col] >= '2025-07-01') & 
            (df[time_col] < '2025-08-01')
        ].copy()
        
        july_data[load_col] = pd.to_numeric(july_data[load_col], errors='coerce')
        
        if len(july_data) > 0:
            max_val = july_data[load_col].max()
            min_val = july_data[load_col].min()
            avg_val = july_data[load_col].mean()
            over_100_count = len(july_data[july_data[load_col] > 100])
            
            print(f"  记录数: {len(july_data)}")
            print(f"  最大值: {max_val}")
            print(f"  最小值: {min_val}")
            print(f"  平均值: {avg_val:.2f}")
            print(f"  >100%记录数: {over_100_count}")
            
            if max_val > 100:
                print(f"  ⚠️ 发现>100%的数据！")
                max_records = july_data[july_data[load_col] == max_val]
                print(f"  最大值出现时间:")
                for _, row in max_records.head(5).iterrows():
                    print(f"    {row[time_col]} -> {row[load_col]}%")
        else:
            print(f"  无2025年7月数据")
            
    except Exception as e:
        print(f"  读取失败: {e}")

print("\n" + "=" * 80)
print("结论: 如果所有G11建筑都没有>100%的数据，说明125%来自:")
print("  1. 数据库导入/聚合过程的bug")
print("  2. 或者查询的是其他建筑(G12)")
print("  3. 或者查询的是其他时间段")
