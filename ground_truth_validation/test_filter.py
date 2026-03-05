"""测试文件过滤逻辑"""
from pathlib import Path

# 测试文件名
test_files = [
    "水泵编号型号额定参数.xlsx",
    "冷却塔编号型号额定参数.xlsx", 
    "冷机编号型号额定参数.xlsx",
    "G11-1冷机1#电量数据查询报表.xlsx",  # 正常数据文件
]

skip_keywords = ['参数', '编号', '型号', '配置', '说明', '汇总']

print("=" * 60)
print("文件过滤测试")
print("=" * 60)

for filename in test_files:
    should_skip = any(keyword in filename for keyword in skip_keywords)
    status = "[SKIP]" if should_skip else "[READ]"
    print(f"{status} {filename}")
    
    if should_skip:
        matched = [kw for kw in skip_keywords if kw in filename]
        print(f"       匹配关键词: {matched}")

print("\n" + "=" * 60)
print("结论:")
print("=" * 60)
print("所有参数文件都应该被 [SKIP]")
print("如果验证时仍然报错，请:")
print("1. 重启 Jupyter kernel")
print("2. 重新运行 validation.ipynb")
