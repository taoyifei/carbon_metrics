"""快速测试脚本 - 验证系统核心功能"""
import sys
from pathlib import Path
from datetime import datetime

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from ground_truth_validation.excel_reader import ExcelReader
from ground_truth_validation.orchestrator import MetricOrchestrator
from ground_truth_validation.backend_client import BackendAPIClient

def test_excel_reader():
    """测试 Excel 读取器"""
    print("=" * 60)
    print("测试 1: Excel 读取器初始化")
    print("=" * 60)
    
    data_dir = Path(r"D:\Github\carbon_metrics\data1")
    reader = ExcelReader(data_dir)
    print(f"[OK] ExcelReader 初始化成功")
    print(f"  数据目录: {data_dir}")
    
    # 统计文件数量
    all_files = list(data_dir.rglob("*.xlsx"))
    valid_files = [f for f in all_files if not f.name.startswith('~$')]
    skip_keywords = ['参数', '编号', '型号', '配置', '说明', '汇总']
    data_files = [f for f in valid_files if not any(kw in f.name for kw in skip_keywords)]
    
    print(f"  总文件数: {len(all_files)}")
    print(f"  有效文件数: {len(valid_files)}")
    print(f"  数据文件数: {len(data_files)}")
    print()
    
    return reader

def test_orchestrator(reader):
    """测试编排器"""
    print("=" * 60)
    print("测试 2: 指标编排器初始化")
    print("=" * 60)
    
    orch = MetricOrchestrator(reader)
    print(f"[OK] MetricOrchestrator 初始化成功")
    print(f"  能耗计算器: {orch.energy.__class__.__name__}")
    print(f"  温度计算器: {orch.temperature.__class__.__name__}")
    print(f"  流量计算器: {orch.flow.__class__.__name__}")
    print(f"  冷机计算器: {orch.chiller.__class__.__name__}")
    print(f"  水泵计算器: {orch.pump.__class__.__name__}")
    print(f"  冷塔计算器: {orch.tower.__class__.__name__}")
    print(f"  稳定性计算器: {orch.stability.__class__.__name__}")
    print(f"  维护计算器: {orch.maintenance.__class__.__name__}")
    print()
    
    return orch

def test_backend_client():
    """测试后端客户端"""
    print("=" * 60)
    print("测试 3: 后端 API 客户端")
    print("=" * 60)
    
    client = BackendAPIClient("http://127.0.0.1:8000")
    
    try:
        # 测试健康检查
        response = client.session.get(f"{client.base_url}/health", timeout=5)
        if response.status_code == 200:
            print(f"[OK] 后端服务可访问")
            print(f"  URL: {client.base_url}")
        else:
            print(f"[ERROR] 后端服务返回错误: {response.status_code}")
    except Exception as e:
        print(f"[ERROR] 后端服务不可访问: {e}")
        print(f"  请确保后端已启动: uvicorn carbon_metrics.backend.main:app --reload")
    
    print()

def test_single_file_read(reader):
    """测试读取单个文件"""
    print("=" * 60)
    print("测试 4: 读取单个 Excel 文件")
    print("=" * 60)
    
    data_dir = Path(r"D:\Github\carbon_metrics\data1")
    
    # 找一个能耗文件
    energy_files = list(data_dir.rglob("*电量*.xlsx"))
    if not energy_files:
        print("[ERROR] 未找到能耗文件")
        return
    
    test_file = energy_files[0]
    print(f"测试文件: {test_file.name}")
    
    try:
        df = reader.read_excel_file(test_file)
        print(f"[OK] 文件读取成功")
        print(f"  行数: {len(df)}")
        print(f"  列: {df.columns}")
        if len(df) > 0:
            print(f"  时间范围: {df['timestamp'].min()} ~ {df['timestamp'].max()}")
            print(f"  指标类型: {df['metric_name'].unique().to_list()}")
    except Exception as e:
        print(f"[ERROR] 文件读取失败: {e}")
    
    print()

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Ground Truth 验证系统 - 快速测试")
    print("=" * 60)
    print()
    
    try:
        reader = test_excel_reader()
        orch = test_orchestrator(reader)
        test_backend_client()
        test_single_file_read(reader)
        
        print("=" * 60)
        print("测试完成")
        print("=" * 60)
        print()
        print("下一步:")
        print("1. 如果后端不可访问，请先启动后端服务")
        print("2. 运行 validation.ipynb 进行完整验证")
        
    except Exception as e:
        print(f"\n[ERROR] 测试失败: {e}")
        import traceback
        traceback.print_exc()
