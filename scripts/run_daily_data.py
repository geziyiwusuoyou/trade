# scripts/run_daily_data.py
import sys
from pathlib import Path

# 将项目根目录加入 Path，防止报错找不到模块
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from data_center.collectors.adapter_qmt import QMTDataLoader

def main():
    print(">>> 开始执行每日数据更新任务")
    loader = QMTDataLoader()
    # 每天收盘后跑，更新最近 5 天的数据以防遗漏
    loader.run_etl(lookback_days=20)

if __name__ == "__main__":
    main()