# scripts/run_daily_selection.py
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from strategy_pool.selectors.policy.n_pattern_policy import NPatternSelector

def main():
    print(">>> 开始执行每日选股任务...")
    
    # 实例化并运行
    # 你可以在这里加 try-except，防止一个策略挂了影响后面
    try:
        strategy = NPatternSelector()
        strategy.run()
    except Exception as e:
        print(f"❌ NPatternSelector 运行失败: {e}")

    print(">>> 选股任务结束")

if __name__ == "__main__":
    main()