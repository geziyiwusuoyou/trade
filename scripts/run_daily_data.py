# scripts/run_daily_data.py
import sys
import argparse
from pathlib import Path
from datetime import datetime

# 路径 Hack
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from data_center.collectors.adapter_qmt import QMTDataLoader
from data_center.collectors.adapter_qmt_finance import QMTFinanceLoader

def main():
    # 1. 定义参数
    parser = argparse.ArgumentParser(description="QuantProject 数据统一更新入口")
    
    # 行情控制
    parser.add_argument("--market", type=str, default="none", choices=["full", "incr", "none"], 
                        help="行情更新模式: full(指定范围全量) / incr(自动增量) / none(不更新)")
    
    # 财务控制
    parser.add_argument("--finance", type=str, default="none", choices=["full", "incr", "none"], 
                        help="财务更新模式: full(指定范围全量) / incr(自动增量) / none(不更新)")
    
    # 日期控制 (仅在 full 模式下生效)
    parser.add_argument("--start", type=str, help="开始日期 (YYYYMMDD), 全量模式必填")
    parser.add_argument("--end", type=str, help="结束日期 (YYYYMMDD), 默认为今天")
    
    args = parser.parse_args()
    
    # 默认结束时间为今天
    today_str = datetime.now().strftime('%Y%m%d')
    end_date = args.end if args.end else today_str

    print("="*60)
    print(f"🚀 数据任务启动 | 行情: {args.market} | 财务: {args.finance}")
    if args.market == 'full' or args.finance == 'full':
        print(f"📅 指定时间范围: {args.start} ~ {end_date}")
    print("="*60)

    # ================= 处理行情 (Market) =================
    if args.market != "none":
        print("\n>>> [Task 1] 执行行情更新...")
        market_loader = QMTDataLoader()
        
        if args.market == "incr":
            # 增量：自动判断
            market_loader.run_incremental_update()
        elif args.market == "full":
            # 全量：必须有 Start
            if not args.start:
                print("❌ 错误: 行情全量模式 (--market full) 必须指定 --start")
                return
            market_loader.run_full_update(args.start, end_date)
    else:
        print("\n>>> [Task 1] 行情更新已跳过")

    # ================= 处理财务 (Finance) =================
    if args.finance != "none":
        print("\n>>> [Task 2] 执行财务更新...")
        fin_loader = QMTFinanceLoader()
        
        if args.finance == "incr":
            # 增量：自动判断
            fin_loader.run_incremental_update()
        elif args.finance == "full":
            # 全量：必须有 Start
            if not args.start:
                print("❌ 错误: 财务全量模式 (--finance full) 必须指定 --start")
                return
            fin_loader.run_full_update(args.start, end_date)
    else:
        print("\n>>> [Task 2] 财务更新已跳过")

    print("\n" + "="*60)
    print("✅ 所有任务执行完毕")

if __name__ == "__main__":
    sys.argv = [
        "run_daily_data.py",
        "--finance", "none",
        "--market", "incr",
        "--start", "20140101",
        "--end", "20251208"
    ]
    main()
