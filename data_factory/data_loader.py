# -*- coding: utf-8 -*-
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from xtquant import xtdata

# 路径Hack，确保能导入 config 和 utils
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from trade.config import config
from trade.utils import finance

def run_etl(lookback_days=8): # 只需要最近30天数据即可跑通你的策略
    print(f">>> [DataFactory] 启动数据更新，存储路径: {config.DATA_SAVE_DIR}")
    
    # 1. 准备参数
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    # 2. 获取A股列表 (xtdata自带)
    stock_list = xtdata.get_stock_list_in_sector('沪深A股')
    print(f"    目标全市场股票数: {len(stock_list)}")

    # 3. 下载数据 (复用逻辑)
    total_stocks = len(stock_list)
    for i, stock in enumerate(stock_list):
        xtdata.download_history_data(stock, period='1d', start_time=start_str, end_time=end_str)
        
        # 为了不刷屏，每 500 只打印一次进度
        if (i + 1) % 500 == 0:
            print(f"   已触发下载进度: {i + 1}/{total_stocks}")
    # 4. 批量读取
    print("    读取数据并计算涨停标记...")
    data_dict = xtdata.get_market_data_ex(
        field_list=['time', 'open', 'high', 'low', 'close', 'volume', 'amount'],
        stock_list=stock_list,
        period='1d',
        start_time=start_str,
        end_time=end_str,
        fill_data=True
    )

    count = 0
    if not os.path.exists(config.DATA_SAVE_DIR):
        os.makedirs(config.DATA_SAVE_DIR)

    for code, df in data_dict.items():
        if df.empty or len(df) < 5: continue

        # --- 数据清洗与计算核心 ---
        try:
            # 加上 8 小时偏移
            df['time'] = pd.to_datetime(df['time'], unit='ms') + pd.Timedelta(hours=8)
            df.set_index('time', inplace=True)
            df.sort_index(inplace=True)

            # 2. 获取昨收 (用于计算当天的涨停价)
            df['prev_close'] = df['close'].shift(1)
            
            # 3. 获取股票名称 (用于判断ST)
            # 注意：xtdata.get_instrument_detail 需要本地有基础数据
            detail = xtdata.get_instrument_detail(code)
            name = detail['InstrumentName'] if detail else ""

            # 4. 计算涨停价 & 标记涨停
            # 这是一个逐行计算的过程，为了性能，用 apply
            def apply_limit_calc(row):
                if pd.isna(row['prev_close']): return False
                up, _, _ = finance.get_limit_price(code, row['prev_close'], name)
                return finance.is_limit_up(row['close'], up)

            df['limit_up_flag'] = df.apply(apply_limit_calc, axis=1)

            # --- 存储 ---
            # 每个股票存一个 Parquet
            safe_name = code.replace('.', '_')
            file_path = os.path.join(config.DATA_SAVE_DIR, f"{safe_name}.parquet")
            df.to_parquet(file_path)
            count += 1
            
        except Exception as e:
            continue

    print(f">>> [DataFactory] 完成！已生成 {count} 个带涨停标记的数据文件。")

if __name__ == "__main__":
    run_etl()