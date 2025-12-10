# -*- coding: utf-8 -*-
"""
Module: adapter_qmt.py
Description: QMT 数据源适配器 - 支持全量/增量更新，精确涨跌停计算
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

# 引入 QMT SDK
from xtquant import xtdata

# 引入配置
from config import MARKET_DATA_DIR
from common.data_structs import BarFields

# ================= 1. 核心工具函数 =================

def _round_to_2_decimals(number):
    """A股价格专用四舍五入"""
    d = Decimal(str(number))
    return float(d.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP))

def _calculate_limit_price(code: str, name: str, prev_close: float):
    """计算涨跌停价格 (同前文逻辑)"""
    if prev_close is None or np.isnan(prev_close):
        return np.nan, np.nan
    
    limit_ratio = 0.10
    if code.startswith('688') or code.startswith('30'):
        limit_ratio = 0.20
    elif code.startswith('8') or code.startswith('4'):
        limit_ratio = 0.30
    elif 'ST' in name:
        limit_ratio = 0.05
    
    up_price = _round_to_2_decimals(prev_close * (1 + limit_ratio))
    down_price = _round_to_2_decimals(prev_close * (1 - limit_ratio))
    return up_price, down_price

# ================= 2. QMT 数据加载器类 =================

class QMTDataLoader:
    def __init__(self):
        self.save_dir = MARKET_DATA_DIR / "stock_daily"
        if not self.save_dir.exists():
            self.save_dir.mkdir(parents=True, exist_ok=True)

    def _get_local_last_date(self, file_path):
        """
        获取本地 Parquet 文件的最后一条数据日期
        :return: datetime对象 或 None (如果文件不存在)
        """
        if not file_path.exists():
            return None
        try:
            # 优化：只读取索引列，速度极快
            # engine='pyarrow' 通常比 fastparquet 快，视环境而定
            df = pd.read_parquet(file_path, columns=[BarFields.DATE_TIME])
            if df.empty:
                return None
            return df.index[-1] # 假设索引是 datetime
        except Exception:
            return None

    def fetch_and_update(self, stock_list, start_str, end_str, mode="append"):
        """
        通用核心方法：下载 -> 清洗 -> 存储
        :param stock_list: 股票代码列表
        :param start_str: '20230101'
        :param end_str: '20230105'
        :param mode: 'overwrite' (覆盖/全量) or 'append' (增量)
        """
        print(f"📥 [QMT] 正在下载数据: {start_str} ~ {end_str}, 模式: {mode}, 数量: {len(stock_list)}")
        
        # 1. 触发下载 (Blocking)
        for i, code in enumerate(stock_list):
            xtdata.download_history_data(code, period='1d', start_time=start_str, end_time=end_str)
            if (i + 1) % 500 == 0:
                print(f"   下载进度: {i + 1}/{len(stock_list)}")

        # 2. 批量获取
        qmt_fields = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        data_dict = xtdata.get_market_data_ex(
            field_list=qmt_fields,
            stock_list=stock_list,
            period='1d',
            start_time=start_str,
            end_time=end_str,
            fill_data=True
        )

        success_count = 0
        
        # 3. 逐个处理
        for code, new_df in data_dict.items():
            if new_df.empty: continue

            try:
                # --- 清洗 ---
                new_df['time'] = pd.to_datetime(new_df['time'], unit='ms') + pd.Timedelta(hours=8)
                new_df.rename(columns={
                    'time': BarFields.DATE_TIME, 'open': BarFields.OPEN,
                    'high': BarFields.HIGH, 'low': BarFields.LOW,
                    'close': BarFields.CLOSE, 'volume': BarFields.VOLUME,
                    'amount': BarFields.AMOUNT
                }, inplace=True)
                new_df.set_index(BarFields.DATE_TIME, inplace=True)
                new_df.sort_index(inplace=True)
                new_df[BarFields.CODE] = code
                new_df[BarFields.ADJ_FACTOR] = 1.0

                file_path = self.save_dir / f"{code}.parquet"

                # --- 模式处理 ---
                if mode == "append" and file_path.exists():
                    # 读取旧数据
                    old_df = pd.read_parquet(file_path)
                    # 合并 (concat) 并去重 (drop_duplicates)
                    # keep='last' 保证如果日期重叠，以最新下载的为准
                    combined_df = pd.concat([old_df, new_df])
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                    combined_df.sort_index(inplace=True)
                    target_df = combined_df
                else:
                    # 覆盖模式 / 文件不存在
                    target_df = new_df

                # --- 涨跌停计算 (在最终 Merged 的 DF 上计算，保证昨收连续性) ---
                # 注意：如果是 Append 模式，最好只重新计算新加部分的涨跌停，
                # 但为了代码简单且防止昨收修正，这里对 target_df 做一次全量计算也很快。
                # 优化点：如果 target_df 很大，这里可以优化。
                
                target_df['prev_close'] = target_df[BarFields.CLOSE].shift(1)
                
                # 获取名称
                instr_detail = xtdata.get_instrument_detail(code)
                stock_name = instr_detail['InstrumentName'] if instr_detail else ""

                # 向量化计算有点难，还是用列表推导
                limit_ups = []
                limit_downs = []
                
                # 这里为了效率，如果数据量 > 5000，可能需要优化。目前日线级别还好。
                # 小技巧：只计算最后 N 行？不，为了数据一致性，建议全算或只算新行。
                # 这里演示全算，保证中间没有断层
                for idx, row in target_df.iterrows():
                    p_close = row['prev_close']
                    if pd.isna(p_close):
                        limit_ups.append(np.nan)
                        limit_downs.append(np.nan)
                    else:
                        u, d = _calculate_limit_price(code, stock_name, p_close)
                        limit_ups.append(u)
                        limit_downs.append(d)
                
                target_df[BarFields.LIMIT_UP] = limit_ups
                target_df[BarFields.LIMIT_DOWN] = limit_downs
                target_df.drop(columns=['prev_close'], inplace=True)

                # --- 落库 ---
                target_df.to_parquet(file_path)
                success_count += 1

            except Exception as e:
                print(f"❌ 处理 {code} 失败: {e}")
                continue
        
        print(f"✅ 批次处理完成，更新了 {success_count} 个文件。")

    # ================= A. 全量更新模式 =================
    def run_full_update(self, start_date: str, end_date: str):
        """
        强制指定日期范围进行全量覆盖
        :param start_date: '20200101'
        :param end_date: '20231231'
        """
        print(f"🚀 [模式: 全量更新] 范围: {start_date} ~ {end_date}")
        stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        # 全量模式直接调用通用方法，模式为 overwrite
        self.fetch_and_update(stock_list, start_date, end_date, mode="overwrite")

    # ================= B. 增量更新模式 =================
    def run_incremental_update(self):
        """
        自动检测每只股票的进度，只下载缺失部分
        """
        print(f"🚀 [模式: 增量更新] 正在检查本地数据状态...")
        stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        
        today = datetime.now()
        today_str = today.strftime('%Y%m%d')
        
        # 待更新列表：存放 (code, start_date_str)
        # 为了避免对每只股票发一次 download 请求 (太慢)，我们将相同开始时间的股票分组
        update_groups = {} # { '20231027': ['000001', '000002'], ... }

        for code in stock_list:
            file_path = self.save_dir / f"{code}.parquet"
            last_dt = self._get_local_last_date(file_path)

            if last_dt is None:
                # 情况1: 新股或本地无文件 -> 默认下载最近365天 (或者你可以设为上市日期)
                start_date = (today - timedelta(days=5)).strftime('%Y%m%d')
            else:
                # 情况2: 有数据 -> 检查是否是最新的
                # 简单判断: 如果 last_dt 是今天(且收盘后)或昨天，可能不需要更新
                # 但为了保险，我们总是尝试请求 last_dt 之后的数据
                
                # 如果 last_dt 就是今天，且现在是盘后，那不需要更新
                # 这里简单处理：请求 last_dt 的下一天
                # 注意：QMT如果请求的 start_time > end_time，不会报错，只会返回空，这很好
                next_day = last_dt + timedelta(days=1)
                if next_day > today:
                    continue # 已经是最新，跳过
                
                start_date = next_day.strftime('%Y%m%d')

            # 加入分组
            if start_date not in update_groups:
                update_groups[start_date] = []
            update_groups[start_date].append(code)

        # 开始分组下载
        if not update_groups:
            print("✨ 所有数据已是最新，无需更新。")
            return

        print(f"📋 检测完毕，将分为 {len(update_groups)} 个时间批次进行更新...")
        
        for start_str, codes in update_groups.items():
            # 过滤一下，如果 start_str 已经超过了 today_str (理论上上面拦截了)，跳过
            if start_str > today_str: continue
            
            print(f"   >> 批次 {start_str} ~ {today_str}: 包含 {len(codes)} 只股票")
            # 增量模式，使用 append
            self.fetch_and_update(codes, start_str, today_str, mode="append")

if __name__ == "__main__":
    loader = QMTDataLoader()
    # # 测试增量
    # loader.run_incremental_update()
    # 测试全量
    loader.run_full_update('201701', '20251205')