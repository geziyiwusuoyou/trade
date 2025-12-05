# -*- coding: utf-8 -*-
"""
Module: n_pattern_policy.py
Description: N字反包选股策略 (Pro版) - 适配 QuantProject 2.0
"""
import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# 引入项目组件
from config import MARKET_DATA_DIR, BASIC_INFO_DIR
from common.data_structs import BarFields
from strategy_pool.selectors.policy.base_selector import SelectorBase

class NPatternSelector(SelectorBase):
    def __init__(self):
        # 策略名，对应 pool_storage/n_pattern_rebound 文件夹
        super().__init__(strategy_name="n_pattern_rebound")
        
        # 基础信息表路径
        self.stock_info_path = BASIC_INFO_DIR / "stock.csv"

    def load_stock_metadata(self):
        """
        加载 stock.csv (复用你的逻辑)
        """
        if not self.stock_info_path.exists():
            print(f"❌ [Error] 找不到 {self.stock_info_path}")
            return set(), pd.DataFrame()

        try:
            # 尝试读取
            try:
                stock_info = pd.read_csv(self.stock_info_path, encoding='utf-8')
            except UnicodeDecodeError:
                stock_info = pd.read_csv(self.stock_info_path, encoding='gbk')

            # 1. 提取6位代码
            # 假设 CSV 里代码列名叫 'order_book_id' 或 'code'，这里做个兼容处理
            code_col = 'order_book_id' if 'order_book_id' in stock_info.columns else 'code'
            if code_col not in stock_info.columns:
                print("⚠️ stock.csv 缺少代码列")
                return set(), pd.DataFrame()
                
            stock_info['code_key'] = stock_info[code_col].astype(str).str[:6]

            # 2. 筛选 Normal (如果有状态列)
            if 'special_type' in stock_info.columns:
                normal_df = stock_info[stock_info['special_type'] == 'Normal']
            else:
                normal_df = stock_info

            # 3. 提取需要的字段
            cols_to_keep = ['code_key', 'symbol', 'sector_code_name', 'industry_name']
            cols_to_keep = [c for c in cols_to_keep if c in stock_info.columns]
            
            info_df = stock_info[cols_to_keep].copy()
            valid_codes = set(normal_df['code_key'].values)
            
            return valid_codes, info_df

        except Exception as e:
            print(f"❌ 读取 stock.csv 失败: {e}")
            return set(), pd.DataFrame()

    def run(self, date=None):
        print(f">>> [Strategy] 启动 {self.strategy_name} ...")
        
        # 1. 加载白名单
        valid_whitelist, stock_info_df = self.load_stock_metadata()
        
        # 2. 扫描 Parquet 文件
        stock_dir = MARKET_DATA_DIR / "stock_daily"
        files = list(stock_dir.glob("*.parquet"))
        print(f"📋 扫描行情文件数: {len(files)}")

        selected_pool = []

        # 3. 遍历计算
        for file_path in files:
            # 解析代码: 000001.SZ.parquet -> 000001
            # 注意: 如果文件名是 000001.SZ.parquet，file_path.stem 是 000001.SZ
            file_stem = file_path.stem 
            code_key = file_stem[:6]  # 取前6位纯数字

            # [过滤 1] 板块过滤 (主板 + 创业板)
            if not re.match(r'^(60|00|30)', code_key):
                continue

            # [过滤 2] ST / 停牌过滤 (白名单)
            if valid_whitelist and code_key not in valid_whitelist:
                continue

            try:
                # 读取 Parquet
                df = pd.read_parquet(file_path)
                if len(df) < 5: continue  # 数据太少

                # 确保按时间排序
                df.sort_index(inplace=True)

                # 取最后几行数据
                # 你的逻辑是用 iloc[-1] 代表最新一天。
                # 如果是盘后跑，就是今天收盘数据。
                curr = df.iloc[-1]
                prev1 = df.iloc[-2]
                prev2 = df.iloc[-3]
                prev3 = df.iloc[-4]

                # 提取关键字段 (使用标准常量)
                # limit_up_flag: True/False
                is_curr_limit = curr.get(BarFields.LIMIT_UP) == curr.get(BarFields.CLOSE) 
                # 如果你在 Adapter 里已经生成了 'limit_up_flag' 列更好，如果没有，现场算一下:
                # 你的Adapter里没有显示生成 limit_up_flag 列，而是生成了 limit_up 价格
                # 所以这里我们要动态判断: close == limit_up
                
                def is_limit(row):
                    # 容错处理：考虑到浮点数精度，用 isclose 或者 差值小于 0.01
                    return abs(row[BarFields.CLOSE] - row[BarFields.LIMIT_UP]) < 0.01

                curr_limit = is_limit(curr)
                prev1_limit = is_limit(prev1)
                prev2_limit = is_limit(prev2)
                prev3_limit = is_limit(prev3)

                reason = None
                
                # 获取近10天的涨停情况，用于排除妖股
                # 既然要算 sum，我们需要构造一个 Series
                # 这里为了性能，只取最后10行算一下
                last_10 = df.iloc[-10:]
                limit_counts = last_10.apply(is_limit, axis=1) # Boolean Series

                # === 策略逻辑复刻 ===
                
                # 模式 A: 1板1调 (昨天板，今天断板且不破板开)
                if prev1_limit and not curr_limit:
                    # 排除妖股: 过去[倒数第8天 到 倒数第2天] 涨停数 < 2
                    if limit_counts.iloc[-8:-2].sum() < 2:
                        if curr[BarFields.CLOSE] >= prev1[BarFields.OPEN]:
                            reason = "1板1调"

                # 模式 B: 1板2调 (前天板，昨今断，不破板开)
                elif prev2_limit and not prev1_limit and not curr_limit:
                    if limit_counts.iloc[-9:-3].sum() < 2:
                        if curr[BarFields.CLOSE] >= prev2[BarFields.OPEN]:
                            reason = "1板2调"

                # 模式 C: 1板3调
                elif prev3_limit and not prev2_limit and not prev1_limit and not curr_limit:
                    if limit_counts.iloc[-10:-4].sum() < 2:
                        if curr[BarFields.CLOSE] >= prev3[BarFields.OPEN]:
                            reason = "1板3调"

                if reason:
                    # 计算量比 (和过去5日均量相比)
                    # volume 是 float
                    vol_ma5 = df[BarFields.VOLUME].iloc[-6:-1].mean()
                    vol_ratio = round(curr[BarFields.VOLUME] / vol_ma5, 2) if vol_ma5 > 0 else 0

                    selected_pool.append({
                        'code_key': code_key,
                        'code': curr[BarFields.CODE], # 带后缀的代码
                        'Close': curr[BarFields.CLOSE],
                        'Vol_Ratio': vol_ratio,
                        'Pattern': reason,
                        'select_time': curr.name.strftime('%Y-%m-%d') # 取那一行的 Index 时间
                    })

            except Exception as e:
                # print(f"Error: {code_key} - {e}")
                continue

        # 4. 合并与保存
        if selected_pool:
            res_df = pd.DataFrame(selected_pool)
            
            # 合并行业信息
            if not stock_info_df.empty:
                final_df = pd.merge(res_df, stock_info_df, on='code_key', how='left')
            else:
                final_df = res_df

            # 排序
            if 'industry_name' in final_df.columns:
                final_df.sort_values(by=['industry_name', 'Pattern'], inplace=True)
            
            # 5. 调用父类方法保存
            # date 参数使用最后一天数据的日期
            self.save_result(final_df)
        else:
            print(f"[{self.strategy_name}] ⚠️ 今日无符合条件股票")

# 调试用
if __name__ == "__main__":
    s = NPatternSelector()
    s.run()