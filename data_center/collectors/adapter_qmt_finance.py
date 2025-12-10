# -*- coding: utf-8 -*-
"""
Module: adapter_qmt_finance.py
Description: QMT 财务数据适配器 (修复版)
修复核心BUG: QMT返回的DF索引是RangeIndex，需手动将 m_anntime 列转为索引
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import time

# QMT SDK
from xtquant import xtdata

# 配置
from config import DATA_ROOT

class QMTFinanceLoader:
    def __init__(self):
        # 存储路径
        self.save_dir = DATA_ROOT / "financial_data"
        if not self.save_dir.exists():
            self.save_dir.mkdir(parents=True, exist_ok=True)
        
        self.tables = ['Balance', 'Income', 'CashFlow']
        self.DOWNLOAD_BATCH_SIZE = 50 

    def _get_local_last_ann_date(self, code):
        """获取本地 Parquet 的最新公告日"""
        file_path = self.save_dir / f"{code}.parquet"
        if not file_path.exists():
            return None
        try:
            # 仅读取索引
            df = pd.read_parquet(file_path, columns=[]) 
            if df.empty: return None
            return df.index.max()
        except:
            return None

    def fetch_and_update(self, stock_list, start_date=None, end_date=None, mode="append"):
        total_stocks = len(stock_list)
        print(f"💰 [Finance] 启动修复版更新 | 目标: {total_stocks} 只")
        
        # 目标结束时间
        target_end_str = end_date if end_date else datetime.now().strftime('%Y%m%d')
        target_end_dt = pd.to_datetime(target_end_str)

        # --- 1. 智能筛选：哪些需要下载 ---
        print("🔍 [阶段一] 扫描本地文件，计算需下载列表...")
        stocks_to_download = []
        
        for code in stock_list:
            last_dt = self._get_local_last_ann_date(code)
            
            # 判读逻辑
            if mode == "overwrite":
                stocks_to_download.append(code)
            elif last_dt is None:
                stocks_to_download.append(code)
            elif last_dt < (target_end_dt - timedelta(days=5)): 
                stocks_to_download.append(code)
            else:
                pass # 跳过

        print(f"📋 需下载/更新: {len(stocks_to_download)} (跳过 {total_stocks - len(stocks_to_download)})")

        # --- 2. 执行下载 (如果需要) ---
        if stocks_to_download:
            dl_start = start_date
            if not dl_start:
                # 默认最近 3 年 (增量模式)
                dl_start = (datetime.now() - timedelta(days=365*3)).strftime('%Y%m%d')
            
            print(f"⬇️ [阶段二] 开始下载 {len(stocks_to_download)} 只股票 (范围: {dl_start}~{target_end_str})...")
            
            # 分批下载
            for i in range(0, len(stocks_to_download), self.DOWNLOAD_BATCH_SIZE):
                batch = stocks_to_download[i : i + self.DOWNLOAD_BATCH_SIZE]
                try:
                    xtdata.download_financial_data2(
                        stock_list=batch,
                        table_list=self.tables,
                        start_time=dl_start,
                        end_time=target_end_str,
                        callback=lambda x: None
                    )
                except Exception as e:
                    print(f"   ⚠️ 下载异常: {e}")
                
                if (i + 1) % 500 == 0:
                    print(f"   ...已发送下载请求 {i + 1}/{len(stocks_to_download)}")
            
            print("✅ 下载指令发送完成，等待后台同步...")
            # 稍微给点时间让QMT缓存写入磁盘，防止马上读读不到
            time.sleep(2) 
        else:
            print("✅ 本地数据已是最新，跳过下载")

        # --- 3. 处理与落库 (核心修复部分) ---
        print("🔄 [阶段三] 开始清洗与落库...")
        
        # 只要在这个列表里的，或者是全量模式，都重新处理一遍落库
        target_list = stocks_to_download if mode == "append" else stock_list
        success_count = 0
        
        for i, code in enumerate(target_list):
            try:
                # A. 读取原始数据
                data_map = xtdata.get_financial_data(
                    stock_list=[code], 
                    table_list=self.tables, 
                    start_time='', end_time='', 
                    report_type='announce_time'
                )
                
                dfs_to_merge = []
                
                # B. 逐表清洗索引 (CRITICAL FIX)
                for tbl in self.tables:
                    df = data_map.get(tbl)
                    if df is None or df.empty: continue
                    
                    # === 核心修复逻辑 ===
                    # 1. 检查是否存在 'm_anntime' 列 (公告日)
                    if 'm_anntime' not in df.columns:
                        continue
                        
                    # 2. 清洗公告日 (转字符串 -> 转datetime)
                    # 处理 NaN 或 0
                    df = df[df['m_anntime'].notna()] 
                    df = df[df['m_anntime'] != 0]
                    
                    if df.empty: continue
                    
                    # 3. 设置索引
                    # copy() 防止 SettingWithCopyWarning
                    df_clean = df.copy()
                    df_clean['ann_date'] = pd.to_datetime(df_clean['m_anntime'].astype(str), format='%Y%m%d', errors='coerce')
                    
                    # 删除转换失败的日期 (NaT)
                    df_clean = df_clean.dropna(subset=['ann_date'])
                    
                    # 设为 Index
                    df_clean.set_index('ann_date', inplace=True)
                    df_clean.sort_index(inplace=True)
                    
                    # 4. 给列名加后缀 (防止 duplicate columns error)
                    # m_timetag 是共有的，可以保留一个或者都加后缀
                    # 这里选择加后缀，方便分辨
                    df_clean.columns = [f"{col}_{tbl}" if col != 'm_timetag' else col for col in df_clean.columns]
                    
                    dfs_to_merge.append(df_clean)

                if not dfs_to_merge:
                    continue

                # C. 合并 (Outer Join)
                # 使用 concat axis=1 进行外连接合并
                # 遇到重复的索引(同一天发了多次报表?) -> drop_duplicates
                
                # 先去重每个 DF 的索引 (理论上同一天不该有两条，除非修正)
                dfs_unique = []
                for d in dfs_to_merge:
                    dfs_unique.append(d[~d.index.duplicated(keep='last')])

                merged_df = pd.concat(dfs_unique, axis=1, join='outer')
                
                # 合并后可能会有多个 m_timetag 列 (m_timetag, m_timetag, ...)
                # 我们可以做一个整理，或者暂时保留

                # D. 时间过滤
                if start_date:
                    merged_df = merged_df[merged_df.index >= pd.Timestamp(start_date)]
                
                if merged_df.empty:
                    continue

                # E. 存储
                file_path = self.save_dir / f"{code}.parquet"
                
                if mode == "append" and file_path.exists():
                    try:
                        old_df = pd.read_parquet(file_path)
                        final_df = pd.concat([old_df, merged_df])
                        # 去重：按索引(公告日)，保留最新的
                        final_df = final_df[~final_df.index.duplicated(keep='last')]
                        final_df.sort_index(inplace=True)
                    except:
                        final_df = merged_df
                else:
                    final_df = merged_df

                final_df.to_parquet(file_path)
                success_count += 1

            except Exception as e:
                # print(f"❌ {code} 异常: {e}")
                continue
            
            if (i + 1) % 200 == 0:
                print(f"   已处理: {i + 1}/{len(target_list)} | 成功: {success_count}")

        print(f"\n✅ [Finance] 任务结束")
        print(f"   尝试处理: {len(target_list)}")
        print(f"   成功落库: {success_count}")

    # ================= 接口 =================
    def run_full_update(self, start_date, end_date):
        stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        self.fetch_and_update(stock_list, start_date, end_date, mode="overwrite")

    def run_incremental_update(self):
        stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        self.fetch_and_update(stock_list, mode="append")

if __name__ == "__main__":
    loader = QMTFinanceLoader()
    # 调试：只跑20个
    print(">>> Debug模式：测试前20个股票")
    test_list = xtdata.get_stock_list_in_sector('沪深A股')[:20]
    # 强制覆盖模式，确保能看到写入
    loader.fetch_and_update(test_list, start_date='20100101', mode="overwrite")