# -*- coding: utf-8 -*-
"""
Module: base_selector.py
Description: 选股策略基类 - 处理路径管理和结果存储
"""
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from config import STRATEGY_WORKSPACE

class SelectorBase:
    def __init__(self, strategy_name):
        """
        初始化策略
        :param strategy_name: 策略唯一英文名称 (作为文件夹名)
        """
        self.strategy_name = strategy_name
        # 自动定位输出目录: strategy_pool/selectors/pool_storage/{strategy_name}
        self.output_dir = STRATEGY_WORKSPACE / strategy_name
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run(self, date=None):
        """
        子类必须实现此方法
        :param date: 指定运行日期 (datetime 或 str)，默认为当天
        """
        raise NotImplementedError

    def save_result(self, df_result, date_str=None):
        """
        统一保存选股结果
        """
        if df_result is None or df_result.empty:
            print(f"[{self.strategy_name}] 今日无选股结果，跳过保存。")
            return

        if date_str is None:
            date_str = datetime.now().strftime('%Y%m%d')

        # 增加策略名和日期列 (数据标准化)
        df_result['strategy_name'] = self.strategy_name
        df_result['date'] = date_str

        file_path = self.output_dir / f"{date_str}.csv"
        
        # 解决中文乱码，使用 utf-8-sig
        df_result.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        print(f"✅ [{self.strategy_name}] 结果已保存: {file_path}")
        print(f"   入选数量: {len(df_result)}")