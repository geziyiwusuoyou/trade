# -*- coding: utf-8 -*-
"""
Module: adapter_qmt.py
Description: QMT 数据源适配器 - 负责下载、清洗、计算涨跌停、并落地为标准 Parquet
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

# 引入 QMT SDK
from xtquant import xtdata

# 引入项目配置和标准定义
from config import MARKET_DATA_DIR, QMTConfig
from common.data_structs import BarFields

# ================= 1. 核心算法：A股涨跌停价格计算 =================

def _round_to_2_decimals(number):
    """
    A股价格专用四舍五入：
    Python的 round() 是"银行家舍入"(偶数舍入)，不符合A股规则。
    A股规则是标准的"四舍五入"保留两位小数。
    """
    # 必须转为字符串再转 Decimal，否则浮点数精度会干扰
    d = Decimal(str(number))
    # ROUND_HALF_UP 就是标准的四舍五入
    return float(d.quantize(Decimal("0.00"), rounding=ROUND_HALF_UP))

def _calculate_limit_price(code: str, name: str, prev_close: float):
    """
    计算涨跌停价格
    :param code: 股票代码 (e.g. 000001.SZ)
    :param name: 股票名称 (用于判断 ST)
    :param prev_close: 昨收价
    :return: (limit_up, limit_down)
    """
    if prev_close is None or np.isnan(prev_close):
        return np.nan, np.nan

    # 1. 确定涨跌幅限制比例
    limit_ratio = 0.10  # 默认主板 10%

    # 科创板(688) / 创业板(300) - 20% (注意：创业板20%是2020年8月后，这里简化处理为当前规则)
    # 北交所(8xx, 4xx) - 30%
    if code.startswith('688') or code.startswith('30'):
        limit_ratio = 0.20
    elif code.startswith('8') or code.startswith('4'):
        limit_ratio = 0.30
    # ST 股票 - 5% (名称包含 ST 或 *ST)
    elif 'ST' in name:
        limit_ratio = 0.05
    
    # 2. 计算并取整
    # 公式：昨收 * (1 + 比例) -> 四舍五入到分
    up_price = _round_to_2_decimals(prev_close * (1 + limit_ratio))
    down_price = _round_to_2_decimals(prev_close * (1 - limit_ratio))

    return up_price, down_price

# ================= 2. 主逻辑：ETL 流程 =================

class QMTDataLoader:
    def __init__(self):
        # 确定存储路径: data_center/storage/market_data/stock_daily
        self.save_dir = MARKET_DATA_DIR / "stock_daily"
        if not self.save_dir.exists():
            self.save_dir.mkdir(parents=True, exist_ok=True)
            print(f"📁 创建存储目录: {self.save_dir}")

    def run_etl(self, lookback_days=30):
        """
        执行数据同步任务
        """
        print(f"🚀 [QMT Adapter] 启动数据更新... (回溯 {lookback_days} 天)")
        
        # 1. 时间范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')

        # 2. 获取全A股列表
        # format: ['000001.SZ', '600000.SH', ...]
        stock_list = xtdata.get_stock_list_in_sector('沪深A股')
        print(f"📋 目标股票数量: {len(stock_list)}")

        # 3. 触发下载 (QMT 只有先 download 才能 get)
        print("📥 开始请求 QMT 下载历史数据...")
        for i, code in enumerate(stock_list):
            xtdata.download_history_data(code, period='1d', start_time=start_str, end_time=end_str)
            if (i + 1) % 1000 == 0:
                print(f"   已下载: {i + 1}/{len(stock_list)}")
        
        # 4. 批量读取与处理
        print("🔄 开始清洗与计算衍生指标...")
        
        # 定义我们需要从 QMT 获取的原始字段
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

        for code, df in data_dict.items():
            if df.empty or len(df) < 2:
                continue

            try:
                # --- A. 基础格式清洗 ---
                # QMT time 是毫秒时间戳，转为 datetime 并调整时区 (QMT返回的是UTC时间戳，A股需+8)
                df['time'] = pd.to_datetime(df['time'], unit='ms') + pd.Timedelta(hours=8)
                
                # 重命名为标准字段 (BarFields)
                df.rename(columns={
                    'time': BarFields.DATE_TIME,
                    'open': BarFields.OPEN,
                    'high': BarFields.HIGH,
                    'low': BarFields.LOW,
                    'close': BarFields.CLOSE,
                    'volume': BarFields.VOLUME,
                    'amount': BarFields.AMOUNT
                }, inplace=True)

                # 设置索引
                df.set_index(BarFields.DATE_TIME, inplace=True)
                df.sort_index(inplace=True)

                # 增加代码列
                df[BarFields.CODE] = code
                
                # 默认复权因子为 1.0 (暂时不用)
                df[BarFields.ADJ_FACTOR] = 1.0

                # --- B. 计算涨跌停 (Precision Logic) ---
                # 1. 获取昨收 (shift 1)
                df['prev_close'] = df[BarFields.CLOSE].shift(1)

                # 2. 获取名称 (用于判断 ST)
                # 注意: 这里循环调用 get_instrument_detail 可能稍微有点慢，但为了准确性是值得的
                # 如果追求极致速度，可以在循环外先获取所有 info
                instr_detail = xtdata.get_instrument_detail(code)
                stock_name = instr_detail['InstrumentName'] if instr_detail else ""

                # 3. 逐行计算涨跌停
                # 为了性能，我们将核心逻辑封装，这里使用 iterrows 或 apply 
                # (考虑到每天只有一条数据，且逻辑依赖上一行，向量化比较复杂，这里用列表推导式处理)
                
                limit_ups = []
                limit_downs = []
                
                for idx, row in df.iterrows():
                    p_close = row['prev_close']
                    if pd.isna(p_close):
                        # 第一天数据没有昨收，没法算涨跌停，填空
                        limit_ups.append(np.nan)
                        limit_downs.append(np.nan)
                    else:
                        u, d = _calculate_limit_price(code, stock_name, p_close)
                        limit_ups.append(u)
                        limit_downs.append(d)
                
                df[BarFields.LIMIT_UP] = limit_ups
                df[BarFields.LIMIT_DOWN] = limit_downs

                # 删除中间变量
                df.drop(columns=['prev_close'], inplace=True)

                # --- C. 落地存储 ---
                # 路径: data_center/storage/market_data/stock_daily/000001.SZ.parquet
                file_path = self.save_dir / f"{code}.parquet"
                
                # 如果是增量更新，其实应该先读取旧文件 merge，这里简化为覆盖模式(因为你每次拉30天)
                # 生产环境建议：读取旧 Parquet -> concat -> drop_duplicates -> save
                df.to_parquet(file_path)
                
                success_count += 1

            except Exception as e:
                print(f"❌ 处理 {code} 失败: {e}")
                continue

        print(f"✅ [QMT Adapter] 任务完成！成功落地 {success_count} 只股票数据。")
        print(f"📂 数据位置: {self.save_dir}")

# ================= 3. 脚本入口 =================

if __name__ == "__main__":
    # 如果你想测试，可以在这里运行
    loader = QMTDataLoader()
    loader.run_etl(lookback_days=10)