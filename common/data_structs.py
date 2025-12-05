# common/data_structs.py
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

# ================= 1. 标准字段名定义 (Constants) =================
# 在整个项目中，不要直接使用字符串 'close', 'open'，尽量引用这些常量
# 这样如果有一天你想把 'vol' 改成 'volume'，只需要改这里一处

class BarFields:
    """
    K线数据标准列名定义
    """
    DATE_TIME = "datetime"      # 时间索引 (pd.Timestamp)
    CODE = "code"               # 标的代码 (str, e.g., "000001.SZ")
    
    OPEN = "open"               # 开盘价 (float)
    HIGH = "high"               # 最高价 (float)
    LOW = "low"                 # 最低价 (float)
    CLOSE = "close"             # 收盘价 (float)
    
    VOLUME = "volume"           # 成交量 (float/int)
    AMOUNT = "amount"           # 成交额 (float)
    
    # 涨跌停价格 (用于实盘风控和回测撮合限制)
    LIMIT_UP = "limit_up"       # 涨停价
    LIMIT_DOWN = "limit_down"   # 跌停价
    
    # 复权相关 (非常关键)
    ADJ_FACTOR = "adj_factor"   # 复权因子 (后复权/前复权基准)
    
    # 可选字段
    VWAP = "vwap"               # 成交量加权平均价
    TURN = "turnover_rate"      # 换手率

# ================= 2. 数据结构对象 (Class) =================
# 主要用于事件驱动回测或实盘中，传递单根K线数据

@dataclass
class BarData:
    """
    单根K线数据类 (用于 Tick 级循环或事件驱动)
    """
    code: str                   # 代码
    datetime: datetime          # 时间
    
    open_price: float = 0.0
    high_price: float = 0.0
    low_price: float = 0.0
    close_price: float = 0.0
    
    volume: float = 0.0         # 成交量
    amount: float = 0.0         # 成交额
    
    limit_up: float = 0.0       # 涨停价
    limit_down: float = 0.0     # 跌停价

    # 预留扩展字典 (比如存 funding_rate, open_interest 等)
    extra: dict = None

    def __post_init__(self):
        if self.extra is None:
            self.extra = {}

# ================= 3. 交易相关枚举 (Enums) =================
# 提前定义好，防止后面写错字符串

class OrderType(Enum):
    LIMIT = "LIMIT"             # 限价单
    MARKET = "MARKET"           # 市价单

class Direction(Enum):
    LONG = "LONG"               # 做多 (买入)
    SHORT = "SHORT"             # 做空 (卖出)
    NET = "NET"                 # 净仓模式 (A股通常用这个或 Long)

class Offset(Enum):
    OPEN = "OPEN"               # 开仓
    CLOSE = "CLOSE"             # 平仓
    CLOSE_TODAY = "CLOSETODAY"  # 平今 (期货用)
    NONE = "NONE"               # 股票普通买卖不用区分