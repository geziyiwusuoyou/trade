# config/__init__.py
# -*- coding: utf-8 -*-

from .path_config import (
    PROJECT_ROOT,
    DATA_ROOT,
    MARKET_DATA_DIR,
    BASIC_INFO_DIR,
    STRATEGY_WORKSPACE,
    LOG_DIR
)

from .account_config import (
    QMTConfig,
    TushareConfig
)

# 定义全局常量 (如果有的话)
SYSTEM_NAME = "trade   2.0"
VERSION = "2.0.0"

# 可以在这里做一些简单的环境检查
if not QMTConfig.MINI_QMT_PATH:
    print("⚠️ 警告: 未配置 QMT 路径，实盘功能将不可用")