# =========================================
# 北向资金 North_Indicator 择时策略（实盘友好 + 撤单重报价）
# 标的：沪深300ETF（510300.XSHG）
# 特点：
#   - 每天只调仓一次（09:40）
#   - 买：按卖一价小幅溢价的限价单
#   - 卖：按买一价小幅折价的限价单
#   - 手续费 / 印花税 / 滑点
#   - 盘口缺失时用最近5分钟 VWAP/last_price 兜底
#   - 挂单超过 2 分钟未成交 → 自动撤单＋更激进价格重下单
# =========================================

from jqdata import *
import pandas as pd
import numpy as np
import datetime


def initialize(context):
    # ---------- 策略参数 ----------
    g.index = "000300.XSHG"          # 基准：沪深300
    g.etf   = "510300.XSHG"          # 交易标的：沪深300ETF

    # North_Indicator 参数（研报：t1=3, t2=15）
    g.t1 = 3
    g.t2 = 15
    g.threshold_p = 0.5              # >0.5 开多
    g.threshold_n = -0.5             # <-0.5 清仓

    # 挂单管理：未成交多少分钟后撤单重报
    g.order_expire_minutes = 2

    # 回测 / 实盘友好设置
    set_benchmark(g.index)
    set_option('use_real_price', True)
    log.set_level('order', 'error')

    # 手续费 + 印花税（接近普通券商）
    set_order_cost(
        OrderCost(open_tax=0,
                  close_tax=0.001,          # 卖出千一印花税
                  open_commission=0.0005,   # 买入万五
                  close_commission=0.0005,  # 卖出万五
                  min_commission=5),
        type='stock'
    )

    # 滑点：0.03%
    set_slippage(PriceRelatedSlippage(0.0003))

    # 预先计算 North_Indicator
    prepare_north_indicator()

    # 每天 09:40 调仓（避免开盘前几分钟噪音）
    run_daily(rebalance, time='09:35')


# =========================================
# 一、预处理：North_Indicator
# =========================================
def prepare_north_indicator():
    start_date = datetime.date(2014, 11, 1)
    end_date = datetime.date.today()

    trade_days = get_trade_days(start_date=start_date, end_date=end_date)
    if len(trade_days) == 0:
        log.error("获取交易日失败")
        return

    q = query(
        finance.STK_ML_QUOTA.day,
        finance.STK_ML_QUOTA.link_id,
        finance.STK_ML_QUOTA.buy_amount,
        finance.STK_ML_QUOTA.sell_amount
    ).filter(
        finance.STK_ML_QUOTA.day >= trade_days[0],
        finance.STK_ML_QUOTA.day <= trade_days[-1],
        finance.STK_ML_QUOTA.link_id.in_([310001, 310002])
    )
    df = finance.run_query(q)
    if df is None or df.shape[0] == 0:
        log.error("没有查询到北向资金数据")
        return

    df["north_flow"] = df["buy_amount"] - df["sell_amount"]
    s = df.groupby("day")["north_flow"].sum().sort_index()
    s = s.reindex(trade_days, method="ffill")
    g.north_flow = s

    flow = s.astype(float)
    ema_short = flow.ewm(span=g.t1).mean()
    ema_long  = flow.ewm(span=g.t2).mean()
    std_long  = flow.rolling(window=g.t2).std()
    indicator = (ema_short - ema_long) / std_long
    g.north_indicator = indicator

    log.info("North_Indicator 样本长度=%d" % len(flow))


def get_indicator_on(date):
    if not hasattr(g, 'north_indicator'):
        return None
    if date not in g.north_indicator.index:
        return None
    val = g.north_indicator.loc[date]
    if np.isnan(val):
        return None
    return float(val)


# =========================================
# 二、价格选取：限价单（盘口优先 + VWAP 兜底）
# =========================================
def get_limit_prices(context, aggressive=False):
    """
    返回 (buy_price, sell_price)

    aggressive=False：正常报价（轻微溢价/折价）
    aggressive=True ：重报价时，更激进一些提高成交概率
    """
    cur = get_current_data()[g.etf]
    last_price = cur.last_price

    # 基础倍率
    if not aggressive:
        buy_mul_ask  = 1.0002
        sell_mul_bid = 0.9998
        buy_mul_vwap = 1.0003
        sell_mul_vwap = 0.9997
    else:
        # 重报价：更狠一点
        buy_mul_ask  = 1.0008
        sell_mul_bid = 0.9992
        buy_mul_vwap = 1.0008
        sell_mul_vwap = 0.9992

    buy_price = None
    sell_price = None

    # ---------- 1）尝试用盘口 ask1 / bid1 ----------
    ask1 = getattr(cur, 'ask1', None)
    bid1 = getattr(cur, 'bid1', None)

    if ask1 and bid1 and ask1 > 0 and bid1 > 0:
        buy_price  = ask1 * buy_mul_ask
        sell_price = bid1 * sell_mul_bid

    # ---------- 2）盘口拿不到，用最近5分钟VWAP ----------
    if buy_price is None or sell_price is None:
        now = context.current_dt
        df = get_price(
            g.etf,
            end_date=now,
            count=5,
            fields=['close', 'volume'],
            frequency='1m',
            panel=False
        )
        if df is not None and not df.empty and df['volume'].sum() > 0:
            vwap = (df['close'] * df['volume']).sum() / df['volume'].sum()
        else:
            vwap = last_price

        if buy_price is None:
            buy_price = vwap * buy_mul_vwap
        if sell_price is None:
            sell_price = vwap * sell_mul_vwap

    # ---------- 3）兜底：last_price ----------
    if buy_price is None:
        buy_price = last_price * (1 + (buy_mul_vwap - 1))
    if sell_price is None:
        sell_price = last_price * sell_mul_vwap

    # 控制在涨跌停范围内
    if cur.high_limit is not None:
        buy_price = min(buy_price, cur.high_limit)
    if cur.low_limit is not None:
        sell_price = max(sell_price, cur.low_limit)

    return buy_price, sell_price


# =========================================
# 三、每日择时 + 调仓（每天一次）
# =========================================
def rebalance(context):
    today = context.current_dt.date()
    ind = get_indicator_on(today)
    if ind is None:
        log.info("今日 North_Indicator 无效，保持不动")
        return

    pos = context.portfolio.positions.get(g.etf, None)
    holding = (pos is not None and pos.total_amount > 0)

    cur = get_current_data()[g.etf]
    if not is_tradable_etf(cur):
        log.info("标的不可交易（停牌/涨停/价格异常），保持不动")
        return

    buy_price, sell_price = get_limit_prices(context, aggressive=False)

    log.info("日期: %s, North_Indicator=%.3f, 持仓=%s, 买价=%.3f, 卖价=%.3f"
             % (today, ind, "有" if holding else "空", buy_price, sell_price))

    # -------- 1）平仓信号 --------
    if ind < g.threshold_n and holding:
        order_target_value(g.etf, 0, style=LimitOrderStyle(sell_price))
        log.info(">> 平仓：限价卖出 %s, 限价 %.3f" % (g.etf, sell_price))
        return

    # -------- 2）开多信号 --------
    if ind > g.threshold_p and not holding:
        cash = context.portfolio.cash
        if cash <= 0:
            log.info("无可用现金，无法开多")
            return
        order_value(g.etf, cash, style=LimitOrderStyle(buy_price))
        log.info(">> 开多：限价买入 %s, 限价 %.3f, 金额 %.2f" %
                 (g.etf, buy_price, cash))
        return

    # -------- 3）中性区间：不动 --------
    log.info("North_Indicator 处于中性区间，保持当前仓位不变")


# =========================================
# 四、挂单管理：超时自动撤单 + 重报价
# =========================================
def manage_open_orders(context):
    """
    每个 bar 调用：
      - 找出所有未成交订单
      - 若挂单时间超过 g.order_expire_minutes 分钟：
            撤单 -> 用更激进的价格重新下单
    """
    open_orders = get_open_orders()
    if not open_orders:
        return

    now = context.current_dt

    # 注意：open_orders 是 {order_id: UserOrder}
    for order_id, o in open_orders.items():
        # 挂单时长（秒）
        elapsed = (now - o.add_time).total_seconds()
        if elapsed < g.order_expire_minutes * 60:
            continue

        # 超时：先撤单
        cancel_order(o)

        security = o.security
        cur = get_current_data()[security]

        # 只处理我们的 ETF，且必须可交易
        if security != g.etf or not is_tradable_etf(cur):
            continue

        # 用更激进的价格重报
        buy_price, sell_price = get_limit_prices(context, aggressive=True)

        if o.is_buy:  # 原订单是买单
            cash = context.portfolio.cash
            if cash > 0:
                order_value(security, cash,
                            style=LimitOrderStyle(buy_price))
                log.info(">> 撤单重报：买单 %s, 新价 %.3f" %
                         (security, buy_price))
        else:         # 原订单是卖单
            order_target_value(security, 0,
                               style=LimitOrderStyle(sell_price))
            log.info(">> 撤单重报：卖单 %s, 新价 %.3f" %
                     (security, sell_price))
                     

def is_tradable_etf(sec_obj):
    """
    ETF 简单可交易过滤：
      - 非停牌
      - 有有效最新价
      - 非涨停
    """
    if sec_obj.paused:
        return False
    if sec_obj.last_price is None or np.isnan(sec_obj.last_price) or sec_obj.last_price <= 0:
        return False
    if sec_obj.high_limit is not None and sec_obj.last_price >= sec_obj.high_limit:
        return False
    return True


def handle_data(context, data):
    # 每分钟检查挂单
    manage_open_orders(context)
    # 记录净值
    record(total_value=context.portfolio.total_value)

