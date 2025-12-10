# debug_finance.py
from xtquant import xtdata
import pandas as pd

# 测试代码
code = '000001.SZ'
print(f"🔍 正在诊断 {code} 的财务数据 status...")

# 1. 强制下载这一只
print("1. 尝试下载...")
xtdata.download_financial_data2([code], table_list=['Balance', 'Income', 'CashFlow'])
print("   下载指令已发送")

# 2. 原始读取 (不加任何筛选)
print("2. 尝试读取原始数据 (不带时间参数)...")
data = xtdata.get_financial_data(
    stock_list=[code],
    table_list=['Balance'], # 先只读一张表，排除合并问题
    start_time='', 
    end_time='', 
    report_type='announce_time'
)

print(f"3. 返回数据类型: {type(data)}")
if not data:
    print("❌ 错误: 返回为空字典! QMT 本地没有数据，或者路径配置错误。")
    print(f"   当前数据路径: {xtdata.data_dir}")
else:
    df = data.get(code, {}).get('Balance')
    if df is None or df.empty:
        print("❌ 错误: 字典里有 keys，但这只股票的 DataFrame 是空的!")
    else:
        print(f"✅ 成功! 读到 {len(df)} 行数据。")
        print("   前5行索引 (看看是不是日期):")
        print(df.index[:5])
        print("   前5列:")
        print(df.columns[:5])
        print(df)