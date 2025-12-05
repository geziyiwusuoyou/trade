<<<<<<< HEAD
# QuantProject 2.0

量化交易系统
=======
# trade - 本地量化交易系统

## 1. 项目简介

`trade` 是一个基于 **Python + MiniQMT** 的轻量级、本地化量化交易系统。
 系统采用清晰的 **“数据 - 策略 - 执行”** 三层解耦架构，面向具备一定开发能力的量化交易者，重点解决：

- 本地复盘与回测数据的可控性
- 策略模块的可插拔与可复用
- 实盘执行与 QMT 终端之间的稳定衔接

### 核心特性

- **本地化数据仓库**：
   使用 Parquet 文件存储全量历史行情数据，摆脱对交易终端实时在线的强依赖，更利于回测与研究。
- **策略模块解耦**：
   将策略划分为两个相互独立、可自由组合的部分：
  - **Selector（选股）**：生成每日股票池
  - **Timer（择时）**：盘中基于 Tick 信号做交易决策
- **混合驱动模式**：
   同时支持：
  - 盘后批量选股（Batch）
  - 盘中事件驱动交易（Event-Driven）
- **QMT 适配层**：
   对 `xtquant` 进行二次封装，统一下单与行情接口，为 MiniQMT 提供稳定的实盘通道。

------

## 2. 目录结构说明

| 目录/文件           | 说明                                                         |
| ------------------- | ------------------------------------------------------------ |
| **`config/`**       | **配置中心**。通过 `settings.py` 配置 MiniQMT 路径、资金账号 ID、本地数据存储路径等全局参数。 |
| **`data/`**         | **数据仓库**。用于存放所有落地后的数据文件（如 Parquet 行情数据）。通常不建议纳入 Git 版本控制。 |
| **`data_factory/`** | **数据工厂（ETL）**。负责从 QMT / Tushare 等数据源拉取、清洗并落地数据；包含统一的 `storage` 读写封装。 |
| **`strategy_lab/`** | **策略实验室**。 - `selectors/`：选股逻辑模块，输出每日股票池。 - `timers/`：择时逻辑模块，基于 Tick 生成买卖信号。 - `targets/`：存放每日生成的目标池 CSV 文件。 |
| **`execution/`**    | **执行引擎**。负责连接 MiniQMT，加载 `targets` 中的目标股票池，并结合 `timers` 的信号执行买卖。 |
| **`scripts/`**      | **运行脚本入口**。包含数据同步、盘后选股、盘中实盘交易等主要脚本。 |

------

## 3. 标准工作流（Workflow）

系统被设计为三个相互独立、可通过定时任务（Crontab / Task Scheduler）自动串联的阶段。

### 阶段一：盘后数据同步（Day End）

- **建议时间**：每日 16:00 左右

- **执行命令**：

  ```bash
  python scripts/run_data_sync.py
  ```

- **主要行为**：

  1. 连接 QMT，获取当日最新行情（如日线 / 分钟线）。
  2. 将增量数据追加写入 `data/market_data/` 目录下的 Parquet 文件。
  3. 保持本地数据仓库与市场数据日常同步。

------

### 阶段二：策略选股（Night Analysis）

- **建议时间**：每日 17:00，或盘前 08:00

- **执行命令**：

  ```bash
  python scripts/run_selector.py
  ```

- **主要行为**：

  1. 读取本地历史行情及相关因子数据。
  2. 运行指定选股策略（例如：低估值 + 动量等组合逻辑）。
  3. 生成当日目标池文件：
      `strategy_lab/targets/target_YYYYMMDD.csv`。

------

### 阶段三：盘中实盘交易（Intraday Trading）

- **时间范围**：交易日 09:15 - 15:00

- **执行命令**：

  ```bash
  python scripts/run_execution.py
  ```

- **主要行为**：

  1. 启动并登录 MiniQMT 客户端（需提前完成，建议勾选“极简模式”）。
  2. 加载阶段二生成的 `target_YYYYMMDD.csv` 目标池。
  3. 订阅目标股票的实时 Tick 行情。
  4. 根据 Timer（择时模块）实时计算买卖信号，并通过 QMT 通道下单。

------

## 4. 环境依赖（Requirements）

- **Python**：3.8 及以上版本
- **核心依赖库**：
  - `pandas`：数据处理与分析
  - `pyarrow` / `fastparquet`：Parquet 文件读写
  - `xtquant`：QMT 官方 SDK（需手动安装或引用本地包）
  - `tushare`（可选）：补充财务数据、因子数据等

------

## 5. 快速开始（Quick Start）

1. **配置环境**

   在 `config/settings.py` 中配置以下内容：

   - MiniQMT 安装路径：`userdata_mini`
   - 资金账号及对应交易服务器信息
   - 本地数据存储目录

2. **初始化数据**

   首次使用时可先拉取一段历史数据用于测试与回测，例如最近 30 个交易日：

   ```bash
   # 下载最近 30 天的全市场行情数据
   python scripts/run_data_sync.py --days 30
   ```

3. **开发与接入策略**

   - 在 `strategy_lab/selectors/` 中新建 `my_strategy.py`，继承 `BaseSelector`，实现自定义选股逻辑。
   - 在 `strategy_lab/timers/` 中新建 `my_timer.py`，继承 `BaseTimer`，实现自定义择时逻辑（如信号生成与仓位管理）。

4. **启动实盘执行**

   在确保 MiniQMT 已启动并登录后，运行实盘执行脚本：

   ```bash
   python scripts/run_execution.py
   ```

------

## 6. 注意事项

- **MiniQMT 依赖**
   启动 `execution` 模块前，务必先打开并登录 MiniQMT 客户端，且建议保持“极简模式”开启，以降低干扰与资源占用。
- **数据存储空间**
   请预留充足的磁盘空间用于存放 Parquet 数据：
  - 全市场日线数据体积较小，一般压力不大；
  - 分钟级 / Tick 级数据体积会显著增大，需提前规划。
- **风控设置**
   系统默认风控逻辑位于 `execution/risk_ctrl.py` 中，包括：
  - 单笔最大下单金额限制
  - 单票持仓上限等
     在接入真实资金之前，务必根据自身风险偏好仔细检查和修改相关配置。
>>>>>>> 312b418d31d9595264c2c31cc190fa0c0c4aec4c
