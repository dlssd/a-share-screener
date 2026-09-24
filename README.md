# A股低位多涨停筛选器 V0.4

这是一个个人盘后复盘工具：每天收盘后，从东方财富涨停池中寻找“中大市值、近 20 个交易日多次但不连续涨停、处于 250 日相对低位、两次涨停间有回落”的股票。它不提供实时行情、K 线、新闻、AI 分析、买卖建议或自动交易。

## 数据源与安全原则

- 今日主路径：东方财富沪深京全市场收盘快照，按未复权昨收和各板块制度计算理论涨停价。
- 核验路径：东方财富公开涨停池；历史日期没有可回放快照时，以涨停池为基线并用腾讯逐股检查科创板，明确标记 `WARNING`。
- 交易日历：AKShare 的新浪交易日历。
- 250 日计算：腾讯历史日线（前复权）。
- 日终核验：东方财富涨停池未复权收盘，对比腾讯和新浪未复权日线。
- BaoStock 只保留为实验 provider，不安装也不影响默认运行；如需实验可安装 `requirements-experimental.txt`。
- 完全不需要 Tushare，也不需要 Token 或付费 API。
- 任一必要交易日缺数据、接口失败或核心字段异常时，当日标记 `FAILED`，不会生成“可信的零候选”。旧的已验证数据仍保留。
- `DATA_MISMATCH` 表示东方财富涨停池与腾讯/新浪的目标日、未复权收盘价或涨跌幅明显不一致，需要人工确认。

## Mac 第一次安装

需要 Python 3.9 或更高版本。在终端进入项目目录后执行：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m app.cli init-db
```

无需创建或填写 `TUSHARE_TOKEN`。

## 每天运行

收盘且两个数据源都更新后执行：

```bash
source .venv/bin/activate
python -m app.cli run
```

`run` 会依次完成交易日确认、今日理论涨停审计、市值筛选、腾讯未复权日线重建20日涨停记录、腾讯前复权规则计算和新浪核验。北京时间18:00前运行时，当天不会作为正式盘后交易日。重复运行不会产生重复行。也可以使用等价命令：

```bash
python -m app.cli sync-latest
```

启动网页：

```bash
uvicorn app.web:app --reload
```

浏览器打开 [http://127.0.0.1:8000](http://127.0.0.1:8000)。网页顶部会显示 `SUCCESS / WARNING / FAILED`、抓取时间和两个数据源状态；`FAILED` 时请查看紧随其后的错误原因，不要把空表理解为没有候选。

查看命令行状态：

```bash
python -m app.cli status
```

## 修改参数

复制示例配置并编辑：

```bash
cp .env.example .env
```

默认规则为：市值至少 80 亿元、近 20 个交易日至少 2 次涨停、不能连续涨停、250 日位置不高于 30%、前次涨停后回撤 3%～25%、近 20 日累计涨幅不高于 40%。市值在内部统一使用人民币“元”，网页才转换为“亿元”。历史位置、回撤和 20 日涨幅统一使用腾讯前复权序列；当日价格使用未复权数据核验，代码中分别命名为 `adjusted_close` 和 `raw_close`。

历史涨停日期由腾讯未复权日K按理论涨停价重建，不依赖东方财富历史涨停池。主板按10%、创业板/科创板按20%、北交所按30%，价格按0.01元和 `ROUND_HALF_UP` 计算。默认排除ST；未知规则和新股历史不足分别标记 `UNKNOWN_LIMIT_RULE`、`INSUFFICIENT_HISTORY`。

审计一个完整交易日：

```bash
python -m app.cli audit --date 2026-09-24
```

输出全市场数量、理论涨停、东方财富池交集和差异、各板块涨停数量及ST排除数量。

## 数据源探测

如 AKShare 升级后怀疑字段变化，可运行：

```bash
python scripts/probe_sources.py 20260924
```

脚本会打印东方财富涨停池的真实列名、行数、前三行和字段单位。日期请换成最近一个完整交易日。BaoStock 探测仅在安装实验依赖时使用，不影响默认流程。
