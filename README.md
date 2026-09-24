# A股低位多涨停筛选器 V0.4

这是一个个人盘后复盘工具：每天收盘后，寻找“中大市值、近20个交易日多次但不连续涨停、处于250日相对低位、两次涨停间有回落”的股票。它不提供实时交易、K线、新闻、AI分析、买卖建议或自动交易。

## 数据源与安全原则

- 今日主路径：东方财富沪深京全市场收盘快照，按未复权昨收和各板块制度计算理论涨停价。
- 核验路径：东方财富公开涨停池；历史日期没有可回放快照时，以涨停池为基线并用腾讯逐股检查科创板，明确标记 `WARNING`。
- 交易日历：AKShare 的新浪交易日历。
- 历史涨停：腾讯未复权日线重建，并用新浪未复权日线核验。
- 250日位置、距离低点、回撤和20日涨幅：腾讯前复权序列。
- BaoStock 仅保留为实验 provider；默认安装和运行不需要它。
- 完全不需要 Tushare、Token 或付费 API。
- 数据异常时显示 `WARNING` / `FAILED`，不会把接口失败显示为可信的零候选。

## Mac 第一次安装

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m app.cli init-db
```

## 每天运行

北京时间18:00后执行：

```bash
source .venv/bin/activate
python -m app.cli run
```

18:00前运行会自动使用前一个完整交易日。`run` 会完成交易日确认、今日理论涨停审计、市值筛选、腾讯未复权日线重建20日涨停记录、腾讯前复权规则计算和新浪核验。重复运行不会产生重复数据。

启动网页：

```bash
uvicorn app.web:app --reload
```

浏览器打开 [http://127.0.0.1:8000](http://127.0.0.1:8000)。

查看状态：

```bash
python -m app.cli status
```

审计一个完整交易日：

```bash
python -m app.cli audit --date 2026-09-24
```

审计输出全市场数量、理论涨停、东方财富池交集和差异、各板块涨停数量及ST排除数量。

## 涨停与复权口径

- 主板10%、创业板/科创板20%、北交所30%。
- 理论涨停价使用0.01元价格单位和 `ROUND_HALF_UP`。
- 默认 `EXCLUDE_ST=true`，ST不进入候选。
- 未知代码规则或特殊状态标记 `UNKNOWN_LIMIT_RULE`。
- 不满250条历史标记 `INSUFFICIENT_HISTORY`，不计算250日位置。
- 当日涨停只使用 `raw_close`；价格形态只使用 `adjusted_close`，两者不会混算。

## 修改参数

```bash
cp .env.example .env
```

默认参数：市值至少80亿元、近20个交易日至少2次涨停、不能连续涨停、250日位置不高于30%、前次涨停后回撤3%～25%、近20日累计涨幅不高于40%。市场开关包括：

```text
EXCLUDE_ST=true
INCLUDE_STAR_MARKET=true
INCLUDE_CHINEXT=true
INCLUDE_BSE=true
```

## 数据源探测

```bash
python scripts/probe_sources.py 20260924
```

脚本会打印东方财富涨停池以及腾讯、​​新浪历史日线的真实返回情况。
