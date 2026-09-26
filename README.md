# A股低位多涨停筛选器 V0.8.1

这是一个个人盘后复盘工具：每天收盘后，寻找“中大市值、近20个交易日多次但不连续涨停、处于250日相对低位、两次涨停间有回落”的股票。它不提供实时交易、K线、AI分析、买卖建议或自动交易；个股近期资讯仅在最新完整交易日按需查询。

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

网页由交易日历驱动：可以进入尚未生成的历史交易日并补跑，周末和交易所休市日不可运行。页面同时展示上证指数环境、已识别涨停、首板/连板/T字板结构、行业分布，以及原有的正式候选和观察池。只有完整全市场收盘快照通过审计时才标记“全市场涨停 / 完整”；历史重建会明确提示免费数据源覆盖限制。

每只已识别涨停股票可独立标记“重点、一般、不要”，保存原因和备注。人工判断按交易日和股票代码单独保存，不属于行情扫描快照，重新抓取和历史补跑不会覆盖；系统还会保留 `LIVE` / `HINDSIGHT` 录入时点标记，供以后统计区分当时判断与事后回看。点击“查看详情”会按需加载并缓存截至该复盘日的腾讯前复权历史画像，以及东方财富最近一期财务摘要。免费资讯接口不能保证历史覆盖，因此历史复盘页不展示当前资讯，避免未来信息泄漏；最新完整交易日仍可按需查看资讯。

补跑单日或一个日期范围：

```bash
python -m app.cli run-date 20260921
python -m app.cli backfill --from 20260901 --to 20260924
python -m app.cli backfill --trading-days 5
```

自动任务可调用 `python -m app.cli run-latest`：休市、未到18:00或当天已发布时会正常输出 `SKIP`，不会白跑。

每次运行都有独立 run 记录，只有 SUCCESS/WARNING 结果才更新该交易日的 published snapshot。数据库默认固定为项目根目录下的 `data/screener.db`，从其他工作目录启动也会使用同一个文件。股票行业只在缺失时按需缓存查询；资讯不会在扫描时批量请求，点击股票行的“资讯”按钮后才查询，并按当天缓存最近5条。

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
MARKET_DATA_DIRECT=true
```

`MARKET_DATA_DIRECT=true` 只让东方财富、腾讯和新浪行情请求使用不继承环境代理的独立连接，不会改变系统代理设置。某些代理软件的 TUN 模式仍会在系统网络层接管连接；若页面技术详情提示网络问题，请把 `eastmoney.com`、`gtimg.cn`、`sina.com.cn` 设置为直连后，在网页点击“重新抓取今日数据”。

## 数据源探测

```bash
python scripts/probe_sources.py 20260924
```

脚本会打印东方财富涨停池以及腾讯、​​新浪历史日线的真实返回情况。
