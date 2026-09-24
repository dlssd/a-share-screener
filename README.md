# A股低位多涨停筛选器 V0.1

目标很窄：**每天收盘后，稳定筛出“中大市值 + 相对低位 + 近一个月多次非连续涨停 + 中间有回落”的股票**。不重做K线，不替代同花顺/开盘啦。

## 为什么第一版用 Tushare Pro 做主数据源

本项目把行情数据和筛选逻辑分开。第一版主源使用 Tushare Pro：

- `daily`：未复权日线 OHLCV
- `daily_basic`：总市值等每日指标
- `stk_limit`：每天每只股票真实涨停/跌停价
- `adj_factor`：复权因子
- `trade_cal`：交易日历
- `stock_basic`：代码、名称、行业

**涨停判断使用未复权收盘价 vs 当日真实涨停价；低位判断使用复权后的历史收盘价。** 这两个口径不要混用。

AKShare 已放在依赖里，但 V0.1 不会在 Tushare 失败时“静默切换”数据源。对于你的用途，错选比暂时没有结果更糟。后续可以把 AKShare/东方财富作为审计源，对最终候选做二次核验。

## 默认规则

默认 `.env`：

- 当日必须涨停
- 总市值 >= 80 亿元
- 最近 20 个交易日至少 2 次涨停
- 最近 20 个交易日不能出现连续涨停
- 当前复权价位于最近 250 个交易日高低区间的下 30%
- 前一次涨停后、今天再次涨停前，至少出现 3% 回撤，且不超过 25%
- 最近 20 个交易日累计上涨不超过 40%

这些都是 **V1 参数，不是假定它们天然有效**。先让机器复现你的手工选股，再通过历史结果调整。

## 数据准确性设计

每天写入数据库前会先做完整性校验：

1. 每张日表必须是目标交易日；
2. 不允许同一股票重复行；
3. 行数异常偏少直接失败；
4. OHLC 逻辑异常直接失败；
5. `daily_basic` / `adj_factor` 对日线代码覆盖率不足直接失败；
6. 只有校验通过后才事务写入；失败时保留上一次已验证数据，不发布“半套数据”；
7. 网页顶部显示最近成功交易日和各表行数，避免把旧数据误认为今天数据。

## 首次运行

需要 Python 3.11+，以及 Tushare Pro Token。

```bash
cp .env.example .env
# 编辑 .env，填 TUSHARE_TOKEN 和网页密码

python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# Windows: .venv\Scripts\activate
pip install -r requirements.txt

python -m app.cli init-db
python -m app.cli backfill --trading-days 320
uvicorn app.web:app --reload
```

浏览器打开 `http://127.0.0.1:8000`。

首次回填会按交易日拉取约 320 天数据，支持重复执行；成功日期会跳过。以后每天只增量拉一个交易日。

## 腾讯云 Docker 部署

```bash
cp .env.example .env
# 填好 TUSHARE_TOKEN、APP_USERNAME、APP_PASSWORD

docker compose build
# 首次回填（只做一次）
docker compose run --rm web python -m app.cli backfill --trading-days 320
# 启动网页 + 自动任务
docker compose up -d
```

网页默认映射服务器 `8000` 端口。生产环境建议再放到 Nginx/Caddy 后面启用 HTTPS，并在腾讯云安全组只开放必要端口。

`scheduler` 会在北京时间工作日 17:20、18:05、19:00 做幂等检查；第一次成功后，后两次自动跳过。服务重启也会先检查是否漏了最近一个应完成交易日。

## 日常使用

网页只显示当日候选：代码、名称、行业、市值、最近20日涨停次数、250日位置、前次涨停后的回撤、涨停日期，并提供“复制名称 / 复制代码”。复制后直接去同花顺或开盘啦复盘即可。

## 下一步（V0.2）

- 对最终候选用第二数据源交叉核验收盘价/涨停状态；
- 保存“我保留 / 我排除”以及人工理由；
- 20个交易日后自动统计你保留和排除的结果；
- 再决定是否加入开盘啦/同花顺题材字段和 AI 服务器、存储等主题标签。
