# Wealth CLI

Node.js 版的個人財務工具，提供三個入口：

- CLI：記帳、投資紀錄、價格更新、BTC 定投
- REST API：給其他記帳軟體或分析流程直接串接
- Web Console：瀏覽器前端，可直接新增 / 刪除 / 顯示收支與投資資料
- Agent：透過 GitHub Copilot SDK 以自然語言操作同一份 SQLite

本專案已移除 Python 依賴，所有主要功能都以 Node.js 為核心。

## 功能

- 收入 / 支出新增、查詢、報表、刪除
- 投資交易、投組損益摘要
- 多來源 BTC 即時價格（Binance / Coinbase / CoinGecko fallback）
- Binance BTC 至少 5 年歷史 K 線同步
- BTC 均線、成交量、區間高低、年化區間報酬分析
- BTC 定期定額計畫與排程執行
- REST API 供外部系統整合

## 環境需求

- Node.js 20+
- npm
- 若要使用 Agent：需完成 GitHub Copilot CLI 登入

## 安裝

```powershell
npm install
```

## CLI

查看說明：

```powershell
npm run cli -- help
```

常用指令：

```powershell
npm run cli -- init
npm run cli -- ledger add expense 150 午餐 --category 餐飲
npm run cli -- ledger report --period month
npm run cli -- invest buy BTC 0.01 70000 --asset-class crypto --market BINANCE
npm run cli -- market btc
npm run cli -- market btc --save
```

若要提供其他工具串接，建議使用 `--json`：

```powershell
npm run cli -- ledger list --period month --json
npm run cli -- market btc --json
```

## REST API

啟動 API：

```powershell
npm run api
```

預設會在 `http://127.0.0.1:8787` 啟動。

## Web Console

啟動 API 後，直接開啟首頁即可使用前端介面：

- `http://127.0.0.1:8787/`

介面內容包含：

- 記帳頁：新增、刪除、顯示收支紀錄與本月分類摘要
- 記帳頁分類固定為：餐飲、工作、投資、娛樂、日用、交通
- 記帳頁可建立每日 / 每月的定期收支計畫，並支援分類 / 日期區間查詢
- 投資頁：新增買賣交易、查看投組摘要與投資活動
- BTC 即時價格卡：在總覽與投資頁面同步顯示當前 BTC 價格

若 Binance 暫時無法連線，系統會自動改用 Coinbase 或 CoinGecko；前端也會保留其他資料畫面，而不會整頁失效。

### 定期收支自動執行

- API 啟動後，內建排程器會每分鐘檢查一次定期收支計畫
- 也可以手動執行：`npm run ledger:run`
- 可用環境變數調整：
	- `WEALTH_LEDGER_SCHEDULER_ENABLED=true|false`
	- `WEALTH_LEDGER_SCHEDULER_INTERVAL_MS=60000`

### 主要端點

- `GET /health`
- `POST /api/init`
- `GET /api/ledger/entries`
- `POST /api/ledger/entries`
- `DELETE /api/ledger/entries/:entryId`
- `GET /api/ledger/report`
- `GET /api/ledger/recurring-plans`
- `POST /api/ledger/recurring-plans`
- `DELETE /api/ledger/recurring-plans/:planId`
- `POST /api/ledger/recurring-plans/run`
- `POST /api/investments/trades`
- `GET /api/investments/activity`
- `GET /api/investments/portfolio`
- `PUT /api/market/prices/:symbol`
- `POST /api/market/prices/refresh`
- `GET /api/market/btc/realtime?save=true`
- `POST /api/market/btc/history/sync?years=5`
- `GET /api/market/btc/history?years=5`
- `GET /api/market/btc/analysis?years=5`
- `GET /api/market/sync-runs`
- `GET /api/recurring/plans`
- `POST /api/recurring/plans`
- `POST /api/recurring/run`

### 範例

新增支出：

```powershell
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8787/api/ledger/entries -ContentType "application/json" -Body '{"entryType":"expense","amount":120,"description":"午餐","category":"餐飲"}'
```

同步五年 BTC 歷史資料：

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8787/api/market/btc/history/sync?years=5"
```

取得 BTC 五年分析：

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8787/api/market/btc/analysis?years=5"
```

## BTC 市場分析

`/api/market/btc/analysis` 目前會提供：

- 20 / 50 / 200 / 365 日均線
- 7 / 30 / 90 / 365 日價格變化
- 20 / 50 日平均成交量與量比
- 30 / 365 日高低區間
- 最近 5 年年度報酬摘要

歷史資料預設來自 Binance `BTCUSDT` 日 K。

## Agent

若仍需自然語言互動：

```powershell
npx copilot auth login
npm run agent -- "幫我記一筆午餐 150 元"
```

## 測試

```powershell
npm test
```

## 環境變數

- `WEALTH_CLI_DB`：自訂 SQLite 路徑
- `WEALTH_API_PORT`：REST API Port
- `COPILOT_MODEL`：Agent 模型名稱
- `COPILOT_CLI_PATH`：自訂 Copilot CLI 路徑

## 專案結構

- `src/cli.js`：CLI 入口
- `src/api-server.js`：REST API 入口
- `public/index.html`：Web Console 頁面
- `public/app.js`：前端互動邏輯
- `public/styles.css`：前端樣式
- `src/wealth-service.js`：記帳與投資服務層
- `src/market-data.js`：多來源 BTC 即時資料與 Binance K 線抓取
- `src/market-history-service.js`：五年 BTC 歷史同步與技術指標分析
- `src/db.js`：SQLite schema 與連線
- `src/recurring-investments.js`：定投排程與執行
- `src/run-recurring-ledger.js`：定期收支手動 runner
- `src/recurring-ledger-scheduler.js`：定期收支背景排程器
- `src/agent.js`：Copilot Agent 入口

## 注意事項

- 歷史資料同步內建重試、退避與基本節流。
- 同步歷史資料時會建立任務鎖，避免重複同步衝突。
- 歷史行情與同步紀錄都會寫入 SQLite，方便後續金融分析流程重用。
