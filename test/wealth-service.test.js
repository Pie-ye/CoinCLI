import test, { afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { openDatabase } from "../src/db.js";
import {
  listLedgerEntries,
  createRecurringLedgerPlan,
  deleteRecurringLedgerPlan,
  getBtcRealtimeInfo,
  getPortfolioSummary,
  initDatabase,
  listRecurringLedgerPlans,
  recordInvestmentTrade,
  refreshMarketPrices,
  runDueRecurringLedgerPlans,
} from "../src/wealth-service.js";

const originalDbEnv = process.env.WEALTH_CLI_DB;
const originalFetch = globalThis.fetch;
const tempDirs = [];

function useTempDb() {
  const dir = mkdtempSync(join(tmpdir(), "wealth-cli-"));
  tempDirs.push(dir);
  process.env.WEALTH_CLI_DB = join(dir, "wealth.db");
}

afterEach(() => {
  process.env.WEALTH_CLI_DB = originalDbEnv;
  globalThis.fetch = originalFetch;

  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("refreshMarketPrices 會抓取 Binance BTC 價格並寫入資料庫", async () => {
  useTempDb();
  initDatabase();
  recordInvestmentTrade({
    tradeType: "buy",
    symbol: "BTC",
    quantity: 0.01,
    unitPrice: 65000,
    fee: 0,
    assetClass: "crypto",
    market: "BINANCE",
    tradeDate: "2026-04-02",
  });

  globalThis.fetch = async (url) => {
    assert.match(String(url), /ticker\/price\?symbol=BTCUSDT$/);
    return {
      ok: true,
      async json() {
        return { symbol: "BTCUSDT", price: "70123.45" };
      },
    };
  };

  const refreshed = await refreshMarketPrices();
  assert.equal(refreshed.updated.length, 1);
  assert.equal(refreshed.updated[0].symbol, "BTC");
  assert.equal(refreshed.updated[0].price, 70123.45);

  const portfolio = getPortfolioSummary();
  assert.equal(portfolio.positions[0].symbol, "BTC");
  assert.equal(portfolio.positions[0].price, 70123.45);
});

test("getBtcRealtimeInfo 在 save=true 時會同步保存市場價格", async () => {
  useTempDb();
  initDatabase();

  globalThis.fetch = async (url) => {
    assert.match(String(url), /ticker\/24hr\?symbol=BTCUSDT$/);
    return {
      ok: true,
      async json() {
        return {
          symbol: "BTCUSDT",
          priceChange: "500.00",
          priceChangePercent: "0.75",
          weightedAvgPrice: "66888.12",
          lastPrice: "67234.56",
          openPrice: "66734.56",
          highPrice: "68000.00",
          lowPrice: "66000.00",
          volume: "4321.987",
          quoteVolume: "288000000.50",
          openTime: 1719990000000,
          closeTime: 1719993600000,
          count: 123456,
        };
      },
    };
  };

  const result = await getBtcRealtimeInfo({ save: true });
  assert.equal(result.saved, true);
  assert.equal(result.snapshot.price, 67234.56);

  const { db } = openDatabase();
  try {
    const row = db.prepare("SELECT symbol, price, currency, source FROM market_prices WHERE symbol = ?").get("BTC");
    assert.equal(row.symbol, "BTC");
    assert.equal(row.price, 67234.56);
    assert.equal(row.currency, "USDT");
    assert.equal(row.source, "binance");
  } finally {
    db.close();
  }
});

test("可建立與刪除定期收支計畫", () => {
  useTempDb();
  initDatabase();

  const created = createRecurringLedgerPlan({
    entryType: "expense",
    amount: 1200,
    description: "通勤月票",
    category: "交通",
    scheduleType: "monthly",
    runTime: "08:30",
    dayOfMonth: 5,
    startDate: "2026-04-02",
  });

  assert.equal(created.plan.category, "交通");
  assert.equal(created.plan.scheduleType, "monthly");
  assert.equal(created.plan.dayOfMonth, 5);
  assert.match(created.plan.nextOccurrence, /^\d{4}-\d{2}-\d{2}T08:30:00$/);

  const plans = listRecurringLedgerPlans();
  assert.equal(plans.length, 1);
  assert.equal(plans[0].description, "通勤月票");

  const deleted = deleteRecurringLedgerPlan({ planId: created.plan.id });
  assert.equal(deleted.deleted.id, created.plan.id);
  assert.equal(listRecurringLedgerPlans().length, 0);
});

test("runDueRecurringLedgerPlans 會自動入帳且避免重複執行", () => {
  useTempDb();
  initDatabase();

  createRecurringLedgerPlan({
    entryType: "expense",
    amount: 150,
    description: "午餐預算",
    category: "餐飲",
    scheduleType: "daily",
    runTime: "09:00",
    startDate: "2026-04-02",
  });

  const firstRun = runDueRecurringLedgerPlans({ now: new Date("2026-04-02T01:10:00.000Z") });
  assert.equal(firstRun.executed.length, 1);
  assert.equal(firstRun.executed[0].category, "餐飲");

  const ledger = listLedgerEntries({ category: "餐飲", startDate: "2026-04-02", endDate: "2026-04-02" });
  assert.equal(ledger.entries.length, 1);
  assert.match(ledger.entries[0].tags.join(","), /auto,plan-/);

  const secondRun = runDueRecurringLedgerPlans({ now: new Date("2026-04-02T02:00:00.000Z") });
  assert.equal(secondRun.executed.length, 0);
  assert.equal(secondRun.skipped.some((item) => item.reason === "already_executed"), true);
});
