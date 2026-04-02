import test, { afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { startApiServer } from "../src/api-server.js";

const originalDbEnv = process.env.WEALTH_CLI_DB;
const originalFetch = globalThis.fetch;
const tempDirs = [];
const servers = [];

function useTempDb() {
  const dir = mkdtempSync(join(tmpdir(), "wealth-cli-api-"));
  tempDirs.push(dir);
  process.env.WEALTH_CLI_DB = join(dir, "wealth.db");
}

function createKlineDataset(total = 2000) {
  const rows = [];
  const dayMs = 24 * 60 * 60 * 1000;
  const startTime = Date.now() - (total - 1) * dayMs;

  for (let index = 0; index < total; index += 1) {
    const openTime = startTime + index * dayMs;
    const base = 25000 + index * 12;
    rows.push([
      openTime,
      `${base}`,
      `${base + 200}`,
      `${base - 150}`,
      `${base + 100}`,
      `${500 + index}`,
      openTime + dayMs - 1,
      `${(500 + index) * (base + 100)}`,
      80 + index,
      `${260 + index}`,
      `${(260 + index) * (base + 100)}`,
      "0",
    ]);
  }

  return rows;
}

afterEach(async () => {
  process.env.WEALTH_CLI_DB = originalDbEnv;
  globalThis.fetch = originalFetch;

  while (servers.length > 0) {
    await new Promise((resolve) => servers.pop().close(resolve));
  }

  while (tempDirs.length > 0) {
    rmSync(tempDirs.pop(), { recursive: true, force: true });
  }
});

test("REST API 可新增收支並提供 BTC 五年分析", async () => {
  useTempDb();

  const dataset = createKlineDataset(2000);
  const pages = [dataset.slice(-1000), dataset.slice(0, 1000)];
  let klineCallIndex = 0;

  globalThis.fetch = async (url, options) => {
    const target = String(url);

    if (target.startsWith("http://127.0.0.1:")) {
      return originalFetch(url, options);
    }

    assert.match(target, /api\/v3\/klines/);
    const payload = pages[klineCallIndex] ?? [];
    klineCallIndex += 1;
    return {
      ok: true,
      async json() {
        return payload;
      },
      status: 200,
      statusText: "OK",
    };
  };

  const started = await startApiServer({ port: 0 });
  servers.push(started.server);
  const baseUrl = `http://127.0.0.1:${started.port}`;

  const addLedgerResponse = await originalFetch(`${baseUrl}/api/ledger/entries`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      entryType: "expense",
      amount: 88,
      description: "咖啡豆",
      category: "餐飲",
      entryDate: "2026-04-02",
    }),
  });
  assert.equal(addLedgerResponse.status, 201);
  const addLedgerPayload = await addLedgerResponse.json();
  assert.equal(addLedgerPayload.entry.description, "咖啡豆");

  const syncResponse = await originalFetch(`${baseUrl}/api/market/btc/history/sync?years=5`, {
    method: "POST",
  });
  assert.equal(syncResponse.status, 202);
  const syncPayload = await syncResponse.json();
  assert.equal(syncPayload.fetchedCount >= 1825, true);

  const analysisResponse = await originalFetch(`${baseUrl}/api/market/btc/analysis?years=5&refreshIfMissing=false`);
  assert.equal(analysisResponse.status, 200);
  const analysisPayload = await analysisResponse.json();
  assert.equal(analysisPayload.symbol, "BTC");
  assert.equal(analysisPayload.analysis.movingAverages.sma50 !== null, true);
  assert.equal(analysisPayload.analysis.trend.aboveSma200 !== null, true);

  const reportResponse = await originalFetch(`${baseUrl}/api/ledger/report?period=month`);
  assert.equal(reportResponse.status, 200);
  const reportPayload = await reportResponse.json();
  assert.equal(reportPayload.totalExpense, 88);
});

test("首頁會提供 Web Console 靜態介面", async () => {
  useTempDb();

  const started = await startApiServer({ port: 0 });
  servers.push(started.server);

  const response = await originalFetch(`http://127.0.0.1:${started.port}/`);
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type"), /text\/html/);

  const html = await response.text();
  assert.match(html, /Wealth CLI \u00b7 Web Console|Wealth CLI · Web Console/);
  assert.match(html, /記帳與投資操作台/);
  assert.doesNotMatch(html, /股利記錄/);
  assert.match(html, /定期收支計畫/);
});

test("REST API 可建立與刪除定期收支計畫", async () => {
  useTempDb();

  const started = await startApiServer({ port: 0 });
  servers.push(started.server);
  const baseUrl = `http://127.0.0.1:${started.port}`;

  const createResponse = await originalFetch(`${baseUrl}/api/ledger/recurring-plans`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      entryType: "income",
      amount: 50000,
      description: "月薪",
      category: "工作",
      scheduleType: "monthly",
      dayOfMonth: 10,
      runTime: "09:00",
      startDate: "2026-04-02",
    }),
  });
  assert.equal(createResponse.status, 201);
  const created = await createResponse.json();
  assert.equal(created.plan.category, "工作");

  const listResponse = await originalFetch(`${baseUrl}/api/ledger/recurring-plans`);
  assert.equal(listResponse.status, 200);
  const listed = await listResponse.json();
  assert.equal(listed.plans.length, 1);

  const deleteResponse = await originalFetch(`${baseUrl}/api/ledger/recurring-plans/${created.plan.id}`, {
    method: "DELETE",
  });
  assert.equal(deleteResponse.status, 200);
});

test("REST API 支援記帳分類與日期區間查詢，且股利端點已移除", async () => {
  useTempDb();

  const started = await startApiServer({ port: 0 });
  servers.push(started.server);
  const baseUrl = `http://127.0.0.1:${started.port}`;

  for (const entry of [
    { entryType: "expense", amount: 120, description: "午餐", category: "餐飲", entryDate: "2026-04-01" },
    { entryType: "expense", amount: 80, description: "捷運", category: "交通", entryDate: "2026-04-02" },
  ]) {
    const response = await originalFetch(`${baseUrl}/api/ledger/entries`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(entry),
    });
    assert.equal(response.status, 201);
  }

  const listResponse = await originalFetch(`${baseUrl}/api/ledger/entries?category=交通&startDate=2026-04-02&endDate=2026-04-02`);
  assert.equal(listResponse.status, 200);
  const listed = await listResponse.json();
  assert.equal(listed.entries.length, 1);
  assert.equal(listed.entries[0].description, "捷運");

  const reportResponse = await originalFetch(`${baseUrl}/api/ledger/report?category=交通&startDate=2026-04-02&endDate=2026-04-02`);
  assert.equal(reportResponse.status, 200);
  const report = await reportResponse.json();
  assert.equal(report.totalExpense, 80);

  const removedDividendResponse = await originalFetch(`${baseUrl}/api/investments/dividends`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ symbol: "BTC", amount: 1 }),
  });
  assert.equal(removedDividendResponse.status, 404);
});
