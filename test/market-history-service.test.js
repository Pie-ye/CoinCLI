import test, { afterEach } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  getBtcMarketAnalysis,
  listMarketSyncRuns,
  syncHistoricalBtcMarketData,
} from "../src/market-history-service.js";

const originalDbEnv = process.env.WEALTH_CLI_DB;
const originalFetch = globalThis.fetch;
const tempDirs = [];

function useTempDb() {
  const dir = mkdtempSync(join(tmpdir(), "wealth-cli-history-"));
  tempDirs.push(dir);
  process.env.WEALTH_CLI_DB = join(dir, "wealth.db");
}

function createKlineDataset(total = 2000) {
  const rows = [];
  const dayMs = 24 * 60 * 60 * 1000;
  const startTime = Date.now() - (total - 1) * dayMs;

  for (let index = 0; index < total; index += 1) {
    const openTime = startTime + index * dayMs;
    const base = 20000 + index * 15;
    rows.push([
      openTime,
      `${base}`,
      `${base + 250}`,
      `${base - 180}`,
      `${base + 120}`,
      `${1000 + index}`,
      openTime + dayMs - 1,
      `${(1000 + index) * (base + 120)}`,
      100 + index,
      `${550 + index}`,
      `${(550 + index) * (base + 120)}`,
      "0",
    ]);
  }

  return rows;
}

afterEach(() => {
  process.env.WEALTH_CLI_DB = originalDbEnv;
  globalThis.fetch = originalFetch;

  while (tempDirs.length > 0) {
    rmSync(tempDirs.pop(), { recursive: true, force: true });
  }
});

test("syncHistoricalBtcMarketData 會同步五年 BTC 歷史並產出均線分析", async () => {
  useTempDb();

  const dataset = createKlineDataset(2000);
  const pages = [dataset.slice(-1000), dataset.slice(0, 1000)];
  let callIndex = 0;

  globalThis.fetch = async (url) => {
    assert.match(String(url), /api\/v3\/klines/);
    const payload = pages[callIndex] ?? [];
    callIndex += 1;
    return {
      ok: true,
      async json() {
        return payload;
      },
      status: 200,
      statusText: "OK",
    };
  };

  const syncResult = await syncHistoricalBtcMarketData({ years: 5, delayMs: 0 });
  assert.equal(syncResult.symbol, "BTC");
  assert.equal(syncResult.interval, "1d");
  assert.equal(syncResult.fetchedCount >= 1825, true);
  assert.equal(syncResult.storedCount >= 1825, true);

  const analysisResult = await getBtcMarketAnalysis({ years: 5, refreshIfMissing: false });
  assert.equal(analysisResult.candles.length >= 1825, true);
  assert.equal(analysisResult.analysis.movingAverages.sma20 !== null, true);
  assert.equal(analysisResult.analysis.movingAverages.sma200 !== null, true);
  assert.equal(analysisResult.analysis.volume.avg20 !== null, true);
  assert.equal(Array.isArray(analysisResult.analysis.yearlyReturns), true);

  const runs = listMarketSyncRuns({ limit: 5 });
  assert.equal(runs.length, 1);
  assert.equal(runs[0].status, "success");
});
