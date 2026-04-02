import { randomUUID } from "node:crypto";

import { openDatabase, withDatabase } from "./db.js";
import { fetchHistoricalKlines } from "./market-data.js";

const DEFAULT_SYMBOL = "BTC";
const DEFAULT_BINANCE_SYMBOL = "BTCUSDT";
const DEFAULT_INTERVAL = "1d";
const DEFAULT_YEARS = 5;
const LOCK_TTL_MS = 10 * 60 * 1000;

function normalizeSymbol(symbol = DEFAULT_SYMBOL) {
  const normalized = String(symbol).trim().toUpperCase();
  if (normalized === "BTC" || normalized === DEFAULT_BINANCE_SYMBOL) {
    return { symbol: "BTC", resolvedSymbol: DEFAULT_BINANCE_SYMBOL };
  }
  throw new Error("Historical market sync currently supports BTC only.");
}

function normalizeInterval(interval = DEFAULT_INTERVAL) {
  const normalized = String(interval).trim();
  if (!["1d", "1w", "1M"].includes(normalized)) {
    throw new Error(`Unsupported interval: ${normalized}`);
  }
  return normalized;
}

function normalizeYears(years = DEFAULT_YEARS) {
  const normalized = Number(years);
  if (!Number.isInteger(normalized) || normalized < 1 || normalized > 15) {
    throw new Error("years must be an integer between 1 and 15.");
  }
  return normalized;
}

function round(value, digits = 2) {
  if (value === null || value === undefined) {
    return null;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  const factor = 10 ** digits;
  return Math.round((numeric + Number.EPSILON) * factor) / factor;
}

function isoNow() {
  return new Date().toISOString();
}

function yearsAgoIso(years) {
  return new Date(Date.now() - years * 365 * 24 * 60 * 60 * 1000).toISOString();
}

function average(values) {
  if (values.length === 0) {
    return null;
  }
  return values.reduce((sum, value) => sum + Number(value), 0) / values.length;
}

function sliceLast(items, count) {
  return items.slice(Math.max(0, items.length - count));
}

function extractClosing(items, count) {
  return sliceLast(items, count).map((item) => Number(item.closePrice));
}

function extractVolume(items, count) {
  return sliceLast(items, count).map((item) => Number(item.volume));
}

function computeSma(items, period) {
  const values = extractClosing(items, period);
  if (values.length < period) {
    return null;
  }
  return average(values);
}

function computePctChange(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous === 0) {
    return null;
  }
  return ((current - previous) / previous) * 100;
}

function findReferenceClose(items, lookback) {
  if (items.length <= lookback) {
    return null;
  }
  return Number(items[items.length - 1 - lookback].closePrice);
}

function computeYearlyReturns(items) {
  const yearlyCloses = new Map();

  for (const item of items) {
    const year = item.openTime.slice(0, 4);
    yearlyCloses.set(year, Number(item.closePrice));
  }

  const years = [...yearlyCloses.keys()].sort();
  const returns = [];
  for (let index = 1; index < years.length; index += 1) {
    const previousYear = years[index - 1];
    const currentYear = years[index];
    const previousClose = yearlyCloses.get(previousYear);
    const currentClose = yearlyCloses.get(currentYear);
    returns.push({
      year: currentYear,
      close: round(currentClose, 2),
      annualReturnPct: round(computePctChange(currentClose, previousClose), 2),
    });
  }

  return returns.slice(-5);
}

function computeRange(items, count) {
  const window = sliceLast(items, count);
  if (window.length === 0) {
    return { high: null, low: null };
  }
  return {
    high: round(Math.max(...window.map((item) => Number(item.highPrice))), 2),
    low: round(Math.min(...window.map((item) => Number(item.lowPrice))), 2),
  };
}

function mapStoredKline(row) {
  return {
    symbol: row.symbol,
    interval: row.interval,
    openTime: row.open_time,
    closeTime: row.close_time,
    openPrice: Number(row.open_price),
    highPrice: Number(row.high_price),
    lowPrice: Number(row.low_price),
    closePrice: Number(row.close_price),
    volume: Number(row.volume),
    quoteVolume: Number(row.quote_volume),
    tradeCount: Number(row.trade_count),
    takerBuyBaseVolume: Number(row.taker_buy_base_volume),
    takerBuyQuoteVolume: Number(row.taker_buy_quote_volume),
    source: row.source,
  };
}

function acquireTaskLock(db, lockKey, ownerId, ttlMs = LOCK_TTL_MS) {
  const now = isoNow();
  const expiresAt = new Date(Date.now() + ttlMs).toISOString();

  db.exec("BEGIN IMMEDIATE");
  try {
    db.prepare("DELETE FROM task_locks WHERE lock_key = ? AND expires_at <= ?").run(lockKey, now);
    const existing = db.prepare("SELECT owner_id, expires_at FROM task_locks WHERE lock_key = ?").get(lockKey);
    if (existing) {
      db.exec("ROLLBACK");
      throw new Error(`Task lock is already active for ${lockKey} until ${existing.expires_at}.`);
    }

    db.prepare(
      `
      INSERT INTO task_locks (lock_key, owner_id, locked_at, expires_at, updated_at)
      VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
      `,
    ).run(lockKey, ownerId, now, expiresAt);
    db.exec("COMMIT");
  } catch (error) {
    try {
      db.exec("ROLLBACK");
    } catch {
      // ignore rollback noise
    }
    throw error;
  }
}

function releaseTaskLock(db, lockKey, ownerId) {
  db.prepare("DELETE FROM task_locks WHERE lock_key = ? AND owner_id = ?").run(lockKey, ownerId);
}

function createSyncRun(db, { jobName, symbol, interval, rangeStart, rangeEnd }) {
  const result = db.prepare(
    `
    INSERT INTO market_sync_runs (job_name, symbol, interval, range_start, range_end, status)
    VALUES (?, ?, ?, ?, ?, 'pending')
    `,
  ).run(jobName, symbol, interval, rangeStart, rangeEnd);

  return Number(result.lastInsertRowid);
}

function completeSyncRun(db, runId, { status, fetchedCount = 0, storedCount = 0, errorMessage = "" }) {
  db.prepare(
    `
    UPDATE market_sync_runs
    SET status = ?, fetched_count = ?, stored_count = ?, error_message = ?, finished_at = CURRENT_TIMESTAMP
    WHERE id = ?
    `,
  ).run(status, fetchedCount, storedCount, errorMessage, runId);
}

function upsertKlines(db, klines) {
  const statement = db.prepare(
    `
    INSERT INTO market_klines (
      symbol, interval, open_time, close_time, open_price, high_price, low_price, close_price,
      volume, quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume, source,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(symbol, interval, open_time) DO UPDATE SET
      close_time = excluded.close_time,
      open_price = excluded.open_price,
      high_price = excluded.high_price,
      low_price = excluded.low_price,
      close_price = excluded.close_price,
      volume = excluded.volume,
      quote_volume = excluded.quote_volume,
      trade_count = excluded.trade_count,
      taker_buy_base_volume = excluded.taker_buy_base_volume,
      taker_buy_quote_volume = excluded.taker_buy_quote_volume,
      source = excluded.source,
      updated_at = CURRENT_TIMESTAMP
    `,
  );

  let storedCount = 0;
  for (const item of klines) {
    statement.run(
      item.symbol,
      item.interval,
      item.openTime,
      item.closeTime,
      item.openPrice,
      item.highPrice,
      item.lowPrice,
      item.closePrice,
      item.volume,
      item.quoteVolume,
      item.tradeCount,
      item.takerBuyBaseVolume,
      item.takerBuyQuoteVolume,
      item.source ?? "binance",
    );
    storedCount += 1;
  }

  if (klines.length > 0) {
    const latest = klines[klines.length - 1];
    db.prepare(
      `
      INSERT INTO market_prices (symbol, price, currency, source, as_of, updated_at)
      VALUES (?, ?, 'USDT', 'binance-history', ?, CURRENT_TIMESTAMP)
      ON CONFLICT(symbol) DO UPDATE SET
        price = excluded.price,
        currency = excluded.currency,
        source = excluded.source,
        as_of = excluded.as_of,
        updated_at = CURRENT_TIMESTAMP
      `,
    ).run(latest.symbol === DEFAULT_BINANCE_SYMBOL ? DEFAULT_SYMBOL : latest.symbol, latest.closePrice, latest.openTime.slice(0, 10));
  }

  return storedCount;
}

function readStoredKlines({ symbol = DEFAULT_SYMBOL, interval = DEFAULT_INTERVAL, years = DEFAULT_YEARS }) {
  const normalized = normalizeSymbol(symbol);
  const normalizedInterval = normalizeInterval(interval);
  const normalizedYears = normalizeYears(years);
  const rangeStart = yearsAgoIso(normalizedYears);

  return withDatabase((db) =>
    db
      .prepare(
        `
        SELECT *
        FROM market_klines
        WHERE symbol = ? AND interval = ? AND open_time >= ?
        ORDER BY open_time ASC
        `,
      )
      .all(normalized.symbol, normalizedInterval, rangeStart)
      .map(mapStoredKline),
  );
}

function hasEnoughHistory(klines, years) {
  if (klines.length === 0) {
    return false;
  }
  const earliest = Date.parse(klines[0].openTime);
  const expectedStart = Date.now() - normalizeYears(years) * 365 * 24 * 60 * 60 * 1000;
  const toleranceMs = 10 * 24 * 60 * 60 * 1000;
  return earliest <= expectedStart + toleranceMs;
}

export async function syncHistoricalBtcMarketData({
  symbol = DEFAULT_SYMBOL,
  interval = DEFAULT_INTERVAL,
  years = DEFAULT_YEARS,
  fetchImpl,
  delayMs,
} = {}) {
  const normalizedSymbol = normalizeSymbol(symbol);
  const normalizedInterval = normalizeInterval(interval);
  const normalizedYears = normalizeYears(years);
  const rangeStart = yearsAgoIso(normalizedYears);
  const rangeEnd = isoNow();
  const lockKey = `market-sync:${normalizedSymbol.symbol}:${normalizedInterval}`;
  const ownerId = randomUUID();
  const { db, dbPath } = openDatabase();
  const runId = createSyncRun(db, {
    jobName: "btc-history-sync",
    symbol: normalizedSymbol.symbol,
    interval: normalizedInterval,
    rangeStart,
    rangeEnd,
  });

  try {
    acquireTaskLock(db, lockKey, ownerId);
    const klines = await fetchHistoricalKlines({
      symbol: normalizedSymbol.resolvedSymbol,
      interval: normalizedInterval,
      years: normalizedYears,
      fetchImpl,
      delayMs,
    });

    const translated = klines.map((item) => ({
      ...item,
      symbol: normalizedSymbol.symbol,
    }));

    db.exec("BEGIN IMMEDIATE");
    let storedCount = 0;
    try {
      storedCount = upsertKlines(db, translated);
      db.exec("COMMIT");
    } catch (error) {
      db.exec("ROLLBACK");
      throw error;
    }

    completeSyncRun(db, runId, {
      status: "success",
      fetchedCount: klines.length,
      storedCount,
    });

    return {
      dbPath,
      runId,
      symbol: normalizedSymbol.symbol,
      resolvedSymbol: normalizedSymbol.resolvedSymbol,
      interval: normalizedInterval,
      years: normalizedYears,
      fetchedCount: klines.length,
      storedCount,
      latestCandle: translated.at(-1) ?? null,
      rangeStart,
      rangeEnd,
    };
  } catch (error) {
    completeSyncRun(db, runId, {
      status: "failed",
      errorMessage: error instanceof Error ? error.message : String(error),
    });
    throw error;
  } finally {
    releaseTaskLock(db, lockKey, ownerId);
    db.close();
  }
}

export async function getHistoricalBtcMarketData({
  symbol = DEFAULT_SYMBOL,
  interval = DEFAULT_INTERVAL,
  years = DEFAULT_YEARS,
  refreshIfMissing = true,
  fetchImpl,
  delayMs,
} = {}) {
  const normalizedSymbol = normalizeSymbol(symbol);
  const normalizedInterval = normalizeInterval(interval);
  const normalizedYears = normalizeYears(years);

  let klines = readStoredKlines({
    symbol: normalizedSymbol.symbol,
    interval: normalizedInterval,
    years: normalizedYears,
  });

  if (refreshIfMissing && !hasEnoughHistory(klines, normalizedYears)) {
    await syncHistoricalBtcMarketData({
      symbol: normalizedSymbol.symbol,
      interval: normalizedInterval,
      years: normalizedYears,
      fetchImpl,
      delayMs,
    });
    klines = readStoredKlines({
      symbol: normalizedSymbol.symbol,
      interval: normalizedInterval,
      years: normalizedYears,
    });
  }

  return {
    symbol: normalizedSymbol.symbol,
    resolvedSymbol: normalizedSymbol.resolvedSymbol,
    interval: normalizedInterval,
    years: normalizedYears,
    candles: klines,
  };
}

export async function getBtcMarketAnalysis(options = {}) {
  const history = await getHistoricalBtcMarketData(options);
  const candles = history.candles;
  if (candles.length === 0) {
    return {
      ...history,
      analysis: null,
    };
  }

  const latest = candles[candles.length - 1];
  const latestClose = Number(latest.closePrice);
  const currentVolume = Number(latest.volume);
  const sma20 = computeSma(candles, 20);
  const sma50 = computeSma(candles, 50);
  const sma200 = computeSma(candles, 200);
  const sma365 = computeSma(candles, 365);
  const avgVol20 = average(extractVolume(candles, 20));
  const avgVol50 = average(extractVolume(candles, 50));
  const reference7 = findReferenceClose(candles, 7);
  const reference30 = findReferenceClose(candles, 30);
  const reference90 = findReferenceClose(candles, 90);
  const reference365 = findReferenceClose(candles, 365);
  const range30 = computeRange(candles, 30);
  const range365 = computeRange(candles, 365);

  return {
    ...history,
    analysis: {
      latest: {
        openTime: latest.openTime,
        closeTime: latest.closeTime,
        closePrice: round(latestClose, 2),
        volume: round(currentVolume, 4),
        quoteVolume: round(latest.quoteVolume, 2),
        tradeCount: latest.tradeCount,
      },
      movingAverages: {
        sma20: round(sma20, 2),
        sma50: round(sma50, 2),
        sma200: round(sma200, 2),
        sma365: round(sma365, 2),
      },
      momentum: {
        change7dPct: round(computePctChange(latestClose, reference7), 2),
        change30dPct: round(computePctChange(latestClose, reference30), 2),
        change90dPct: round(computePctChange(latestClose, reference90), 2),
        change365dPct: round(computePctChange(latestClose, reference365), 2),
      },
      volume: {
        current: round(currentVolume, 4),
        avg20: round(avgVol20, 4),
        avg50: round(avgVol50, 4),
        ratio20: round(avgVol20 ? currentVolume / avgVol20 : null, 4),
        ratio50: round(avgVol50 ? currentVolume / avgVol50 : null, 4),
      },
      ranges: {
        high30d: range30.high,
        low30d: range30.low,
        high365d: range365.high,
        low365d: range365.low,
      },
      trend: {
        aboveSma20: sma20 === null ? null : latestClose > sma20,
        aboveSma50: sma50 === null ? null : latestClose > sma50,
        aboveSma200: sma200 === null ? null : latestClose > sma200,
        goldenCrossLike: sma50 === null || sma200 === null ? null : sma50 > sma200,
      },
      yearlyReturns: computeYearlyReturns(candles),
      sampleSize: candles.length,
    },
  };
}

export function listMarketSyncRuns({ limit = 20 } = {}) {
  const normalizedLimit = Math.max(1, Math.min(Number(limit) || 20, 100));
  return withDatabase((db) =>
    db
      .prepare(
        `
        SELECT *
        FROM market_sync_runs
        ORDER BY started_at DESC, id DESC
        LIMIT ?
        `,
      )
      .all(normalizedLimit)
      .map((row) => ({
        id: row.id,
        jobName: row.job_name,
        symbol: row.symbol,
        interval: row.interval,
        rangeStart: row.range_start,
        rangeEnd: row.range_end,
        status: row.status,
        fetchedCount: row.fetched_count,
        storedCount: row.stored_count,
        errorMessage: row.error_message,
        startedAt: row.started_at,
        finishedAt: row.finished_at,
      })),
  );
}
