#!/usr/bin/env node

import express from "express";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";

import {
  addLedgerEntry,
  createRecurringLedgerPlan,
  deleteLedgerEntry,
  deleteRecurringLedgerPlan,
  getBtcRealtimeInfo,
  getDatabasePath,
  getLedgerReport,
  getPortfolioSummary,
  initDatabase,
  listInvestmentActivity,
  listLedgerEntries,
  listRecurringLedgerPlans,
  listRecurringInvestmentPlans,
  recordInvestmentTrade,
  refreshMarketPrices,
  runDueRecurringLedgerPlans,
  runDueRecurringInvestmentPlans,
  setMarketPrice,
  upsertRecurringInvestmentPlan,
} from "./wealth-service.js";
import { startRecurringLedgerScheduler } from "./recurring-ledger-scheduler.js";
import {
  getBtcMarketAnalysis,
  getHistoricalBtcMarketData,
  listMarketSyncRuns,
  syncHistoricalBtcMarketData,
} from "./market-history-service.js";

function toNumber(value, fieldName) {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error(`${fieldName} must be numeric.`);
  }
  return numeric;
}

function toBoolean(value, fieldName) {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["true", "1", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["false", "0", "no", "n", "off"].includes(normalized)) {
    return false;
  }
  throw new Error(`${fieldName} must be true or false.`);
}

function asyncHandler(handler) {
  return async (request, response, next) => {
    try {
      await handler(request, response, next);
    } catch (error) {
      next(error);
    }
  };
}

const currentFilePath = fileURLToPath(import.meta.url);
const currentDirPath = dirname(currentFilePath);
const publicDirPath = join(currentDirPath, "..", "public");

export function createApiApp() {
  const app = express();
  app.use(express.json({ limit: "1mb" }));
  app.use(express.static(publicDirPath));

  app.get("/health", (_request, response) => {
    response.json({ ok: true, service: "wealth-api", dbPath: String(getDatabasePath()) });
  });

  app.post("/api/init", (_request, response) => {
    response.status(201).json(initDatabase());
  });

  app.get(
    "/api/ledger/entries",
    asyncHandler(async (request, response) => {
      const result = listLedgerEntries({
        period: request.query.period,
        startDate: request.query.startDate,
        endDate: request.query.endDate,
        entryType: request.query.entryType,
        category: request.query.category,
        limit: toNumber(request.query.limit, "limit"),
      });
      response.json(result);
    }),
  );

  app.post(
    "/api/ledger/entries",
    asyncHandler(async (request, response) => {
      const body = request.body ?? {};
      const result = addLedgerEntry({
        entryType: body.entryType,
        amount: toNumber(body.amount, "amount"),
        description: body.description,
        category: body.category,
        tags: body.tags,
        entryDate: body.entryDate,
      });
      response.status(201).json(result);
    }),
  );

  app.delete(
    "/api/ledger/entries/:entryId",
    asyncHandler(async (request, response) => {
      const result = deleteLedgerEntry({ entryId: toNumber(request.params.entryId, "entryId") });
      response.json(result);
    }),
  );

  app.get(
    "/api/ledger/report",
    asyncHandler(async (request, response) => {
      const result = getLedgerReport({
        period: request.query.period,
        startDate: request.query.startDate,
        endDate: request.query.endDate,
        entryType: request.query.entryType,
        category: request.query.category,
      });
      response.json(result);
    }),
  );

  app.get(
    "/api/ledger/recurring-plans",
    asyncHandler(async (request, response) => {
      response.json({
        plans: listRecurringLedgerPlans({ enabled: toBoolean(request.query.enabled, "enabled") }),
      });
    }),
  );

  app.post(
    "/api/ledger/recurring-plans",
    asyncHandler(async (request, response) => {
      const body = request.body ?? {};
      const result = createRecurringLedgerPlan({
        entryType: body.entryType,
        amount: toNumber(body.amount, "amount"),
        description: body.description,
        category: body.category,
        scheduleType: body.scheduleType,
        runTime: body.runTime,
        dayOfMonth: toNumber(body.dayOfMonth, "dayOfMonth"),
        timeZone: body.timeZone,
        startDate: body.startDate,
        note: body.note,
        enabled: toBoolean(body.enabled, "enabled"),
      });
      response.status(201).json(result);
    }),
  );

  app.delete(
    "/api/ledger/recurring-plans/:planId",
    asyncHandler(async (request, response) => {
      response.json(deleteRecurringLedgerPlan({ planId: toNumber(request.params.planId, "planId") }));
    }),
  );

  app.post(
    "/api/ledger/recurring-plans/run",
    asyncHandler(async (_request, response) => {
      response.json(runDueRecurringLedgerPlans());
    }),
  );

  app.post(
    "/api/investments/trades",
    asyncHandler(async (request, response) => {
      const body = request.body ?? {};
      const result = recordInvestmentTrade({
        tradeType: body.tradeType,
        symbol: body.symbol,
        quantity: toNumber(body.quantity, "quantity"),
        unitPrice: toNumber(body.unitPrice, "unitPrice"),
        fee: toNumber(body.fee, "fee") ?? 0,
        tradeDate: body.tradeDate,
        assetClass: body.assetClass,
        market: body.market,
        note: body.note,
      });
      response.status(201).json(result);
    }),
  );

  app.get(
    "/api/investments/activity",
    asyncHandler(async (request, response) => {
      const result = listInvestmentActivity({
        symbol: request.query.symbol,
        limit: toNumber(request.query.limit, "limit"),
      });
      response.json(result);
    }),
  );

  app.get(
    "/api/investments/portfolio",
    asyncHandler(async (_request, response) => {
      response.json(getPortfolioSummary());
    }),
  );

  app.put(
    "/api/market/prices/:symbol",
    asyncHandler(async (request, response) => {
      const body = request.body ?? {};
      const result = setMarketPrice({
        symbol: request.params.symbol,
        price: toNumber(body.price, "price"),
        currency: body.currency,
        source: body.source,
        asOf: body.asOf,
      });
      response.json(result);
    }),
  );

  app.post(
    "/api/market/prices/refresh",
    asyncHandler(async (request, response) => {
      const body = request.body ?? {};
      const result = await refreshMarketPrices({ symbol: body.symbol ?? request.query.symbol });
      response.json(result);
    }),
  );

  app.get(
    "/api/market/btc/realtime",
    asyncHandler(async (request, response) => {
      const result = await getBtcRealtimeInfo({
        save: toBoolean(request.query.save, "save") ?? false,
      });
      response.json(result);
    }),
  );

  app.get(
    "/api/market/btc/history",
    asyncHandler(async (request, response) => {
      const result = await getHistoricalBtcMarketData({
        years: toNumber(request.query.years, "years") ?? 5,
        interval: request.query.interval ?? "1d",
        refreshIfMissing: toBoolean(request.query.refreshIfMissing, "refreshIfMissing") ?? true,
      });
      response.json(result);
    }),
  );

  app.post(
    "/api/market/btc/history/sync",
    asyncHandler(async (request, response) => {
      const body = request.body ?? {};
      const result = await syncHistoricalBtcMarketData({
        years: toNumber(body.years ?? request.query.years, "years") ?? 5,
        interval: body.interval ?? request.query.interval ?? "1d",
      });
      response.status(202).json(result);
    }),
  );

  app.get(
    "/api/market/btc/analysis",
    asyncHandler(async (request, response) => {
      const result = await getBtcMarketAnalysis({
        years: toNumber(request.query.years, "years") ?? 5,
        interval: request.query.interval ?? "1d",
        refreshIfMissing: toBoolean(request.query.refreshIfMissing, "refreshIfMissing") ?? true,
      });
      response.json(result);
    }),
  );

  app.get(
    "/api/market/sync-runs",
    asyncHandler(async (request, response) => {
      response.json({ runs: listMarketSyncRuns({ limit: toNumber(request.query.limit, "limit") ?? 20 }) });
    }),
  );

  app.get(
    "/api/recurring/plans",
    asyncHandler(async (request, response) => {
      response.json({
        plans: listRecurringInvestmentPlans({ enabled: toBoolean(request.query.enabled, "enabled") }),
      });
    }),
  );

  app.post(
    "/api/recurring/plans",
    asyncHandler(async (request, response) => {
      const body = request.body ?? {};
      const result = upsertRecurringInvestmentPlan({
        symbol: body.symbol,
        budgetAmount: toNumber(body.budgetAmount, "budgetAmount"),
        quoteCurrency: body.quoteCurrency,
        scheduleType: body.scheduleType,
        runTime: body.runTime,
        timeZone: body.timeZone,
        assetClass: body.assetClass,
        market: body.market,
        priceSource: body.priceSource,
        note: body.note,
        enabled: toBoolean(body.enabled, "enabled"),
      });
      response.status(201).json(result);
    }),
  );

  app.post(
    "/api/recurring/run",
    asyncHandler(async (_request, response) => {
      const result = await runDueRecurringInvestmentPlans();
      response.json(result);
    }),
  );

  app.use((error, _request, response, _next) => {
    const statusCode = /not found/i.test(error.message) ? 404 : 400;
    response.status(statusCode).json({
      error: {
        message: error instanceof Error ? error.message : String(error),
      },
    });
  });

  return app;
}

export function startApiServer({ port = Number(process.env.WEALTH_API_PORT || 8787) } = {}) {
  const app = createApiApp();
  return new Promise((resolve) => {
    const server = app.listen(port, () => {
      const scheduler = startRecurringLedgerScheduler();
      server.on("close", () => {
        scheduler.stop();
      });
      const address = server.address();
      resolve({
        app,
        server,
        scheduler,
        port: typeof address === "object" && address ? address.port : port,
      });
    });
  });
}

async function main() {
  const { port } = await startApiServer();
  console.log(`Wealth API server is listening on http://127.0.0.1:${port}`);
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  main().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Wealth API server failed to start: ${message}`);
    process.exitCode = 1;
  });
}
