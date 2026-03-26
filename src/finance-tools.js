import { defineTool } from "@github/copilot-sdk";
import { z } from "zod";

import { withDatabase } from "./db.js";
import { fetchLatestMarketPrice } from "./market-data.js";
import {
  listRecurringInvestmentPlans,
  runDueRecurringInvestmentPlans,
  upsertRecurringInvestmentPlan,
} from "./recurring-investments.js";

const PERIOD_VALUES = ["day", "week", "month", "year", "all"];

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function ensureIsoDate(raw, fieldName = "date") {
  if (!raw) {
    return todayIso();
  }

  const normalized = String(raw).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw new Error(`${fieldName} must use YYYY-MM-DD format.`);
  }

  const parsed = new Date(`${normalized}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`${fieldName} is invalid.`);
  }
  return normalized;
}

function round(value, digits = 2) {
  const factor = 10 ** digits;
  return Math.round((Number(value) + Number.EPSILON) * factor) / factor;
}

function normalizeTags(tags) {
  if (!tags) {
    return "";
  }
  if (Array.isArray(tags)) {
    return tags.map((tag) => String(tag).trim()).filter(Boolean).join(",");
  }
  return String(tags)
    .split(",")
    .map((tag) => tag.trim())
    .filter(Boolean)
    .join(",");
}

function periodBounds(period = "all") {
  if (!period || period === "all") {
    return { startDate: null, endDate: null };
  }

  const today = new Date();
  const current = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  const start = new Date(current);

  if (period === "week") {
    const day = (start.getUTCDay() + 6) % 7;
    start.setUTCDate(start.getUTCDate() - day);
  } else if (period === "month") {
    start.setUTCDate(1);
  } else if (period === "year") {
    start.setUTCMonth(0, 1);
  }

  return {
    startDate: period === "day" ? current.toISOString().slice(0, 10) : start.toISOString().slice(0, 10),
    endDate: current.toISOString().slice(0, 10),
  };
}

function buildLedgerFilters(input) {
  const clauses = [];
  const values = [];

  const bounds = periodBounds(input.period ?? "all");
  const startDate = input.startDate ? ensureIsoDate(input.startDate, "startDate") : bounds.startDate;
  const endDate = input.endDate ? ensureIsoDate(input.endDate, "endDate") : bounds.endDate;

  if (input.entryType) {
    clauses.push("entry_type = ?");
    values.push(input.entryType);
  }
  if (input.category) {
    clauses.push("category = ?");
    values.push(input.category);
  }
  if (startDate) {
    clauses.push("entry_date >= ?");
    values.push(startDate);
  }
  if (endDate) {
    clauses.push("entry_date <= ?");
    values.push(endDate);
  }

  return {
    whereClause: clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "",
    values,
    startDate,
    endDate,
  };
}

function listLedgerEntriesInternal(db, input) {
  const { whereClause, values } = buildLedgerFilters(input);
  const limit = Math.max(1, Math.min(Number(input.limit ?? 20), 100));
  return db
    .prepare(
      `
      SELECT id, entry_date, entry_type, category, amount, description, tags
      FROM ledger_entries
      ${whereClause}
      ORDER BY entry_date DESC, id DESC
      LIMIT ?
      `,
    )
    .all(...values, limit);
}

function ledgerSummary(rows) {
  return rows.reduce(
    (summary, row) => {
      if (row.entry_type === "income") {
        summary.totalIncome += Number(row.amount);
      } else {
        summary.totalExpense += Number(row.amount);
      }
      return summary;
    },
    { totalIncome: 0, totalExpense: 0 },
  );
}

function computePositions(db, symbolFilter = null) {
  const tradeRows = symbolFilter
    ? db
        .prepare(
          `
          SELECT trade_type, symbol, asset_class, market, quantity, unit_price, fee, trade_date
          FROM investment_trades
          WHERE symbol = ?
          ORDER BY trade_date ASC, id ASC
          `,
        )
        .all(String(symbolFilter).toUpperCase())
    : db
        .prepare(
          `
          SELECT trade_type, symbol, asset_class, market, quantity, unit_price, fee, trade_date
          FROM investment_trades
          ORDER BY trade_date ASC, id ASC
          `,
        )
        .all();

  const dividendRows = symbolFilter
    ? db
        .prepare(
          `
          SELECT symbol, dividend_type, amount, quantity, payout_date
          FROM investment_dividends
          WHERE symbol = ?
          ORDER BY payout_date ASC, id ASC
          `,
        )
        .all(String(symbolFilter).toUpperCase())
    : db
        .prepare(
          `
          SELECT symbol, dividend_type, amount, quantity, payout_date
          FROM investment_dividends
          ORDER BY payout_date ASC, id ASC
          `,
        )
        .all();

  const priceRows = db.prepare("SELECT symbol, price, currency, as_of FROM market_prices").all();
  const state = new Map();

  const ensureState = (symbol, assetClass = "unknown", market = "") => {
    if (!state.has(symbol)) {
      state.set(symbol, {
        symbol,
        assetClass,
        market,
        quantity: 0,
        remainingCost: 0,
        capitalIn: 0,
        realizedPnl: 0,
        dividends: 0,
      });
    }
    return state.get(symbol);
  };

  for (const row of tradeRows) {
    const position = ensureState(row.symbol, row.asset_class, row.market);
    const quantity = Number(row.quantity);
    const unitPrice = Number(row.unit_price);
    const fee = Number(row.fee);

    if (row.trade_type === "buy") {
      position.quantity += quantity;
      position.remainingCost += quantity * unitPrice + fee;
      position.capitalIn += quantity * unitPrice + fee;
      continue;
    }

    if (quantity > position.quantity) {
      throw new Error(`Sell quantity for ${row.symbol} exceeds current holdings.`);
    }

    const averageCost = position.quantity === 0 ? 0 : position.remainingCost / position.quantity;
    const costBasis = averageCost * quantity;
    const proceeds = quantity * unitPrice - fee;
    position.realizedPnl += proceeds - costBasis;
    position.quantity -= quantity;
    position.remainingCost -= costBasis;
  }

  for (const row of dividendRows) {
    const position = ensureState(row.symbol);
    if (row.dividend_type === "cash") {
      position.dividends += Number(row.amount);
    } else {
      position.quantity += Number(row.quantity);
    }
  }

  const prices = new Map(priceRows.map((row) => [row.symbol, row]));

  return [...state.values()]
    .map((position) => {
      const priceInfo = prices.get(position.symbol);
      const price = priceInfo ? Number(priceInfo.price) : null;
      const marketValue = position.quantity === 0 ? 0 : price === null ? null : position.quantity * price;
      const unrealizedPnl = marketValue === null ? null : marketValue - position.remainingCost;
      const totalReturn = unrealizedPnl === null ? null : unrealizedPnl + position.realizedPnl + position.dividends;
      const roiPct = totalReturn === null || position.capitalIn <= 0 ? null : (totalReturn / position.capitalIn) * 100;

      return {
        symbol: position.symbol,
        assetClass: position.assetClass,
        market: position.market,
        quantity: round(position.quantity, 6),
        remainingCost: round(position.remainingCost),
        capitalIn: round(position.capitalIn),
        realizedPnl: round(position.realizedPnl),
        dividends: round(position.dividends),
        price: price === null ? null : round(price, 6),
        currency: priceInfo?.currency ?? "",
        priceDate: priceInfo?.as_of ?? null,
        marketValue: marketValue === null ? null : round(marketValue),
        unrealizedPnl: unrealizedPnl === null ? null : round(unrealizedPnl),
        totalReturn: totalReturn === null ? null : round(totalReturn),
        roiPct: roiPct === null ? null : round(roiPct),
      };
    })
    .sort((left, right) => left.symbol.localeCompare(right.symbol));
}

function loadTrackedSymbols(db, { symbol = null, scope = "open_positions" } = {}) {
  if (symbol) {
    const normalized = String(symbol).trim().toUpperCase();
    const trade = db
      .prepare(
        `
        SELECT symbol, asset_class, market
        FROM investment_trades
        WHERE symbol = ?
        ORDER BY trade_date DESC, id DESC
        LIMIT 1
        `,
      )
      .get(normalized);

    return [
      {
        symbol: normalized,
        assetClass: trade?.asset_class ?? "",
        market: trade?.market ?? "",
      },
    ];
  }

  if (scope === "all_symbols") {
    return db
      .prepare(
        `
        SELECT
          symbol,
          COALESCE(
            (
              SELECT asset_class
              FROM investment_trades AS latest_trade
              WHERE latest_trade.symbol = symbols.symbol
              ORDER BY latest_trade.trade_date DESC, latest_trade.id DESC
              LIMIT 1
            ),
            ''
          ) AS asset_class,
          COALESCE(
            (
              SELECT market
              FROM investment_trades AS latest_trade
              WHERE latest_trade.symbol = symbols.symbol
              ORDER BY latest_trade.trade_date DESC, latest_trade.id DESC
              LIMIT 1
            ),
            ''
          ) AS market
        FROM (
          SELECT DISTINCT symbol FROM investment_trades
          UNION
          SELECT DISTINCT symbol FROM investment_dividends
        ) AS symbols
        ORDER BY symbol ASC
        `,
      )
      .all()
      .map((row) => ({
        symbol: row.symbol,
        assetClass: row.asset_class,
        market: row.market,
      }));
  }

  return computePositions(db)
    .filter((position) => Number(position.quantity) > 0)
    .map((position) => ({
      symbol: position.symbol,
      assetClass: position.assetClass,
      market: position.market,
    }));
}

const commonText = {
  success: "Operation completed. Summarize the result for the user in Traditional Chinese.",
};

export const TOOL_NAMES = [
  "add_ledger_entry",
  "list_ledger_entries",
  "delete_ledger_entry",
  "get_ledger_report",
  "record_investment_trade",
  "record_dividend",
  "set_market_price",
  "refresh_market_prices",
  "list_investment_activity",
  "get_portfolio_summary",
  "upsert_recurring_investment_plan",
  "list_recurring_investment_plans",
  "run_due_recurring_investments",
];

export const financeTools = [
  defineTool("add_ledger_entry", {
    description: "Create one income or expense ledger entry.",
    parameters: z.object({
      entryType: z.enum(["expense", "income"]),
      amount: z.number().positive(),
      description: z.string().min(1),
      category: z.string().default("uncategorized"),
      tags: z.array(z.string()).optional(),
      entryDate: z.string().optional(),
    }),
    handler: async (input) =>
      withDatabase((db, dbPath) => {
        const entryDate = ensureIsoDate(input.entryDate);
        const tags = normalizeTags(input.tags);
        const result = db
          .prepare(
            `
            INSERT INTO ledger_entries (entry_type, amount, description, category, tags, entry_date)
            VALUES (?, ?, ?, ?, ?, ?)
            `,
          )
          .run(input.entryType, input.amount, input.description, input.category, tags, entryDate);

        return {
          message: commonText.success,
          dbPath,
          entry: {
            id: Number(result.lastInsertRowid),
            entryType: input.entryType,
            amount: round(input.amount),
            description: input.description,
            category: input.category,
            tags: tags ? tags.split(",") : [],
            entryDate,
          },
        };
      }),
  }),
  defineTool("list_ledger_entries", {
    description: "List ledger entries with optional date, type, and category filters.",
    parameters: z.object({
      period: z.enum(PERIOD_VALUES).optional(),
      startDate: z.string().optional(),
      endDate: z.string().optional(),
      entryType: z.enum(["expense", "income"]).optional(),
      category: z.string().optional(),
      limit: z.number().int().positive().max(100).optional(),
    }),
    handler: async (input) =>
      withDatabase((db) => {
        const rows = listLedgerEntriesInternal(db, input);
        const summary = ledgerSummary(rows);
        return {
          message: commonText.success,
          count: rows.length,
          totalIncome: round(summary.totalIncome),
          totalExpense: round(summary.totalExpense),
          net: round(summary.totalIncome - summary.totalExpense),
          entries: rows.map((row) => ({
            id: row.id,
            date: row.entry_date,
            entryType: row.entry_type,
            category: row.category,
            amount: round(row.amount),
            description: row.description,
            tags: row.tags ? String(row.tags).split(",").filter(Boolean) : [],
          })),
        };
      }),
  }),
  defineTool("delete_ledger_entry", {
    description: "Delete one ledger entry by id.",
    parameters: z.object({
      entryId: z.number().int().positive(),
    }),
    handler: async ({ entryId }) =>
      withDatabase((db) => {
        const existing = db
          .prepare("SELECT id, entry_type, amount, description FROM ledger_entries WHERE id = ?")
          .get(entryId);
        if (!existing) {
          throw new Error(`Ledger entry ${entryId} was not found.`);
        }
        db.prepare("DELETE FROM ledger_entries WHERE id = ?").run(entryId);
        return {
          message: commonText.success,
          deleted: {
            id: existing.id,
            entryType: existing.entry_type,
            amount: round(existing.amount),
            description: existing.description,
          },
        };
      }),
  }),
  defineTool("get_ledger_report", {
    description: "Get ledger totals and category breakdowns.",
    parameters: z.object({
      period: z.enum(PERIOD_VALUES).optional(),
      startDate: z.string().optional(),
      endDate: z.string().optional(),
    }),
    handler: async (input) =>
      withDatabase((db) => {
        const { whereClause, values, startDate, endDate } = buildLedgerFilters(input);
        const summary = db
          .prepare(
            `
            SELECT
              COALESCE(SUM(CASE WHEN entry_type = 'income' THEN amount ELSE 0 END), 0) AS total_income,
              COALESCE(SUM(CASE WHEN entry_type = 'expense' THEN amount ELSE 0 END), 0) AS total_expense
            FROM ledger_entries
            ${whereClause}
            `,
          )
          .get(...values);
        const categories = db
          .prepare(
            `
            SELECT
              category,
              COALESCE(SUM(CASE WHEN entry_type = 'income' THEN amount ELSE 0 END), 0) AS income,
              COALESCE(SUM(CASE WHEN entry_type = 'expense' THEN amount ELSE 0 END), 0) AS expense
            FROM ledger_entries
            ${whereClause}
            GROUP BY category
            ORDER BY expense DESC, income DESC, category ASC
            `,
          )
          .all(...values);

        return {
          message: commonText.success,
          range: {
            startDate: startDate ?? null,
            endDate: endDate ?? null,
          },
          totalIncome: round(summary.total_income),
          totalExpense: round(summary.total_expense),
          net: round(summary.total_income - summary.total_expense),
          categories: categories.map((row) => ({
            category: row.category,
            income: round(row.income),
            expense: round(row.expense),
            net: round(row.income - row.expense),
          })),
        };
      }),
  }),
  defineTool("record_investment_trade", {
    description: "Record one buy or sell investment trade.",
    parameters: z.object({
      tradeType: z.enum(["buy", "sell"]),
      symbol: z.string().min(1),
      quantity: z.number().positive(),
      unitPrice: z.number().nonnegative(),
      fee: z.number().nonnegative().default(0),
      tradeDate: z.string().optional(),
      assetClass: z.string().default("stock"),
      market: z.string().default(""),
      note: z.string().default(""),
    }),
    handler: async (input) =>
      withDatabase((db) => {
        const symbol = input.symbol.toUpperCase();
        if (input.tradeType === "sell") {
          const positions = computePositions(db, symbol);
          const current = positions[0];
          const availableQuantity = current ? current.quantity : 0;
          if (input.quantity > availableQuantity) {
            throw new Error(`Cannot sell ${input.quantity} units of ${symbol}; only ${availableQuantity} available.`);
          }
        }

        const tradeDate = ensureIsoDate(input.tradeDate);
        const result = db
          .prepare(
            `
            INSERT INTO investment_trades (
              trade_type, symbol, asset_class, market, quantity, unit_price, fee, trade_date, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `,
          )
          .run(
            input.tradeType,
            symbol,
            input.assetClass,
            input.market.toUpperCase(),
            input.quantity,
            input.unitPrice,
            input.fee,
            tradeDate,
            input.note,
          );

        return {
          message: commonText.success,
          trade: {
            id: Number(result.lastInsertRowid),
            tradeType: input.tradeType,
            symbol,
            quantity: round(input.quantity, 6),
            unitPrice: round(input.unitPrice),
            fee: round(input.fee),
            tradeDate,
            assetClass: input.assetClass,
            market: input.market.toUpperCase(),
            note: input.note,
          },
        };
      }),
  }),
  defineTool("record_dividend", {
    description: "Record one cash or stock dividend.",
    parameters: z.object({
      symbol: z.string().min(1),
      dividendType: z.enum(["cash", "stock"]).default("cash"),
      amount: z.number().nonnegative().default(0),
      quantity: z.number().nonnegative().default(0),
      payoutDate: z.string().optional(),
      note: z.string().default(""),
    }),
    handler: async (input) =>
      withDatabase((db) => {
        if (input.dividendType === "stock" && input.quantity <= 0) {
          throw new Error("Stock dividend requires quantity greater than 0.");
        }

        const payoutDate = ensureIsoDate(input.payoutDate);
        const symbol = input.symbol.toUpperCase();
        const result = db
          .prepare(
            `
            INSERT INTO investment_dividends (symbol, dividend_type, amount, quantity, payout_date, note)
            VALUES (?, ?, ?, ?, ?, ?)
            `,
          )
          .run(symbol, input.dividendType, input.amount, input.quantity, payoutDate, input.note);

        return {
          message: commonText.success,
          dividend: {
            id: Number(result.lastInsertRowid),
            symbol,
            dividendType: input.dividendType,
            amount: round(input.amount),
            quantity: round(input.quantity, 6),
            payoutDate,
            note: input.note,
          },
        };
      }),
  }),
  defineTool("set_market_price", {
    description: "Update the latest stored price for one asset.",
    parameters: z.object({
      symbol: z.string().min(1),
      price: z.number().nonnegative(),
      currency: z.string().default(""),
      source: z.string().default("manual"),
      asOf: z.string().optional(),
    }),
    handler: async (input) =>
      withDatabase((db) => {
        const symbol = input.symbol.toUpperCase();
        const asOf = ensureIsoDate(input.asOf);
        db
          .prepare(
            `
            INSERT INTO market_prices (symbol, price, currency, source, as_of, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
              price = excluded.price,
              currency = excluded.currency,
              source = excluded.source,
              as_of = excluded.as_of,
              updated_at = CURRENT_TIMESTAMP
            `,
          )
          .run(symbol, input.price, input.currency.toUpperCase(), input.source, asOf);

        return {
          message: commonText.success,
          price: {
            symbol,
            price: round(input.price, 6),
            currency: input.currency.toUpperCase(),
            source: input.source,
            asOf,
          },
        };
      }),
  }),
  defineTool("refresh_market_prices", {
    description: "Fetch and store the latest BTC market price from Binance for BTC symbols in investment records.",
    parameters: z.object({
      symbol: z.string().optional(),
    }),
    handler: async (input) => {
      const requestedSymbol = input.symbol?.trim().toUpperCase();
      if (requestedSymbol && !["BTC", "BTCUSDT"].includes(requestedSymbol)) {
        throw new Error("Automatic market price refresh currently supports BTC only.");
      }

      const targets = withDatabase((db) => {
        if (requestedSymbol) {
          return loadTrackedSymbols(db, { symbol: requestedSymbol });
        }

        return loadTrackedSymbols(db, { scope: "all_symbols" }).filter((item) => ["BTC", "BTCUSDT"].includes(item.symbol));
      });

      if (targets.length === 0) {
        return {
          message: commonText.success,
          updated: [],
          failed: [],
          scope: requestedSymbol ? "single_symbol" : "tracked_btc_symbols",
        };
      }

      const settled = await Promise.all(
        targets.map(async (target) => {
          try {
            const quote = await fetchLatestMarketPrice(target);
            return { ok: true, quote };
          } catch (error) {
            return {
              ok: false,
              symbol: target.symbol,
              error: error instanceof Error ? error.message : String(error),
            };
          }
        }),
      );

      const updated = settled.filter((item) => item.ok).map((item) => item.quote);
      const failed = settled.filter((item) => !item.ok);

      withDatabase((db) => {
        const statement = db.prepare(
          `
          INSERT INTO market_prices (symbol, price, currency, source, as_of, updated_at)
          VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
          ON CONFLICT(symbol) DO UPDATE SET
            price = excluded.price,
            currency = excluded.currency,
            source = excluded.source,
            as_of = excluded.as_of,
            updated_at = CURRENT_TIMESTAMP
          `,
        );

        for (const quote of updated) {
          statement.run(quote.symbol, quote.price, quote.currency, quote.source, quote.asOf);
        }
      });

      return {
        message: commonText.success,
        scope: requestedSymbol ? "single_symbol" : "tracked_btc_symbols",
        updated: updated.map((quote) => ({
          symbol: quote.symbol,
          resolvedSymbol: quote.resolvedSymbol,
          price: round(quote.price, 6),
          currency: quote.currency,
          source: quote.source,
          asOf: quote.asOf,
        })),
        failed: failed.map((item) => ({
          symbol: item.symbol,
          error: item.error,
        })),
      };
    },
  }),
  defineTool("list_investment_activity", {
    description: "List trades and dividend history.",
    parameters: z.object({
      symbol: z.string().optional(),
      limit: z.number().int().positive().max(100).default(20),
    }),
    handler: async (input) =>
      withDatabase((db) => {
        const limit = input.limit ?? 20;
        const symbol = input.symbol?.toUpperCase();

        const trades = symbol
          ? db
              .prepare(
                `
                SELECT id, trade_date, trade_type, symbol, asset_class, market, quantity, unit_price, fee, note
                FROM investment_trades
                WHERE symbol = ?
                ORDER BY trade_date DESC, id DESC
                LIMIT ?
                `,
              )
              .all(symbol, limit)
          : db
              .prepare(
                `
                SELECT id, trade_date, trade_type, symbol, asset_class, market, quantity, unit_price, fee, note
                FROM investment_trades
                ORDER BY trade_date DESC, id DESC
                LIMIT ?
                `,
              )
              .all(limit);

        const dividends = symbol
          ? db
              .prepare(
                `
                SELECT id, payout_date, dividend_type, symbol, amount, quantity, note
                FROM investment_dividends
                WHERE symbol = ?
                ORDER BY payout_date DESC, id DESC
                LIMIT ?
                `,
              )
              .all(symbol, limit)
          : db
              .prepare(
                `
                SELECT id, payout_date, dividend_type, symbol, amount, quantity, note
                FROM investment_dividends
                ORDER BY payout_date DESC, id DESC
                LIMIT ?
                `,
              )
              .all(limit);

        return {
          message: commonText.success,
          trades: trades.map((row) => ({
            id: row.id,
            tradeDate: row.trade_date,
            tradeType: row.trade_type,
            symbol: row.symbol,
            assetClass: row.asset_class,
            market: row.market,
            quantity: round(row.quantity, 6),
            unitPrice: round(row.unit_price),
            fee: round(row.fee),
            note: row.note,
          })),
          dividends: dividends.map((row) => ({
            id: row.id,
            payoutDate: row.payout_date,
            dividendType: row.dividend_type,
            symbol: row.symbol,
            amount: round(row.amount),
            quantity: round(row.quantity, 6),
            note: row.note,
          })),
        };
      }),
  }),
  defineTool("get_portfolio_summary", {
    description: "Get position, market value, pnl, and roi summary.",
    parameters: z.object({}),
    handler: async () =>
      withDatabase((db) => {
        const positions = computePositions(db);
        const missingPrices = positions.filter((item) => item.marketValue === null && item.quantity > 0).map((item) => item.symbol);
        const totals = positions.reduce(
          (accumulator, item) => {
            accumulator.capitalIn += item.capitalIn;
            accumulator.openCost += item.remainingCost;
            accumulator.realized += item.realizedPnl;
            accumulator.dividends += item.dividends;
            if (item.marketValue !== null) {
              accumulator.marketValue += item.marketValue;
            }
            return accumulator;
          },
          { capitalIn: 0, openCost: 0, realized: 0, dividends: 0, marketValue: 0 },
        );

        const complete = missingPrices.length === 0;
        const unrealized = complete ? totals.marketValue - totals.openCost : null;
        const totalReturn = complete ? unrealized + totals.realized + totals.dividends : null;
        const roiPct = complete && totals.capitalIn > 0 ? (totalReturn / totals.capitalIn) * 100 : null;

        return {
          message: commonText.success,
          positions,
          totals: {
            capitalIn: round(totals.capitalIn),
            openCost: round(totals.openCost),
            realized: round(totals.realized),
            dividends: round(totals.dividends),
            marketValue: complete ? round(totals.marketValue) : null,
            unrealized: unrealized === null ? null : round(unrealized),
            totalReturn: totalReturn === null ? null : round(totalReturn),
            roiPct: roiPct === null ? null : round(roiPct),
            missingPrices,
          },
        };
      }),
  }),
  defineTool("upsert_recurring_investment_plan", {
    description: "Create or update one recurring daily BTC investment plan.",
    parameters: z.object({
      symbol: z.string().min(1),
      budgetAmount: z.number().positive(),
      quoteCurrency: z.string().default("USDT"),
      scheduleType: z.enum(["daily"]).default("daily"),
      runTime: z.string(),
      timeZone: z.string().default("Asia/Taipei"),
      assetClass: z.string().default("crypto"),
      market: z.string().default("BINANCE"),
      priceSource: z.string().default("binance"),
      note: z.string().default(""),
      enabled: z.boolean().default(true),
    }),
    handler: async (input) => {
      const result = upsertRecurringInvestmentPlan(input);
      return {
        message: commonText.success,
        dbPath: result.dbPath,
        plan: result.plan,
      };
    },
  }),
  defineTool("list_recurring_investment_plans", {
    description: "List recurring investment plans and the latest execution status.",
    parameters: z.object({
      enabled: z.boolean().optional(),
    }),
    handler: async (input) => ({
      message: commonText.success,
      plans: listRecurringInvestmentPlans(input),
    }),
  }),
  defineTool("run_due_recurring_investments", {
    description: "Execute recurring investment plans that are already due at the current time.",
    parameters: z.object({}),
    handler: async () => {
      const result = await runDueRecurringInvestmentPlans();
      return {
        message: commonText.success,
        dbPath: result.dbPath,
        executed: result.executed,
        skipped: result.skipped,
        failed: result.failed,
      };
    },
  }),
];
