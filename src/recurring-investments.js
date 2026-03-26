import { openDatabase } from "./db.js";
import { fetchLatestMarketPrice } from "./market-data.js";

const DEFAULT_TIMEZONE = "Asia/Taipei";

function round(value, digits = 2) {
  const factor = 10 ** digits;
  return Math.round((Number(value) + Number.EPSILON) * factor) / factor;
}

function normalizeSymbol(raw) {
  const symbol = String(raw ?? "").trim().toUpperCase();
  if (symbol === "BTC" || symbol === "BTCUSDT") {
    return "BTC";
  }
  throw new Error("Recurring investment currently supports BTC only.");
}

function normalizeQuoteCurrency(raw) {
  const currency = String(raw ?? "USDT").trim().toUpperCase();
  if (currency !== "USDT") {
    throw new Error("Recurring investment currently supports USDT budget only.");
  }
  return currency;
}

function normalizeRunTime(raw) {
  const text = String(raw ?? "").trim();
  const match = /^(\d{1,2}):(\d{2})$/.exec(text);
  if (!match) {
    throw new Error("runTime must use HH:MM in 24-hour format.");
  }

  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (hours > 23 || minutes > 59) {
    throw new Error("runTime is invalid.");
  }

  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
}

function normalizeTimezone(raw) {
  const timeZone = String(raw ?? DEFAULT_TIMEZONE).trim() || DEFAULT_TIMEZONE;
  try {
    new Intl.DateTimeFormat("en-CA", { timeZone }).format(new Date());
  } catch {
    throw new Error(`Unsupported timezone: ${timeZone}`);
  }
  return timeZone;
}

function getZonedDateTime(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  });

  const parts = Object.fromEntries(
    formatter
      .formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );

  return {
    date: `${parts.year}-${parts.month}-${parts.day}`,
    time: `${parts.hour}:${parts.minute}`,
  };
}

function normalizePlanInput(input) {
  const budgetAmount = Number(input.budgetAmount);
  if (!Number.isFinite(budgetAmount) || budgetAmount <= 0) {
    throw new Error("budgetAmount must be greater than 0.");
  }

  const scheduleType = String(input.scheduleType ?? "daily").trim().toLowerCase();
  if (scheduleType !== "daily") {
    throw new Error("Recurring investment currently supports daily schedule only.");
  }

  return {
    symbol: normalizeSymbol(input.symbol),
    quoteCurrency: normalizeQuoteCurrency(input.quoteCurrency),
    budgetAmount: round(budgetAmount),
    scheduleType,
    runTime: normalizeRunTime(input.runTime),
    timeZone: normalizeTimezone(input.timeZone),
    assetClass: String(input.assetClass ?? "crypto").trim() || "crypto",
    market: String(input.market ?? "BINANCE").trim().toUpperCase() || "BINANCE",
    priceSource: String(input.priceSource ?? "binance").trim().toLowerCase() || "binance",
    note: String(input.note ?? "").trim(),
    enabled: input.enabled === undefined ? true : Boolean(input.enabled),
  };
}

function buildTradeNote(plan) {
  const fragments = [
    `Auto DCA plan #${plan.id}`,
    `${plan.schedule_type} ${plan.run_time} ${plan.timezone}`,
    `budget ${round(plan.budget_amount)} ${plan.quote_currency}`,
  ];
  if (plan.note) {
    fragments.push(plan.note);
  }
  return fragments.join(" | ");
}

async function fetchPlanQuote(plan, fetchQuote) {
  if (plan.price_source !== "binance") {
    throw new Error(`Unsupported price source: ${plan.price_source}`);
  }

  const quote = await fetchQuote({ symbol: plan.symbol });
  return {
    ...quote,
    symbol: plan.symbol,
    currency: normalizeQuoteCurrency(quote.currency),
    price: Number(quote.price),
  };
}

function mapPlanRow(row) {
  return {
    id: row.id,
    symbol: row.symbol,
    quoteCurrency: row.quote_currency,
    budgetAmount: round(row.budget_amount),
    scheduleType: row.schedule_type,
    runTime: row.run_time,
    timeZone: row.timezone,
    assetClass: row.asset_class,
    market: row.market,
    priceSource: row.price_source,
    note: row.note,
    enabled: Boolean(row.is_enabled),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastScheduledFor: row.last_scheduled_for ?? null,
    lastExecutedAt: row.last_executed_at ?? null,
    lastTradeId: row.last_trade_id ?? null,
  };
}

export function upsertRecurringInvestmentPlan(input) {
  const plan = normalizePlanInput(input);
  const { db, dbPath } = openDatabase();

  try {
    db.prepare(
      `
      INSERT INTO recurring_investment_plans (
        symbol, quote_currency, budget_amount, schedule_type, run_time, timezone,
        asset_class, market, price_source, note, is_enabled, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(symbol, quote_currency, schedule_type, run_time, timezone) DO UPDATE SET
        budget_amount = excluded.budget_amount,
        asset_class = excluded.asset_class,
        market = excluded.market,
        price_source = excluded.price_source,
        note = excluded.note,
        is_enabled = excluded.is_enabled,
        updated_at = CURRENT_TIMESTAMP
      `,
    ).run(
      plan.symbol,
      plan.quoteCurrency,
      plan.budgetAmount,
      plan.scheduleType,
      plan.runTime,
      plan.timeZone,
      plan.assetClass,
      plan.market,
      plan.priceSource,
      plan.note,
      plan.enabled ? 1 : 0,
    );

    const stored = db
      .prepare(
        `
        SELECT
          plan.*,
          latest_run.scheduled_for AS last_scheduled_for,
          latest_run.executed_at AS last_executed_at,
          latest_run.trade_id AS last_trade_id
        FROM recurring_investment_plans AS plan
        LEFT JOIN recurring_investment_runs AS latest_run
          ON latest_run.id = (
            SELECT id
            FROM recurring_investment_runs
            WHERE plan_id = plan.id
            ORDER BY scheduled_for DESC, id DESC
            LIMIT 1
          )
        WHERE
          plan.symbol = ?
          AND plan.quote_currency = ?
          AND plan.schedule_type = ?
          AND plan.run_time = ?
          AND plan.timezone = ?
        `,
      )
      .get(plan.symbol, plan.quoteCurrency, plan.scheduleType, plan.runTime, plan.timeZone);

    return {
      dbPath,
      plan: mapPlanRow(stored),
    };
  } finally {
    db.close();
  }
}

export function listRecurringInvestmentPlans({ enabled } = {}) {
  const { db } = openDatabase();

  try {
    let whereClause = "";
    const values = [];
    if (enabled !== undefined) {
      whereClause = "WHERE plan.is_enabled = ?";
      values.push(enabled ? 1 : 0);
    }

    const rows = db
      .prepare(
        `
        SELECT
          plan.*,
          latest_run.scheduled_for AS last_scheduled_for,
          latest_run.executed_at AS last_executed_at,
          latest_run.trade_id AS last_trade_id
        FROM recurring_investment_plans AS plan
        LEFT JOIN recurring_investment_runs AS latest_run
          ON latest_run.id = (
            SELECT id
            FROM recurring_investment_runs
            WHERE plan_id = plan.id
            ORDER BY scheduled_for DESC, id DESC
            LIMIT 1
          )
        ${whereClause}
        ORDER BY plan.is_enabled DESC, plan.run_time ASC, plan.id ASC
        `,
      )
      .all(...values);

    return rows.map(mapPlanRow);
  } finally {
    db.close();
  }
}

export async function runDueRecurringInvestmentPlans(options = {}) {
  const now = options.now instanceof Date ? options.now : new Date();
  const fetchQuote = options.fetchQuote ?? fetchLatestMarketPrice;
  const { db, dbPath } = openDatabase();

  try {
    const plans = db
      .prepare(
        `
        SELECT *
        FROM recurring_investment_plans
        WHERE is_enabled = 1
        ORDER BY run_time ASC, id ASC
        `,
      )
      .all();

    const executed = [];
    const skipped = [];
    const failed = [];

    for (const plan of plans) {
      const zonedNow = getZonedDateTime(now, plan.timezone);
      if (zonedNow.time < plan.run_time) {
        skipped.push({
          planId: plan.id,
          symbol: plan.symbol,
          scheduledFor: `${zonedNow.date}T${plan.run_time}:00`,
          reason: "not_due_yet",
        });
        continue;
      }

      const scheduledFor = `${zonedNow.date}T${plan.run_time}:00`;
      const existingRun = db
        .prepare(
          `
          SELECT id, trade_id
          FROM recurring_investment_runs
          WHERE plan_id = ? AND scheduled_for = ?
          `,
        )
        .get(plan.id, scheduledFor);

      if (existingRun) {
        skipped.push({
          planId: plan.id,
          symbol: plan.symbol,
          scheduledFor,
          tradeId: existingRun.trade_id ?? null,
          reason: "already_executed",
        });
        continue;
      }

      try {
        const quote = await fetchPlanQuote(plan, fetchQuote);
        if (!Number.isFinite(quote.price) || quote.price <= 0) {
          throw new Error(`Invalid market price for ${plan.symbol}.`);
        }

        const quantity = round(plan.budget_amount / quote.price, 8);
        if (quantity <= 0) {
          throw new Error(`Calculated quantity for ${plan.symbol} is zero.`);
        }

        const note = buildTradeNote(plan);
        let runId;
        let tradeId;

        db.exec("BEGIN IMMEDIATE");
        try {
          const duplicate = db
            .prepare(
              `
              SELECT id
              FROM recurring_investment_runs
              WHERE plan_id = ? AND scheduled_for = ?
              `,
            )
            .get(plan.id, scheduledFor);

          if (duplicate) {
            db.exec("ROLLBACK");
            skipped.push({
              planId: plan.id,
              symbol: plan.symbol,
              scheduledFor,
              reason: "already_executed",
            });
            continue;
          }

          const runResult = db
            .prepare(
              `
              INSERT INTO recurring_investment_runs (
                plan_id, scheduled_for, status, symbol, quote_currency, budget_amount,
                price, quantity, source, message
              )
              VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
              `,
            )
            .run(
              plan.id,
              scheduledFor,
              plan.symbol,
              plan.quote_currency,
              plan.budget_amount,
              quote.price,
              quantity,
              quote.source,
              note,
            );
          runId = Number(runResult.lastInsertRowid);

          const tradeResult = db
            .prepare(
              `
              INSERT INTO investment_trades (
                trade_type, symbol, asset_class, market, quantity, unit_price, fee, trade_date, note
              )
              VALUES ('buy', ?, ?, ?, ?, ?, 0, ?, ?)
              `,
            )
            .run(plan.symbol, plan.asset_class, plan.market, quantity, quote.price, zonedNow.date, note);
          tradeId = Number(tradeResult.lastInsertRowid);

          db.prepare(
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
          ).run(plan.symbol, quote.price, quote.currency, quote.source, quote.asOf ?? zonedNow.date);

          db.prepare(
            `
            UPDATE recurring_investment_runs
            SET status = 'success', trade_id = ?, executed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            `,
          ).run(tradeId, runId);

          db.exec("COMMIT");
        } catch (error) {
          db.exec("ROLLBACK");
          throw error;
        }

        executed.push({
          planId: plan.id,
          tradeId,
          symbol: plan.symbol,
          scheduledFor,
          tradeDate: zonedNow.date,
          price: round(quote.price, 6),
          quantity,
          budgetAmount: round(plan.budget_amount),
          quoteCurrency: plan.quote_currency,
          source: quote.source,
        });
      } catch (error) {
        failed.push({
          planId: plan.id,
          symbol: plan.symbol,
          scheduledFor,
          reason: error instanceof Error ? error.message : String(error),
        });
      }
    }

    return {
      dbPath,
      now: now.toISOString(),
      executed,
      skipped,
      failed,
    };
  } finally {
    db.close();
  }
}
