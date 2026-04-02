import { openDatabase, resolveDbPath, withDatabase } from "./db.js";
import { fetchBtcRealtimeSnapshot, fetchLatestMarketPrice } from "./market-data.js";
import {
  listRecurringInvestmentPlans,
  runDueRecurringInvestmentPlans,
  upsertRecurringInvestmentPlan,
} from "./recurring-investments.js";
const PERIOD_VALUES = ["day", "week", "month", "year", "all"];
const LEDGER_CATEGORY_VALUES = ["餐飲", "工作", "投資", "娛樂", "日用", "交通"];
const RECURRING_LEDGER_LOCK_KEY = "recurring-ledger-runner";
const RECURRING_LEDGER_LOCK_TTL_MS = 5 * 60 * 1000;

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

function isoNow() {
  return new Date().toISOString();
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

function normalizeLedgerCategory(category) {
  const normalized = String(category ?? "日用").trim() || "日用";
  if (!LEDGER_CATEGORY_VALUES.includes(normalized)) {
    throw new Error(`Unsupported category: ${normalized}`);
  }
  return normalized;
}

function normalizeRunTime(raw, fieldName = "runTime") {
  const normalized = String(raw ?? "").trim();
  const match = /^(\d{1,2}):(\d{2})$/.exec(normalized);
  if (!match) {
    throw new Error(`${fieldName} must use HH:MM in 24-hour format.`);
  }

  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (hours > 23 || minutes > 59) {
    throw new Error(`${fieldName} is invalid.`);
  }

  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
}

function normalizeTimeZone(raw) {
  const normalized = String(raw ?? "Asia/Taipei").trim() || "Asia/Taipei";
  try {
    new Intl.DateTimeFormat("en-CA", { timeZone: normalized }).format(new Date());
  } catch {
    throw new Error(`Unsupported timezone: ${normalized}`);
  }
  return normalized;
}

function getTimeZoneDateParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  });

  return Object.fromEntries(
    formatter
      .formatToParts(date)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
}

function getMonthlyNextDate(baseDate, targetDayOfMonth) {
  const year = baseDate.getUTCFullYear();
  const month = baseDate.getUTCMonth();
  const thisMonthLastDay = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
  const thisMonthDay = Math.min(targetDayOfMonth, thisMonthLastDay);
  const currentMonthCandidate = new Date(Date.UTC(year, month, thisMonthDay));

  if (currentMonthCandidate >= baseDate) {
    return currentMonthCandidate;
  }

  const nextMonthLastDay = new Date(Date.UTC(year, month + 2, 0)).getUTCDate();
  const nextMonthDay = Math.min(targetDayOfMonth, nextMonthLastDay);
  return new Date(Date.UTC(year, month + 1, nextMonthDay));
}

function computeNextRecurringLedgerOccurrence(plan, now = new Date()) {
  const timeZone = plan.timeZone ?? plan.timezone ?? "Asia/Taipei";
  const parts = getTimeZoneDateParts(now, timeZone);
  const currentDate = new Date(Date.UTC(Number(parts.year), Number(parts.month) - 1, Number(parts.day)));
  const currentTime = `${parts.hour}:${parts.minute}`;
  const startDate = new Date(`${plan.startDate ?? plan.start_date}T00:00:00Z`);
  const baseDate = currentDate < startDate ? startDate : currentDate;

  let targetDate = baseDate;
  if ((plan.scheduleType ?? plan.schedule_type) === "monthly") {
    const requestedDay = Number(plan.dayOfMonth ?? plan.day_of_month ?? 1);
    targetDate = getMonthlyNextDate(baseDate, requestedDay);
  }

  const runTime = plan.runTime ?? plan.run_time;
  if ((plan.scheduleType ?? plan.schedule_type) === "daily" && currentDate.getTime() === targetDate.getTime() && currentTime > runTime) {
    targetDate = new Date(Date.UTC(targetDate.getUTCFullYear(), targetDate.getUTCMonth(), targetDate.getUTCDate() + 1));
  }

  if ((plan.scheduleType ?? plan.schedule_type) === "monthly" && currentDate.getTime() === targetDate.getTime() && currentTime > runTime) {
    targetDate = getMonthlyNextDate(new Date(Date.UTC(targetDate.getUTCFullYear(), targetDate.getUTCMonth(), targetDate.getUTCDate() + 1)), Number(plan.dayOfMonth ?? plan.day_of_month ?? 1));
  }

  const dateText = targetDate.toISOString().slice(0, 10);
  return `${dateText}T${runTime}:00`;
}

function mapRecurringLedgerPlanRow(row) {
  return {
    id: row.id,
    entryType: row.entry_type,
    amount: round(row.amount),
    description: row.description,
    category: row.category,
    scheduleType: row.schedule_type,
    runTime: row.run_time,
    dayOfMonth: row.day_of_month,
    timeZone: row.timezone,
    startDate: row.start_date,
    note: row.note,
    enabled: Boolean(row.is_enabled),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    lastScheduledFor: row.last_scheduled_for ?? null,
    lastExecutedAt: row.last_executed_at ?? null,
    lastStatus: row.last_status ?? null,
    lastLedgerEntryId: row.last_ledger_entry_id ?? null,
    lastMessage: row.last_message ?? null,
  };
}

function acquireTaskLock(db, lockKey, ownerId, ttlMs = RECURRING_LEDGER_LOCK_TTL_MS) {
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

function getDaysInMonth(year, month1Based) {
  return new Date(Date.UTC(year, month1Based, 0)).getUTCDate();
}

function compareDateText(left, right) {
  return left.localeCompare(right);
}

function getRecurringLedgerDueState(plan, now = new Date()) {
  const timeZone = plan.timezone;
  const parts = getTimeZoneDateParts(now, timeZone);
  const currentDate = `${parts.year}-${parts.month}-${parts.day}`;
  const currentTime = `${parts.hour}:${parts.minute}`;
  const startDate = plan.start_date;

  if (compareDateText(currentDate, startDate) < 0) {
    return { due: false, reason: "before_start_date", scheduledFor: `${startDate}T${plan.run_time}:00`, tradeDate: startDate };
  }

  if (plan.schedule_type === "daily") {
    const scheduledFor = `${currentDate}T${plan.run_time}:00`;
    if (currentTime < plan.run_time) {
      return { due: false, reason: "not_due_yet", scheduledFor, tradeDate: currentDate };
    }
    return { due: true, scheduledFor, tradeDate: currentDate };
  }

  const scheduledDay = Math.min(Number(plan.day_of_month ?? 1), getDaysInMonth(Number(parts.year), Number(parts.month)));
  const scheduledDate = `${parts.year}-${parts.month}-${String(scheduledDay).padStart(2, "0")}`;

  if (compareDateText(scheduledDate, startDate) < 0) {
    return { due: false, reason: "before_start_date", scheduledFor: `${scheduledDate}T${plan.run_time}:00`, tradeDate: scheduledDate };
  }

  const scheduledFor = `${scheduledDate}T${plan.run_time}:00`;
  if (compareDateText(currentDate, scheduledDate) < 0) {
    return { due: false, reason: "not_due_yet", scheduledFor, tradeDate: scheduledDate };
  }
  if (currentDate === scheduledDate && currentTime < plan.run_time) {
    return { due: false, reason: "not_due_yet", scheduledFor, tradeDate: scheduledDate };
  }

  return { due: true, scheduledFor, tradeDate: scheduledDate };
}

function buildRecurringLedgerTags(plan) {
  return [`auto`, `plan-${plan.id}`, plan.schedule_type].join(",");
}

function normalizeRecurringLedgerPlanInput(input) {
  const entryType = String(input.entryType ?? "").trim().toLowerCase();
  if (!["expense", "income"].includes(entryType)) {
    throw new Error(`Unsupported entryType: ${entryType}`);
  }

  const amount = Number(input.amount);
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error("amount must be greater than 0.");
  }

  const scheduleType = String(input.scheduleType ?? "daily").trim().toLowerCase();
  if (!["daily", "monthly"].includes(scheduleType)) {
    throw new Error(`Unsupported scheduleType: ${scheduleType}`);
  }

  const dayOfMonth = scheduleType === "monthly"
    ? Number(input.dayOfMonth ?? 1)
    : null;

  if (scheduleType === "monthly" && (!Number.isInteger(dayOfMonth) || dayOfMonth < 1 || dayOfMonth > 31)) {
    throw new Error("dayOfMonth must be an integer between 1 and 31.");
  }

  return {
    entryType,
    amount: round(amount),
    description: String(input.description ?? "").trim(),
    category: normalizeLedgerCategory(input.category),
    scheduleType,
    runTime: normalizeRunTime(input.runTime),
    dayOfMonth,
    timeZone: normalizeTimeZone(input.timeZone),
    startDate: ensureIsoDate(input.startDate, "startDate"),
    note: String(input.note ?? "").trim(),
    enabled: input.enabled === undefined ? true : Boolean(input.enabled),
  };
}

function assertSupportedPeriod(period) {
  if (period && !PERIOD_VALUES.includes(period)) {
    throw new Error(`Unsupported period: ${period}`);
  }
}

function periodBounds(period = "all") {
  assertSupportedPeriod(period);

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

function buildLedgerFilters(input = {}) {
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

function listLedgerEntriesInternal(db, input = {}) {
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

  const prices = new Map(priceRows.map((row) => [row.symbol, row]));

  return [...state.values()]
    .map((position) => {
      const priceInfo = prices.get(position.symbol);
      const price = priceInfo ? Number(priceInfo.price) : null;
      const marketValue = position.quantity === 0 ? 0 : price === null ? null : position.quantity * price;
      const unrealizedPnl = marketValue === null ? null : marketValue - position.remainingCost;
      const totalReturn = unrealizedPnl === null ? null : unrealizedPnl + position.realizedPnl;
      const roiPct = totalReturn === null || position.capitalIn <= 0 ? null : (totalReturn / position.capitalIn) * 100;

      return {
        symbol: position.symbol,
        assetClass: position.assetClass,
        market: position.market,
        quantity: round(position.quantity, 6),
        remainingCost: round(position.remainingCost),
        capitalIn: round(position.capitalIn),
        realizedPnl: round(position.realizedPnl),
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

function upsertMarketPrice(db, { symbol, price, currency = "", source = "manual", asOf }) {
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
  ).run(symbol, price, currency, source, asOf);
}

export function initDatabase() {
  const { db, dbPath } = openDatabase();
  db.close();
  return { dbPath };
}

export function getDatabasePath() {
  return resolveDbPath();
}

export function addLedgerEntry(input) {
  return withDatabase((db, dbPath) => {
    const entryDate = ensureIsoDate(input.entryDate);
    const tags = normalizeTags(input.tags);
    const category = normalizeLedgerCategory(input.category);
    const result = db
      .prepare(
        `
        INSERT INTO ledger_entries (entry_type, amount, description, category, tags, entry_date)
        VALUES (?, ?, ?, ?, ?, ?)
        `,
      )
      .run(input.entryType, Number(input.amount), input.description, category, tags, entryDate);

    return {
      dbPath,
      entry: {
        id: Number(result.lastInsertRowid),
        entryType: input.entryType,
        amount: round(input.amount),
        description: input.description,
        category,
        tags: tags ? tags.split(",") : [],
        entryDate,
      },
    };
  });
}

export function listLedgerEntries(input = {}) {
  return withDatabase((db) => {
    const rows = listLedgerEntriesInternal(db, input);
    const summary = ledgerSummary(rows);

    return {
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
  });
}

export function deleteLedgerEntry({ entryId }) {
  return withDatabase((db) => {
    const existing = db
      .prepare("SELECT id, entry_type, amount, description FROM ledger_entries WHERE id = ?")
      .get(entryId);

    if (!existing) {
      throw new Error(`Ledger entry ${entryId} was not found.`);
    }

    db.prepare("DELETE FROM ledger_entries WHERE id = ?").run(entryId);

    return {
      deleted: {
        id: existing.id,
        entryType: existing.entry_type,
        amount: round(existing.amount),
        description: existing.description,
      },
    };
  });
}

export function createRecurringLedgerPlan(input) {
  return withDatabase((db, dbPath) => {
    const plan = normalizeRecurringLedgerPlanInput(input);
    if (!plan.description) {
      throw new Error("description is required.");
    }

    const result = db
      .prepare(
        `
        INSERT INTO recurring_ledger_plans (
          entry_type, amount, description, category, schedule_type, run_time, day_of_month,
          timezone, start_date, note, is_enabled, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        `,
      )
      .run(
        plan.entryType,
        plan.amount,
        plan.description,
        plan.category,
        plan.scheduleType,
        plan.runTime,
        plan.dayOfMonth,
        plan.timeZone,
        plan.startDate,
        plan.note,
        plan.enabled ? 1 : 0,
      );

    const stored = db.prepare("SELECT * FROM recurring_ledger_plans WHERE id = ?").get(Number(result.lastInsertRowid));
    const mapped = mapRecurringLedgerPlanRow(stored);

    return {
      dbPath,
      plan: {
        ...mapped,
        nextOccurrence: computeNextRecurringLedgerOccurrence(mapped),
      },
    };
  });
}

export function listRecurringLedgerPlans({ enabled } = {}) {
  return withDatabase((db) => {
    const whereClause = enabled === undefined ? "" : "WHERE is_enabled = ?";
    const rows = db
      .prepare(
        `
        SELECT
          plan.*,
          latest_run.scheduled_for AS last_scheduled_for,
          latest_run.executed_at AS last_executed_at,
          latest_run.status AS last_status,
          latest_run.ledger_entry_id AS last_ledger_entry_id,
          latest_run.message AS last_message
        FROM recurring_ledger_plans AS plan
        LEFT JOIN recurring_ledger_runs AS latest_run
          ON latest_run.id = (
            SELECT id
            FROM recurring_ledger_runs
            WHERE plan_id = plan.id
            ORDER BY scheduled_for DESC, id DESC
            LIMIT 1
          )
        ${whereClause}
        ORDER BY plan.is_enabled DESC, plan.schedule_type ASC, plan.run_time ASC, plan.id DESC
        `,
      )
      .all(...(enabled === undefined ? [] : [enabled ? 1 : 0]));

    return rows.map((row) => {
      const mapped = mapRecurringLedgerPlanRow(row);
      return {
        ...mapped,
        nextOccurrence: computeNextRecurringLedgerOccurrence(mapped),
      };
    });
  });
}

export function runDueRecurringLedgerPlans(options = {}) {
  const now = options.now instanceof Date ? options.now : new Date();
  const ownerId = `pid-${process.pid}-${now.getTime()}`;
  const { db, dbPath } = openDatabase();
  let lockAcquired = false;

  try {
    acquireTaskLock(db, RECURRING_LEDGER_LOCK_KEY, ownerId, options.lockTtlMs ?? RECURRING_LEDGER_LOCK_TTL_MS);
    lockAcquired = true;

    const plans = db
      .prepare(
        `
        SELECT *
        FROM recurring_ledger_plans
        WHERE is_enabled = 1
        ORDER BY run_time ASC, id ASC
        `,
      )
      .all();

    const executed = [];
    const skipped = [];
    const failed = [];

    for (const plan of plans) {
      const dueState = getRecurringLedgerDueState(plan, now);
      if (!dueState.due) {
        skipped.push({
          planId: plan.id,
          scheduledFor: dueState.scheduledFor,
          reason: dueState.reason,
        });
        continue;
      }

      const existingRun = db
        .prepare(
          `
          SELECT id, status, ledger_entry_id
          FROM recurring_ledger_runs
          WHERE plan_id = ? AND scheduled_for = ?
          `,
        )
        .get(plan.id, dueState.scheduledFor);

      if (existingRun?.status === "success") {
        skipped.push({
          planId: plan.id,
          scheduledFor: dueState.scheduledFor,
          ledgerEntryId: existingRun.ledger_entry_id ?? null,
          reason: "already_executed",
        });
        continue;
      }

      if (existingRun?.status === "pending") {
        skipped.push({
          planId: plan.id,
          scheduledFor: dueState.scheduledFor,
          reason: "already_processing",
        });
        continue;
      }

      let runId = existingRun?.id ?? null;
      try {
        db.exec("BEGIN IMMEDIATE");
        try {
          if (runId) {
            db.prepare(
              `
              UPDATE recurring_ledger_runs
              SET status = 'pending', message = '', executed_at = CURRENT_TIMESTAMP, ledger_entry_id = NULL
              WHERE id = ?
              `,
            ).run(runId);
          } else {
            const runResult = db
              .prepare(
                `
                INSERT INTO recurring_ledger_runs (
                  plan_id, scheduled_for, status, entry_type, amount, description, category, message
                ) VALUES (?, ?, 'pending', ?, ?, ?, ?, '')
                `,
              )
              .run(
                plan.id,
                dueState.scheduledFor,
                plan.entry_type,
                plan.amount,
                plan.description,
                plan.category,
              );
            runId = Number(runResult.lastInsertRowid);
          }

          const ledgerResult = db
            .prepare(
              `
              INSERT INTO ledger_entries (entry_type, amount, description, category, tags, entry_date)
              VALUES (?, ?, ?, ?, ?, ?)
              `,
            )
            .run(
              plan.entry_type,
              plan.amount,
              plan.description,
              plan.category,
              buildRecurringLedgerTags(plan),
              dueState.tradeDate,
            );

          const ledgerEntryId = Number(ledgerResult.lastInsertRowid);
          const successMessage = plan.note ? `auto_run | ${plan.note}` : "auto_run";

          db.prepare(
            `
            UPDATE recurring_ledger_runs
            SET status = 'success', ledger_entry_id = ?, message = ?, executed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            `,
          ).run(ledgerEntryId, successMessage, runId);

          db.exec("COMMIT");

          executed.push({
            planId: plan.id,
            ledgerEntryId,
            scheduledFor: dueState.scheduledFor,
            tradeDate: dueState.tradeDate,
            entryType: plan.entry_type,
            amount: round(plan.amount),
            category: plan.category,
            description: plan.description,
          });
        } catch (error) {
          db.exec("ROLLBACK");
          throw error;
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (runId) {
          db.prepare(
            `
            UPDATE recurring_ledger_runs
            SET status = 'failed', message = ?, executed_at = CURRENT_TIMESTAMP
            WHERE id = ?
            `,
          ).run(message, runId);
        } else {
          db.prepare(
            `
            INSERT INTO recurring_ledger_runs (
              plan_id, scheduled_for, status, entry_type, amount, description, category, message
            ) VALUES (?, ?, 'failed', ?, ?, ?, ?, ?)
            `,
          ).run(
            plan.id,
            dueState.scheduledFor,
            plan.entry_type,
            plan.amount,
            plan.description,
            plan.category,
            message,
          );
        }

        failed.push({
          planId: plan.id,
          scheduledFor: dueState.scheduledFor,
          reason: message,
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
    if (lockAcquired) {
      releaseTaskLock(db, RECURRING_LEDGER_LOCK_KEY, ownerId);
    }
    db.close();
  }
}

export function deleteRecurringLedgerPlan({ planId }) {
  return withDatabase((db) => {
    const existing = db.prepare("SELECT * FROM recurring_ledger_plans WHERE id = ?").get(planId);
    if (!existing) {
      throw new Error(`Recurring ledger plan ${planId} was not found.`);
    }

    db.prepare("DELETE FROM recurring_ledger_plans WHERE id = ?").run(planId);
    return {
      deleted: {
        id: existing.id,
        description: existing.description,
        entryType: existing.entry_type,
        amount: round(existing.amount),
      },
    };
  });
}

export function getLedgerReport(input = {}) {
  return withDatabase((db) => {
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
  });
}

export function recordInvestmentTrade(input) {
  return withDatabase((db) => {
    const symbol = String(input.symbol).trim().toUpperCase();
    const tradeType = String(input.tradeType).trim().toLowerCase();

    if (!["buy", "sell"].includes(tradeType)) {
      throw new Error(`Unsupported tradeType: ${tradeType}`);
    }

    if (tradeType === "sell") {
      const positions = computePositions(db, symbol);
      const current = positions[0];
      const availableQuantity = current ? current.quantity : 0;
      if (Number(input.quantity) > availableQuantity) {
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
        tradeType,
        symbol,
        input.assetClass ?? "stock",
        String(input.market ?? "").trim().toUpperCase(),
        Number(input.quantity),
        Number(input.unitPrice),
        Number(input.fee ?? 0),
        tradeDate,
        input.note ?? "",
      );

    return {
      trade: {
        id: Number(result.lastInsertRowid),
        tradeType,
        symbol,
        quantity: round(input.quantity, 6),
        unitPrice: round(input.unitPrice),
        fee: round(input.fee ?? 0),
        tradeDate,
        assetClass: input.assetClass ?? "stock",
        market: String(input.market ?? "").trim().toUpperCase(),
        note: input.note ?? "",
      },
    };
  });
}

export function setMarketPrice(input) {
  return withDatabase((db) => {
    const symbol = String(input.symbol).trim().toUpperCase();
    const asOf = ensureIsoDate(input.asOf);
    const price = Number(input.price);

    upsertMarketPrice(db, {
      symbol,
      price,
      currency: String(input.currency ?? "").trim().toUpperCase(),
      source: input.source ?? "manual",
      asOf,
    });

    return {
      price: {
        symbol,
        price: round(price, 6),
        currency: String(input.currency ?? "").trim().toUpperCase(),
        source: input.source ?? "manual",
        asOf,
      },
    };
  });
}

export async function refreshMarketPrices(input = {}) {
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
      scope: requestedSymbol ? "single_symbol" : "tracked_btc_symbols",
      updated: [],
      failed: [],
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
    for (const quote of updated) {
      upsertMarketPrice(db, {
        symbol: quote.symbol,
        price: quote.price,
        currency: quote.currency,
        source: quote.source,
        asOf: quote.asOf,
      });
    }
  });

  return {
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
}

export function listInvestmentActivity(input = {}) {
  return withDatabase((db) => {
    const limit = Math.max(1, Math.min(Number(input.limit ?? 20), 100));
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

    return {
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
    };
  });
}

export function getPortfolioSummary() {
  return withDatabase((db) => {
    const positions = computePositions(db);
    const missingPrices = positions.filter((item) => item.marketValue === null && item.quantity > 0).map((item) => item.symbol);
    const totals = positions.reduce(
      (accumulator, item) => {
        accumulator.capitalIn += item.capitalIn;
        accumulator.openCost += item.remainingCost;
        accumulator.realized += item.realizedPnl;
        if (item.marketValue !== null) {
          accumulator.marketValue += item.marketValue;
        }
        return accumulator;
      },
      { capitalIn: 0, openCost: 0, realized: 0, marketValue: 0 },
    );

    const complete = missingPrices.length === 0;
    const unrealized = complete ? totals.marketValue - totals.openCost : null;
    const totalReturn = complete ? unrealized + totals.realized : null;
    const roiPct = complete && totals.capitalIn > 0 ? (totalReturn / totals.capitalIn) * 100 : null;

    return {
      positions,
      totals: {
        capitalIn: round(totals.capitalIn),
        openCost: round(totals.openCost),
        realized: round(totals.realized),
        marketValue: complete ? round(totals.marketValue) : null,
        unrealized: unrealized === null ? null : round(unrealized),
        totalReturn: totalReturn === null ? null : round(totalReturn),
        roiPct: roiPct === null ? null : round(roiPct),
        missingPrices,
      },
    };
  });
}

export async function getBtcRealtimeInfo({ save = false, symbol = "BTC" } = {}) {
  const snapshot = await fetchBtcRealtimeSnapshot({ symbol });

  if (save) {
    withDatabase((db) => {
      upsertMarketPrice(db, {
        symbol: snapshot.symbol,
        price: snapshot.price,
        currency: snapshot.currency,
        source: snapshot.source,
        asOf: snapshot.asOf,
      });
    });
  }

  return {
    saved: Boolean(save),
    snapshot,
  };
}

export {
  LEDGER_CATEGORY_VALUES,
  PERIOD_VALUES,
  listRecurringInvestmentPlans,
  round,
  runDueRecurringInvestmentPlans,
  upsertRecurringInvestmentPlan,
};
