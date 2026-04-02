import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { DatabaseSync } from "node:sqlite";

export const DEFAULT_DB_PATH = join(homedir(), ".wealth_cli", "wealth.db");

export const SCHEMA = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS ledger_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_type TEXT NOT NULL CHECK (entry_type IN ('expense', 'income')),
    amount REAL NOT NULL CHECK (amount >= 0),
    description TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'uncategorized',
    tags TEXT NOT NULL DEFAULT '',
    entry_date TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

  CREATE TABLE IF NOT EXISTS recurring_ledger_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_type TEXT NOT NULL CHECK (entry_type IN ('expense', 'income')),
    amount REAL NOT NULL CHECK (amount > 0),
    description TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT '日用',
    schedule_type TEXT NOT NULL CHECK (schedule_type IN ('daily', 'monthly')),
    run_time TEXT NOT NULL,
    day_of_month INTEGER CHECK (day_of_month IS NULL OR (day_of_month >= 1 AND day_of_month <= 31)),
    timezone TEXT NOT NULL DEFAULT 'Asia/Taipei',
    start_date TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    is_enabled INTEGER NOT NULL DEFAULT 1 CHECK (is_enabled IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  );

  CREATE INDEX IF NOT EXISTS idx_recurring_ledger_plans_enabled_schedule
  ON recurring_ledger_plans(is_enabled, schedule_type, run_time);

CREATE TABLE IF NOT EXISTS investment_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_type TEXT NOT NULL CHECK (trade_type IN ('buy', 'sell')),
    symbol TEXT NOT NULL,
    asset_class TEXT NOT NULL DEFAULT 'stock',
    market TEXT NOT NULL DEFAULT '',
    quantity REAL NOT NULL CHECK (quantity > 0),
    unit_price REAL NOT NULL CHECK (unit_price >= 0),
    fee REAL NOT NULL DEFAULT 0 CHECK (fee >= 0),
    trade_date TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_prices (
    symbol TEXT PRIMARY KEY,
    price REAL NOT NULL CHECK (price >= 0),
    currency TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'manual',
    as_of TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS market_klines (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  open_time TEXT NOT NULL,
  close_time TEXT NOT NULL,
  open_price REAL NOT NULL CHECK (open_price >= 0),
  high_price REAL NOT NULL CHECK (high_price >= 0),
  low_price REAL NOT NULL CHECK (low_price >= 0),
  close_price REAL NOT NULL CHECK (close_price >= 0),
  volume REAL NOT NULL CHECK (volume >= 0),
  quote_volume REAL NOT NULL CHECK (quote_volume >= 0),
  trade_count INTEGER NOT NULL DEFAULT 0 CHECK (trade_count >= 0),
  taker_buy_base_volume REAL NOT NULL DEFAULT 0 CHECK (taker_buy_base_volume >= 0),
  taker_buy_quote_volume REAL NOT NULL DEFAULT 0 CHECK (taker_buy_quote_volume >= 0),
  source TEXT NOT NULL DEFAULT 'binance',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(symbol, interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_market_klines_symbol_interval_close_time
ON market_klines(symbol, interval, close_time DESC);

CREATE TABLE IF NOT EXISTS market_sync_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_name TEXT NOT NULL,
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  range_start TEXT NOT NULL,
  range_end TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'success', 'failed')),
  fetched_count INTEGER NOT NULL DEFAULT 0 CHECK (fetched_count >= 0),
  stored_count INTEGER NOT NULL DEFAULT 0 CHECK (stored_count >= 0),
  error_message TEXT NOT NULL DEFAULT '',
  started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_market_sync_runs_job_status_started_at
ON market_sync_runs(job_name, status, started_at DESC);

CREATE TABLE IF NOT EXISTS task_locks (
  lock_key TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  locked_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS recurring_investment_plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    quote_currency TEXT NOT NULL DEFAULT 'USDT',
    budget_amount REAL NOT NULL CHECK (budget_amount > 0),
    schedule_type TEXT NOT NULL CHECK (schedule_type IN ('daily')),
    run_time TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'Asia/Taipei',
    asset_class TEXT NOT NULL DEFAULT 'crypto',
    market TEXT NOT NULL DEFAULT 'BINANCE',
    price_source TEXT NOT NULL DEFAULT 'binance',
    note TEXT NOT NULL DEFAULT '',
    is_enabled INTEGER NOT NULL DEFAULT 1 CHECK (is_enabled IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, quote_currency, schedule_type, run_time, timezone)
);

CREATE TABLE IF NOT EXISTS recurring_investment_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL,
    scheduled_for TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'success')),
    symbol TEXT NOT NULL,
    quote_currency TEXT NOT NULL DEFAULT 'USDT',
    budget_amount REAL NOT NULL CHECK (budget_amount > 0),
    price REAL NOT NULL CHECK (price > 0),
    quantity REAL NOT NULL CHECK (quantity > 0),
    source TEXT NOT NULL DEFAULT 'binance',
    trade_id INTEGER,
    message TEXT NOT NULL DEFAULT '',
    executed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(plan_id) REFERENCES recurring_investment_plans(id) ON DELETE CASCADE,
    FOREIGN KEY(trade_id) REFERENCES investment_trades(id) ON DELETE SET NULL,
    UNIQUE(plan_id, scheduled_for)
);

CREATE TABLE IF NOT EXISTS recurring_ledger_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL,
    scheduled_for TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'success', 'failed')),
    entry_type TEXT NOT NULL CHECK (entry_type IN ('expense', 'income')),
    amount REAL NOT NULL CHECK (amount > 0),
    description TEXT NOT NULL,
    category TEXT NOT NULL,
    ledger_entry_id INTEGER,
    message TEXT NOT NULL DEFAULT '',
    executed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(plan_id) REFERENCES recurring_ledger_plans(id) ON DELETE CASCADE,
    FOREIGN KEY(ledger_entry_id) REFERENCES ledger_entries(id) ON DELETE SET NULL,
    UNIQUE(plan_id, scheduled_for)
);

CREATE INDEX IF NOT EXISTS idx_recurring_ledger_runs_plan_scheduled
ON recurring_ledger_runs(plan_id, scheduled_for DESC, status);
`;

function applySchemaMigrations(db) {
  db.exec("DROP TABLE IF EXISTS investment_dividends;");
}

export function resolveDbPath() {
  if (process.env.WEALTH_CLI_DB) {
    return resolve(process.env.WEALTH_CLI_DB);
  }
  return DEFAULT_DB_PATH;
}

export function openDatabase() {
  const dbPath = resolveDbPath();
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new DatabaseSync(dbPath);
  db.exec(SCHEMA);
  applySchemaMigrations(db);
  return { db, dbPath };
}

export function withDatabase(handler) {
  const { db, dbPath } = openDatabase();
  try {
    return handler(db, dbPath);
  } finally {
    db.close();
  }
}
