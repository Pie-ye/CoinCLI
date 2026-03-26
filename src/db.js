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

CREATE TABLE IF NOT EXISTS investment_dividends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    dividend_type TEXT NOT NULL CHECK (dividend_type IN ('cash', 'stock')),
    amount REAL NOT NULL DEFAULT 0 CHECK (amount >= 0),
    quantity REAL NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    payout_date TEXT NOT NULL,
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
`;

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
