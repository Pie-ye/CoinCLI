from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


DEFAULT_DB_PATH = Path.home() / ".wealth_cli" / "wealth.db"

SCHEMA = """
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
"""


def resolve_db_path() -> Path:
    configured = os.environ.get("WEALTH_CLI_DB")
    if configured:
        return Path(configured).expanduser().resolve()
    return DEFAULT_DB_PATH


def initialize_database(db_path: Path | None = None) -> Path:
    path = db_path or resolve_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.executescript(SCHEMA)
        connection.commit()
    finally:
        connection.close()
    return path


@contextmanager
def connect(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    path = initialize_database(db_path)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()
