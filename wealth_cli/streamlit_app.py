from __future__ import annotations

import re
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import streamlit as st

from wealth_cli.app import (
    compute_positions,
    ensure_iso_date,
    ensure_non_negative_decimal,
    ensure_positive_decimal,
    money,
    percent_text,
    quantity_text,
    signed_money,
)
from wealth_cli.database import connect, resolve_db_path


HELP_TEXT = """支援的快捷指令：

- `支出 150 午餐 分類 餐飲`
- `收入 50000 薪水 分類 工作`
- `買入 BTC 0.01 70000 手續費 0`
- `賣出 AAPL 2 195 手續費 1.5`
- `價格 BTC 70059.39 USDT 來源 binance`
- `定投 BTC 100 USDT 12:00`

也支援 slash 版本：

- `/expense 150 午餐 | 餐飲 | 2026-03-26`
- `/income 50000 薪水 | 工作`
- `/buy BTC 0.01 70000 | 0 | crypto | BINANCE | 備註 | 2026-03-26`
- `/sell AAPL 1 200 | 1.5`
- `/price BTC 70059.39 | USDT | binance | 2026-03-26`
- `/dca BTC 100 12:00 | Asia/Taipei | 每日定投`
"""

LEDGER_PATTERN = re.compile(
    r"^(?P<kind>支出|收入)\s+"
    r"(?P<amount>\d+(?:\.\d+)?)\s+"
    r"(?P<description>.+?)"
    r"(?:\s+分類\s+(?P<category>\S+))?"
    r"(?:\s+日期\s+(?P<entry_date>\d{4}-\d{2}-\d{2}))?$"
)
TRADE_PATTERN = re.compile(
    r"^(?P<kind>買入|賣出)\s+"
    r"(?P<symbol>[A-Za-z0-9._-]+)\s+"
    r"(?P<quantity>\d+(?:\.\d+)?)\s+"
    r"(?P<unit_price>\d+(?:\.\d+)?)"
    r"(?:\s+手續費\s+(?P<fee>\d+(?:\.\d+)?))?"
    r"(?:\s+日期\s+(?P<trade_date>\d{4}-\d{2}-\d{2}))?$"
)
PRICE_PATTERN = re.compile(
    r"^(?:價格|設定價格|更新價格)\s+"
    r"(?P<symbol>[A-Za-z0-9._-]+)\s+"
    r"(?P<price>\d+(?:\.\d+)?)"
    r"(?:\s+(?P<currency>[A-Za-z]{2,8}))?"
    r"(?:\s+來源\s+(?P<source>\S+))?"
    r"(?:\s+日期\s+(?P<as_of>\d{4}-\d{2}-\d{2}))?$"
)
DCA_PATTERN = re.compile(
    r"^(?:定投|設定定投)\s+"
    r"(?P<symbol>[A-Za-z0-9._-]+)\s+"
    r"(?P<budget>\d+(?:\.\d+)?)\s+"
    r"(?P<currency>[A-Za-z]{2,8})\s+"
    r"(?P<run_time>\d{1,2}:\d{2})"
    r"(?:\s+時區\s+(?P<time_zone>\S+))?$"
)


def style_page() -> None:
    st.set_page_config(page_title="CLI-Wealth Streamlit", layout="wide", page_icon="W")
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        .wealth-panel {
            background: linear-gradient(180deg, #fffef8 0%, #f6f7fb 100%);
            border: 1px solid rgba(15, 23, 42, 0.08);
            border-radius: 20px;
            padding: 1.25rem 1.25rem 0.75rem 1.25rem;
            box-shadow: 0 18px 40px rgba(15, 23, 42, 0.08);
        }
        .wealth-caption {
            color: #5b6472;
            font-size: 0.95rem;
            margin-top: -0.35rem;
            margin-bottom: 0.75rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def month_bounds() -> tuple[str, str]:
    today = date.today()
    start = today.replace(day=1).isoformat()
    return start, today.isoformat()


def fetch_ledger_rows(entry_type: str | None = None, *, limit: int = 200) -> list[dict[str, Any]]:
    query = """
        SELECT id, entry_date, entry_type, category, amount, description, tags, created_at
        FROM ledger_entries
    """
    values: list[Any] = []
    if entry_type:
        query += " WHERE entry_type = ?"
        values.append(entry_type)
    query += " ORDER BY entry_date DESC, id DESC LIMIT ?"
    values.append(limit)

    with connect() as connection:
        rows = connection.execute(query, values).fetchall()

    return [
        {
            "ID": row["id"],
            "日期": row["entry_date"],
            "類型": row["entry_type"],
            "分類": row["category"],
            "金額": float(row["amount"]),
            "說明": row["description"],
            "標籤": row["tags"],
            "建立時間": row["created_at"],
        }
        for row in rows
    ]


def fetch_ledger_totals() -> dict[str, Decimal]:
    start_date, end_date = month_bounds()
    with connect() as connection:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN entry_type = 'income' THEN amount ELSE 0 END), 0) AS total_income,
                COALESCE(SUM(CASE WHEN entry_type = 'expense' THEN amount ELSE 0 END), 0) AS total_expense
            FROM ledger_entries
            WHERE entry_date >= ? AND entry_date <= ?
            """,
            (start_date, end_date),
        ).fetchone()

    income = Decimal(str(row["total_income"]))
    expense = Decimal(str(row["total_expense"]))
    return {
        "income": income,
        "expense": expense,
        "net": income - expense,
    }


def fetch_trade_rows(limit: int = 200) -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT id, trade_date, trade_type, symbol, asset_class, market, quantity, unit_price, fee, note, created_at
            FROM investment_trades
            ORDER BY trade_date DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        {
            "ID": row["id"],
            "日期": row["trade_date"],
            "方向": row["trade_type"],
            "標的": row["symbol"],
            "資產類型": row["asset_class"],
            "市場": row["market"],
            "數量": float(row["quantity"]),
            "單價": float(row["unit_price"]),
            "手續費": float(row["fee"]),
            "備註": row["note"],
            "建立時間": row["created_at"],
        }
        for row in rows
    ]


def fetch_dividend_rows(limit: int = 100) -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT id, payout_date, dividend_type, symbol, amount, quantity, note, created_at
            FROM investment_dividends
            ORDER BY payout_date DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        {
            "ID": row["id"],
            "日期": row["payout_date"],
            "類型": row["dividend_type"],
            "標的": row["symbol"],
            "金額": float(row["amount"]),
            "數量": float(row["quantity"]),
            "備註": row["note"],
            "建立時間": row["created_at"],
        }
        for row in rows
    ]


def fetch_price_rows() -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT symbol, price, currency, source, as_of, updated_at
            FROM market_prices
            ORDER BY symbol ASC
            """
        ).fetchall()

    return [
        {
            "標的": row["symbol"],
            "價格": float(row["price"]),
            "幣別": row["currency"],
            "來源": row["source"],
            "價格日期": row["as_of"],
            "更新時間": row["updated_at"],
        }
        for row in rows
    ]


def fetch_portfolio_rows() -> list[dict[str, Any]]:
    positions = compute_positions()
    return [
        {
            "標的": item.symbol,
            "資產類型": item.asset_class,
            "市場": item.market,
            "持有數量": quantity_text(item.quantity),
            "持倉成本": money(item.remaining_cost),
            "現價": money(item.price) if item.price is not None else "N/A",
            "市值": money(item.market_value) if item.market_value is not None else "N/A",
            "已實現損益": signed_money(item.realized_pnl),
            "股利": signed_money(item.dividends),
            "未實現損益": signed_money(item.unrealized_pnl) if item.unrealized_pnl is not None else "N/A",
            "ROI": percent_text(item.roi_pct),
            "價格日期": item.price_date or "",
        }
        for item in positions
    ]


def fetch_plan_rows() -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT
                plan.id,
                plan.symbol,
                plan.quote_currency,
                plan.budget_amount,
                plan.schedule_type,
                plan.run_time,
                plan.timezone,
                plan.asset_class,
                plan.market,
                plan.price_source,
                plan.note,
                plan.is_enabled,
                plan.updated_at,
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
            ORDER BY plan.is_enabled DESC, plan.run_time ASC, plan.id ASC
            """
        ).fetchall()

    return [
        {
            "ID": row["id"],
            "標的": row["symbol"],
            "預算": float(row["budget_amount"]),
            "預算幣別": row["quote_currency"],
            "排程": row["schedule_type"],
            "時間": row["run_time"],
            "時區": row["timezone"],
            "資產類型": row["asset_class"],
            "市場": row["market"],
            "價格來源": row["price_source"],
            "啟用": "是" if row["is_enabled"] else "否",
            "最後排程": row["last_scheduled_for"] or "",
            "最後執行": row["last_executed_at"] or "",
            "最後交易ID": row["last_trade_id"] or "",
            "備註": row["note"],
            "更新時間": row["updated_at"],
        }
        for row in rows
    ]


def fetch_run_rows(limit: int = 200) -> list[dict[str, Any]]:
    with connect() as connection:
        rows = connection.execute(
            """
            SELECT
                id, plan_id, scheduled_for, status, symbol, quote_currency,
                budget_amount, price, quantity, source, trade_id, message, executed_at
            FROM recurring_investment_runs
            ORDER BY scheduled_for DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [
        {
            "ID": row["id"],
            "計畫ID": row["plan_id"],
            "排程時間": row["scheduled_for"],
            "狀態": row["status"],
            "標的": row["symbol"],
            "預算": float(row["budget_amount"]),
            "預算幣別": row["quote_currency"],
            "成交價": float(row["price"]),
            "數量": float(row["quantity"]),
            "來源": row["source"],
            "交易ID": row["trade_id"] or "",
            "訊息": row["message"],
            "執行時間": row["executed_at"],
        }
        for row in rows
    ]


def default_asset_class(symbol: str) -> str:
    return "crypto" if symbol.upper() in {"BTC", "ETH", "BTCUSDT"} else "stock"


def default_market(symbol: str) -> str:
    return "BINANCE" if symbol.upper() in {"BTC", "ETH", "BTCUSDT"} else ""


def add_ledger_entry(entry_type: str, amount: str, description: str, category: str, entry_date: str | None) -> str:
    normalized_amount = ensure_positive_decimal(amount, field_name="amount")
    normalized_date = ensure_iso_date(entry_date, field_name="date")

    with connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO ledger_entries (entry_type, amount, description, category, tags, entry_date)
            VALUES (?, ?, ?, ?, '', ?)
            """,
            (entry_type, float(normalized_amount), description, category, normalized_date),
        )

    return f"已新增 {entry_type} #{cursor.lastrowid}，金額 {money(normalized_amount)}，說明：{description}"


def add_trade(
    trade_type: str,
    symbol: str,
    quantity: str,
    unit_price: str,
    fee: str,
    trade_date: str | None,
    asset_class: str | None,
    market: str | None,
    note: str,
) -> str:
    normalized_symbol = symbol.upper()
    normalized_quantity = ensure_positive_decimal(quantity, field_name="quantity")
    normalized_price = ensure_non_negative_decimal(unit_price, field_name="unit price")
    normalized_fee = ensure_non_negative_decimal(fee, field_name="fee")
    normalized_date = ensure_iso_date(trade_date, field_name="date")

    if trade_type == "sell":
        positions = compute_positions(symbol_filter=normalized_symbol)
        current = positions[0] if positions else None
        available = current.quantity if current else Decimal("0")
        if normalized_quantity > available:
            raise ValueError(
                f"不能賣出 {quantity_text(normalized_quantity)} {normalized_symbol}，目前只有 {quantity_text(available)}。"
            )

    with connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO investment_trades (
                trade_type, symbol, asset_class, market, quantity, unit_price, fee, trade_date, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_type,
                normalized_symbol,
                asset_class or default_asset_class(normalized_symbol),
                (market or default_market(normalized_symbol)).upper(),
                float(normalized_quantity),
                float(normalized_price),
                float(normalized_fee),
                normalized_date,
                note,
            ),
        )

    return (
        f"已新增 {trade_type} 交易 #{cursor.lastrowid}，"
        f"{normalized_symbol} {quantity_text(normalized_quantity)} @ {money(normalized_price)}"
    )


def set_market_price(symbol: str, price: str, currency: str | None, source: str | None, as_of: str | None) -> str:
    normalized_symbol = symbol.upper()
    normalized_price = ensure_non_negative_decimal(price, field_name="price")
    normalized_date = ensure_iso_date(as_of, field_name="date")
    normalized_currency = (currency or "").upper()
    normalized_source = source or "manual"

    with connect() as connection:
        connection.execute(
            """
            INSERT INTO market_prices (symbol, price, currency, source, as_of, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                price = excluded.price,
                currency = excluded.currency,
                source = excluded.source,
                as_of = excluded.as_of,
                updated_at = CURRENT_TIMESTAMP
            """,
            (normalized_symbol, float(normalized_price), normalized_currency, normalized_source, normalized_date),
        )

    return f"已更新 {normalized_symbol} 價格為 {money(normalized_price)} {normalized_currency}".strip()


def upsert_recurring_plan(symbol: str, budget: str, currency: str, run_time: str, time_zone: str | None, note: str) -> str:
    normalized_symbol = symbol.upper()
    if normalized_symbol not in {"BTC", "BTCUSDT"}:
        raise ValueError("目前 Streamlit 版定投只支援 BTC。")

    normalized_budget = ensure_positive_decimal(budget, field_name="budget")
    normalized_currency = currency.upper()
    if normalized_currency != "USDT":
        raise ValueError("目前 Streamlit 版定投只支援 USDT 預算。")

    if not re.match(r"^\d{1,2}:\d{2}$", run_time):
        raise ValueError("定投時間必須是 HH:MM。")

    time_zone = time_zone or "Asia/Taipei"

    with connect() as connection:
        connection.execute(
            """
            INSERT INTO recurring_investment_plans (
                symbol, quote_currency, budget_amount, schedule_type, run_time, timezone,
                asset_class, market, price_source, note, is_enabled, updated_at
            )
            VALUES (?, ?, ?, 'daily', ?, ?, 'crypto', 'BINANCE', 'binance', ?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol, quote_currency, schedule_type, run_time, timezone) DO UPDATE SET
                budget_amount = excluded.budget_amount,
                note = excluded.note,
                is_enabled = 1,
                updated_at = CURRENT_TIMESTAMP
            """,
            ("BTC", normalized_currency, float(normalized_budget), run_time, time_zone, note),
        )

    return f"已設定每日 {run_time} 定投 BTC {money(normalized_budget)} {normalized_currency}"


def handle_slash_command(message: str) -> str:
    command, _, payload = message.partition(" ")
    command = command.lower()
    payload = payload.strip()

    if command in {"/help", "/h"}:
        return HELP_TEXT

    if command in {"/expense", "/income"}:
        amount, _, rest = payload.partition(" ")
        if not amount or not rest.strip():
            raise ValueError("格式：/expense 150 午餐 | 餐飲 | 2026-03-26")
        segments = [part.strip() for part in rest.split("|")]
        description = segments[0]
        category = segments[1] if len(segments) > 1 and segments[1] else "uncategorized"
        entry_date = segments[2] if len(segments) > 2 and segments[2] else None
        entry_type = "expense" if command == "/expense" else "income"
        return add_ledger_entry(entry_type, amount, description, category, entry_date)

    if command in {"/buy", "/sell"}:
        fields = payload.split()
        if len(fields) < 3:
            raise ValueError("格式：/buy BTC 0.01 70000 | 0 | crypto | BINANCE | 備註 | 2026-03-26")
        symbol, quantity, unit_price = fields[:3]
        extra = payload.split("|")
        fee = extra[1].strip() if len(extra) > 1 and extra[1].strip() else "0"
        asset_class = extra[2].strip() if len(extra) > 2 and extra[2].strip() else None
        market = extra[3].strip() if len(extra) > 3 and extra[3].strip() else None
        note = extra[4].strip() if len(extra) > 4 and extra[4].strip() else ""
        trade_date = extra[5].strip() if len(extra) > 5 and extra[5].strip() else None
        trade_type = "buy" if command == "/buy" else "sell"
        return add_trade(trade_type, symbol, quantity, unit_price, fee, trade_date, asset_class, market, note)

    if command == "/price":
        fields = payload.split()
        if len(fields) < 2:
            raise ValueError("格式：/price BTC 70059.39 | USDT | binance | 2026-03-26")
        symbol, price = fields[:2]
        extra = payload.split("|")
        currency = extra[1].strip() if len(extra) > 1 and extra[1].strip() else ""
        source = extra[2].strip() if len(extra) > 2 and extra[2].strip() else "manual"
        as_of = extra[3].strip() if len(extra) > 3 and extra[3].strip() else None
        return set_market_price(symbol, price, currency, source, as_of)

    if command == "/dca":
        fields = payload.split()
        if len(fields) < 3:
            raise ValueError("格式：/dca BTC 100 12:00 | Asia/Taipei | 每日定投")
        symbol, budget, run_time = fields[:3]
        extra = payload.split("|")
        time_zone = extra[1].strip() if len(extra) > 1 and extra[1].strip() else "Asia/Taipei"
        note = extra[2].strip() if len(extra) > 2 and extra[2].strip() else ""
        return upsert_recurring_plan(symbol, budget, "USDT", run_time, time_zone, note)

    raise ValueError("目前不支援這個指令，輸入 /help 查看格式。")


def handle_text_command(message: str) -> str:
    text = message.strip()
    if not text:
        raise ValueError("請輸入指令。")

    if text.lower() in {"help", "/help", "?", "幫助"}:
        return HELP_TEXT

    if text.startswith("/"):
        return handle_slash_command(text)

    match = LEDGER_PATTERN.match(text)
    if match:
        entry_type = "expense" if match.group("kind") == "支出" else "income"
        return add_ledger_entry(
            entry_type,
            match.group("amount"),
            match.group("description"),
            match.group("category") or "uncategorized",
            match.group("entry_date"),
        )

    match = TRADE_PATTERN.match(text)
    if match:
        trade_type = "buy" if match.group("kind") == "買入" else "sell"
        return add_trade(
            trade_type,
            match.group("symbol"),
            match.group("quantity"),
            match.group("unit_price"),
            match.group("fee") or "0",
            match.group("trade_date"),
            None,
            None,
            "",
        )

    match = PRICE_PATTERN.match(text)
    if match:
        return set_market_price(
            match.group("symbol"),
            match.group("price"),
            match.group("currency"),
            match.group("source"),
            match.group("as_of"),
        )

    match = DCA_PATTERN.match(text)
    if match:
        return upsert_recurring_plan(
            match.group("symbol"),
            match.group("budget"),
            match.group("currency"),
            match.group("run_time"),
            match.group("time_zone"),
            "",
        )

    raise ValueError("無法辨識這段指令，輸入 /help 可查看支援格式。")


def render_chat_panel() -> None:
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = [
            {
                "role": "assistant",
                "content": "這裡是快捷輸入框。你可以直接新增收入、支出、投資交易、價格與 BTC 定投。",
            }
        ]

    left, center, right = st.columns([1, 1.8, 1])
    del left, right

    with center:
        st.markdown('<div class="wealth-panel">', unsafe_allow_html=True)
        st.subheader("快捷對話框")
        st.markdown(
            f'<div class="wealth-caption">資料庫：{resolve_db_path()}。輸入 `help` 或 `/help` 可查看格式。</div>',
            unsafe_allow_html=True,
        )

        for item in st.session_state.chat_history[-8:]:
            with st.chat_message(item["role"]):
                st.markdown(item["content"])

        with st.form("chat_form", clear_on_submit=True):
            prompt = st.text_input("輸入指令", placeholder="例如：支出 150 午餐 分類 餐飲")
            submitted = st.form_submit_button("送出", use_container_width=True)

        if submitted:
            st.session_state.chat_history.append({"role": "user", "content": prompt})
            try:
                response = handle_text_command(prompt)
            except (Exception, SystemExit) as error:
                response = str(error)
            st.session_state.chat_history.append({"role": "assistant", "content": response})
            st.rerun()

        with st.expander("可用格式", expanded=False):
            st.markdown(HELP_TEXT)
        st.markdown("</div>", unsafe_allow_html=True)


def render_metrics() -> None:
    totals = fetch_ledger_totals()
    positions = compute_positions()
    plans = fetch_plan_rows()

    capital_in = sum((item.capital_in for item in positions), Decimal("0"))
    open_cost = sum((item.remaining_cost for item in positions), Decimal("0"))

    first, second, third, fourth = st.columns(4)
    first.metric("本月收入", money(totals["income"]))
    second.metric("本月支出", money(totals["expense"]))
    third.metric("本月淨額", signed_money(totals["net"]))
    fourth.metric("投資成本 / 定投計畫", f"{money(open_cost)} / {len(plans)}")

    if capital_in > 0:
        st.caption(f"累計投入資本：{money(capital_in)}")


def render_table(title: str, rows: list[dict[str, Any]], *, empty_text: str) -> None:
    st.subheader(title)
    if not rows:
        st.info(empty_text)
        return
    st.dataframe(rows, use_container_width=True, hide_index=True)


def render_selected_view(view: str) -> None:
    if view == "總覽":
        income_rows = fetch_ledger_rows("income", limit=8)
        expense_rows = fetch_ledger_rows("expense", limit=8)
        trade_rows = fetch_trade_rows(limit=8)
        left, middle, right = st.columns(3)
        with left:
            render_table("最新收入", income_rows, empty_text="目前沒有收入資料。")
        with middle:
            render_table("最新支出", expense_rows, empty_text="目前沒有支出資料。")
        with right:
            render_table("最新投資交易", trade_rows, empty_text="目前沒有投資交易資料。")
        render_table("目前投資部位", fetch_portfolio_rows(), empty_text="目前沒有持倉。")
        return

    if view == "收入":
        render_table("收入列表", fetch_ledger_rows("income"), empty_text="目前沒有收入資料。")
        return

    if view == "支出":
        render_table("支出列表", fetch_ledger_rows("expense"), empty_text="目前沒有支出資料。")
        return

    if view == "投資交易":
        render_table("投資交易列表", fetch_trade_rows(), empty_text="目前沒有投資交易資料。")
        render_table("股利列表", fetch_dividend_rows(), empty_text="目前沒有股利資料。")
        return

    if view == "投資部位":
        render_table("投資部位", fetch_portfolio_rows(), empty_text="目前沒有持倉。")
        return

    if view == "價格":
        render_table("最新價格", fetch_price_rows(), empty_text="目前沒有價格資料。")
        return

    if view == "定投計畫":
        render_table("定投計畫", fetch_plan_rows(), empty_text="目前沒有定投計畫。")
        return

    if view == "定投執行":
        render_table("定投執行紀錄", fetch_run_rows(), empty_text="目前沒有定投執行紀錄。")


def main() -> None:
    style_page()
    st.sidebar.title("CLI-Wealth")
    st.sidebar.caption("Streamlit 版本")
    view = st.sidebar.radio(
        "查看列表",
        ["總覽", "收入", "支出", "投資交易", "投資部位", "價格", "定投計畫", "定投執行"],
    )
    st.sidebar.markdown(
        "\n".join(
            [
                "快捷指令重點：",
                "- 支出 / 收入",
                "- 買入 / 賣出",
                "- 價格更新",
                "- BTC 定投設定",
            ]
        )
    )

    st.title("CLI-Wealth")
    st.caption("第二版本：Streamlit 介面，中央輸入，側欄瀏覽資料。")

    render_chat_panel()
    render_metrics()
    st.divider()
    render_selected_view(view)


def launch() -> None:
    from streamlit.web.cli import main as streamlit_main

    script_path = str(Path(__file__).resolve())
    sys.argv = ["streamlit", "run", script_path]
    raise SystemExit(streamlit_main())


if __name__ == "__main__":
    launch()
