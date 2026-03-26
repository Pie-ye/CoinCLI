from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Iterable, Sequence

from wealth_cli.database import connect, initialize_database


DecimalLike = Decimal | int | float | str


@dataclass
class PositionSnapshot:
    symbol: str
    asset_class: str
    market: str
    quantity: Decimal
    remaining_cost: Decimal
    capital_in: Decimal
    realized_pnl: Decimal
    dividends: Decimal
    price: Decimal | None
    currency: str
    price_date: str | None

    @property
    def market_value(self) -> Decimal | None:
        if self.quantity == 0:
            return Decimal("0")
        if self.price is None:
            return None
        return self.quantity * self.price

    @property
    def unrealized_pnl(self) -> Decimal | None:
        market_value = self.market_value
        if market_value is None:
            return None
        return market_value - self.remaining_cost

    @property
    def total_return(self) -> Decimal | None:
        unrealized = self.unrealized_pnl
        if unrealized is None:
            return None
        return unrealized + self.realized_pnl + self.dividends

    @property
    def roi_pct(self) -> Decimal | None:
        total_return = self.total_return
        if total_return is None or self.capital_in <= 0:
            return None
        return (total_return / self.capital_in) * Decimal("100")


def money(value: DecimalLike, places: str = "0.01") -> str:
    amount = to_decimal(value).quantize(Decimal(places))
    return f"{amount:,.2f}"


def quantity_text(value: DecimalLike) -> str:
    amount = to_decimal(value).normalize()
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def signed_money(value: DecimalLike) -> str:
    amount = to_decimal(value).quantize(Decimal("0.01"))
    return f"{amount:+,.2f}"


def percent_text(value: DecimalLike | None) -> str:
    if value is None:
        return "N/A"
    amount = to_decimal(value).quantize(Decimal("0.01"))
    return f"{amount:+.2f}%"


def to_decimal(value: DecimalLike) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def ensure_iso_date(raw: str | None, *, field_name: str = "date") -> str:
    if raw is None:
        return date.today().isoformat()
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError as exc:
        raise SystemExit(f"Invalid {field_name}: {raw}. Expected YYYY-MM-DD.") from exc


def ensure_positive_decimal(raw: str, *, field_name: str) -> Decimal:
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise SystemExit(f"Invalid {field_name}: {raw}") from exc
    if value <= 0:
        raise SystemExit(f"{field_name} must be greater than 0.")
    return value


def ensure_non_negative_decimal(raw: str, *, field_name: str) -> Decimal:
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise SystemExit(f"Invalid {field_name}: {raw}") from exc
    if value < 0:
        raise SystemExit(f"{field_name} must be 0 or greater.")
    return value


def parse_tags(raw: str | None) -> str:
    if not raw:
        return ""
    parts = [item.strip() for item in raw.split(",") if item.strip()]
    return ",".join(parts)


def normalize_entry_type(raw: str) -> str:
    aliases = {"exp": "expense", "inc": "income"}
    return aliases.get(raw, raw)


def period_bounds(period: str | None) -> tuple[str | None, str | None]:
    if not period or period == "all":
        return None, None

    today = date.today()
    if period == "day":
        start = today
    elif period == "week":
        start = today - timedelta(days=today.weekday())
    elif period == "month":
        start = today.replace(day=1)
    elif period == "year":
        start = today.replace(month=1, day=1)
    else:
        raise SystemExit(f"Unsupported period: {period}")
    return start.isoformat(), today.isoformat()


def render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    widths = [len(header) for header in headers]
    for row in rows:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(str(cell)))

    def format_row(values: Sequence[str]) -> str:
        formatted: list[str] = []
        for index, value in enumerate(values):
            text = str(value)
            if looks_numeric(text):
                formatted.append(text.rjust(widths[index]))
            else:
                formatted.append(text.ljust(widths[index]))
        return " | ".join(formatted)

    separator = "-+-".join("-" * width for width in widths)
    lines = [format_row(headers), separator]
    lines.extend(format_row(row) for row in rows)
    return "\n".join(lines)


def looks_numeric(text: str) -> bool:
    if "-" in text and ":" not in text and text.count("-") >= 2:
        return False
    cleaned = text.replace(",", "").replace("%", "").replace("+", "").replace("-", "")
    cleaned = cleaned.replace(".", "", 1)
    return cleaned.isdigit() or text == "N/A"


def make_bar(value: Decimal, maximum: Decimal, width: int = 24) -> str:
    if maximum <= 0:
        return ""
    scale = float(abs(value) / maximum)
    length = max(1, math.floor(scale * width)) if value != 0 else 0
    return "#" * length


def cmd_init(_: argparse.Namespace) -> int:
    path = initialize_database()
    print(f"Database ready at {path}")
    return 0


def cmd_ledger_add(args: argparse.Namespace) -> int:
    entry_type = normalize_entry_type(args.entry_type)
    amount = ensure_positive_decimal(args.amount, field_name="amount")
    entry_date = ensure_iso_date(args.date, field_name="date")
    tags = parse_tags(args.tags)

    with connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO ledger_entries (entry_type, amount, description, category, tags, entry_date)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (entry_type, float(amount), args.description, args.category, tags, entry_date),
        )
    print(f"Saved {entry_type} entry #{cursor.lastrowid}: {money(amount)} {args.description}")
    return 0


def cmd_ledger_list(args: argparse.Namespace) -> int:
    period_start, period_end = period_bounds(args.period)
    start_date = ensure_iso_date(args.start, field_name="start date") if args.start else period_start
    end_date = ensure_iso_date(args.end, field_name="end date") if args.end else period_end

    clauses: list[str] = []
    values: list[object] = []

    if args.entry_type:
        clauses.append("entry_type = ?")
        values.append(normalize_entry_type(args.entry_type))
    if args.category:
        clauses.append("category = ?")
        values.append(args.category)
    if start_date:
        clauses.append("entry_date >= ?")
        values.append(start_date)
    if end_date:
        clauses.append("entry_date <= ?")
        values.append(end_date)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"""
        SELECT id, entry_date, entry_type, category, amount, description, tags
        FROM ledger_entries
        {where}
        ORDER BY entry_date DESC, id DESC
    """

    with connect() as connection:
        rows = connection.execute(query, values).fetchall()

    if not rows:
        print("No ledger entries found.")
        return 0

    table_rows = [
        [
            str(row["id"]),
            row["entry_date"],
            row["entry_type"],
            row["category"],
            money(row["amount"]),
            row["description"],
            row["tags"],
        ]
        for row in rows
    ]
    print(render_table(["ID", "Date", "Type", "Category", "Amount", "Description", "Tags"], table_rows))
    return 0


def cmd_ledger_delete(args: argparse.Namespace) -> int:
    with connect() as connection:
        existing = connection.execute(
            "SELECT id, entry_type, amount, description FROM ledger_entries WHERE id = ?",
            (args.entry_id,),
        ).fetchone()
        if existing is None:
            raise SystemExit(f"Ledger entry #{args.entry_id} not found.")

        connection.execute("DELETE FROM ledger_entries WHERE id = ?", (args.entry_id,))

    print(
        f"Deleted {existing['entry_type']} entry #{existing['id']}: "
        f"{money(existing['amount'])} {existing['description']}"
    )
    return 0


def cmd_ledger_report(args: argparse.Namespace) -> int:
    start_date, end_date = period_bounds(args.period)
    if args.start:
        start_date = ensure_iso_date(args.start, field_name="start date")
    if args.end:
        end_date = ensure_iso_date(args.end, field_name="end date")

    clauses: list[str] = []
    values: list[object] = []
    if start_date:
        clauses.append("entry_date >= ?")
        values.append(start_date)
    if end_date:
        clauses.append("entry_date <= ?")
        values.append(end_date)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect() as connection:
        summary = connection.execute(
            f"""
            SELECT
                COALESCE(SUM(CASE WHEN entry_type = 'income' THEN amount ELSE 0 END), 0) AS total_income,
                COALESCE(SUM(CASE WHEN entry_type = 'expense' THEN amount ELSE 0 END), 0) AS total_expense
            FROM ledger_entries
            {where}
            """,
            values,
        ).fetchone()

        by_category = connection.execute(
            f"""
            SELECT
                category,
                COALESCE(SUM(CASE WHEN entry_type = 'income' THEN amount ELSE 0 END), 0) AS income,
                COALESCE(SUM(CASE WHEN entry_type = 'expense' THEN amount ELSE 0 END), 0) AS expense
            FROM ledger_entries
            {where}
            GROUP BY category
            ORDER BY expense DESC, income DESC, category ASC
            """,
            values,
        ).fetchall()

    total_income = to_decimal(summary["total_income"])
    total_expense = to_decimal(summary["total_expense"])
    net = total_income - total_expense

    print(f"Period: {start_date or 'beginning'} -> {end_date or 'today'}")
    print(f"Income : {money(total_income)}")
    print(f"Expense: {money(total_expense)}")
    print(f"Net    : {signed_money(net)}")

    if not by_category:
        return 0

    max_value = max(
        [to_decimal(row["income"]) for row in by_category] + [to_decimal(row["expense"]) for row in by_category],
        default=Decimal("0"),
    )
    print("\nBy category")
    report_rows = []
    for row in by_category:
        income = to_decimal(row["income"])
        expense = to_decimal(row["expense"])
        report_rows.append(
            [
                row["category"],
                money(income),
                money(expense),
                signed_money(income - expense),
                make_bar(max(income, expense), max_value),
            ]
        )
    print(render_table(["Category", "Income", "Expense", "Net", "Bar"], report_rows))
    return 0


def cmd_invest_trade(args: argparse.Namespace) -> int:
    quantity = ensure_positive_decimal(args.quantity, field_name="quantity")
    unit_price = ensure_non_negative_decimal(args.unit_price, field_name="unit price")
    fee = ensure_non_negative_decimal(args.fee, field_name="fee")
    trade_date = ensure_iso_date(args.date, field_name="date")

    trade_type = args.trade_type
    if trade_type == "sell":
        current_positions = compute_positions(symbol_filter=args.symbol)
        position = current_positions[0] if current_positions else None
        available_qty = position.quantity if position else Decimal("0")
        if quantity > available_qty:
            raise SystemExit(
                f"Cannot sell {quantity_text(quantity)} {args.symbol}. "
                f"Only {quantity_text(available_qty)} available."
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
                args.symbol.upper(),
                args.asset_class,
                args.market.upper(),
                float(quantity),
                float(unit_price),
                float(fee),
                trade_date,
                args.note,
            ),
        )

    notional = quantity * unit_price
    print(
        f"Saved {trade_type} trade #{cursor.lastrowid}: "
        f"{args.symbol.upper()} {quantity_text(quantity)} @ {money(unit_price)} "
        f"(fee {money(fee)}, gross {money(notional)})"
    )
    return 0


def cmd_invest_dividend(args: argparse.Namespace) -> int:
    amount = ensure_non_negative_decimal(args.amount, field_name="amount")
    quantity = ensure_non_negative_decimal(args.quantity, field_name="quantity")
    payout_date = ensure_iso_date(args.date, field_name="date")
    dividend_type = args.dividend_type
    if dividend_type == "stock" and quantity <= 0:
        raise SystemExit("Stock dividend requires --quantity greater than 0.")

    with connect() as connection:
        cursor = connection.execute(
            """
            INSERT INTO investment_dividends (symbol, dividend_type, amount, quantity, payout_date, note)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (args.symbol.upper(), dividend_type, float(amount), float(quantity), payout_date, args.note),
        )

    print(
        f"Saved {dividend_type} dividend #{cursor.lastrowid}: "
        f"{args.symbol.upper()} amount {money(amount)} qty {quantity_text(quantity)}"
    )
    return 0


def cmd_invest_price_set(args: argparse.Namespace) -> int:
    price = ensure_non_negative_decimal(args.price, field_name="price")
    as_of = ensure_iso_date(args.date, field_name="date")
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
            (args.symbol.upper(), float(price), args.currency.upper(), args.source, as_of),
        )

    print(f"Stored price for {args.symbol.upper()}: {money(price)} {args.currency.upper()} ({args.source})")
    return 0


def cmd_invest_trades(_: argparse.Namespace) -> int:
    with connect() as connection:
        trades = connection.execute(
            """
            SELECT id, trade_date, trade_type, symbol, asset_class, market, quantity, unit_price, fee, note
            FROM investment_trades
            ORDER BY trade_date DESC, id DESC
            """
        ).fetchall()
        dividends = connection.execute(
            """
            SELECT id, payout_date, dividend_type, symbol, amount, quantity, note
            FROM investment_dividends
            ORDER BY payout_date DESC, id DESC
            """
        ).fetchall()

    if trades:
        trade_rows = [
            [
                str(row["id"]),
                row["trade_date"],
                row["trade_type"],
                row["symbol"],
                row["asset_class"],
                row["market"],
                quantity_text(row["quantity"]),
                money(row["unit_price"]),
                money(row["fee"]),
                row["note"],
            ]
            for row in trades
        ]
        print("Trades")
        print(render_table(["ID", "Date", "Type", "Symbol", "Asset", "Market", "Qty", "Price", "Fee", "Note"], trade_rows))
    else:
        print("No trades recorded.")

    if dividends:
        dividend_rows = [
            [
                str(row["id"]),
                row["payout_date"],
                row["dividend_type"],
                row["symbol"],
                money(row["amount"]),
                quantity_text(row["quantity"]),
                row["note"],
            ]
            for row in dividends
        ]
        print("\nDividends")
        print(render_table(["ID", "Date", "Type", "Symbol", "Amount", "Qty", "Note"], dividend_rows))
    return 0


def compute_positions(symbol_filter: str | None = None) -> list[PositionSnapshot]:
    with connect() as connection:
        trade_query = """
            SELECT trade_type, symbol, asset_class, market, quantity, unit_price, fee, trade_date
            FROM investment_trades
        """
        params: list[object] = []
        if symbol_filter:
            trade_query += " WHERE symbol = ?"
            params.append(symbol_filter.upper())
        trade_query += " ORDER BY trade_date ASC, id ASC"
        trades = connection.execute(trade_query, params).fetchall()

        dividend_query = """
            SELECT symbol, dividend_type, amount, quantity, payout_date
            FROM investment_dividends
        """
        dividend_params: list[object] = []
        if symbol_filter:
            dividend_query += " WHERE symbol = ?"
            dividend_params.append(symbol_filter.upper())
        dividend_query += " ORDER BY payout_date ASC, id ASC"
        dividends = connection.execute(dividend_query, dividend_params).fetchall()

        prices = connection.execute(
            "SELECT symbol, price, currency, as_of FROM market_prices"
        ).fetchall()

    state: dict[str, dict[str, Decimal | str]] = {}

    for row in trades:
        symbol = row["symbol"]
        snapshot = state.setdefault(
            symbol,
            {
                "symbol": symbol,
                "asset_class": row["asset_class"],
                "market": row["market"],
                "quantity": Decimal("0"),
                "remaining_cost": Decimal("0"),
                "capital_in": Decimal("0"),
                "realized_pnl": Decimal("0"),
                "dividends": Decimal("0"),
            },
        )

        quantity = to_decimal(row["quantity"])
        unit_price = to_decimal(row["unit_price"])
        fee = to_decimal(row["fee"])
        qty = to_decimal(snapshot["quantity"])
        remaining_cost = to_decimal(snapshot["remaining_cost"])

        if row["trade_type"] == "buy":
            snapshot["quantity"] = qty + quantity
            snapshot["remaining_cost"] = remaining_cost + (quantity * unit_price) + fee
            snapshot["capital_in"] = to_decimal(snapshot["capital_in"]) + (quantity * unit_price) + fee
        else:
            if qty <= 0 or quantity > qty:
                raise SystemExit(f"Invalid sell history for {symbol}.")
            average_cost = remaining_cost / qty if qty else Decimal("0")
            cost_basis = average_cost * quantity
            proceeds = (quantity * unit_price) - fee
            snapshot["realized_pnl"] = to_decimal(snapshot["realized_pnl"]) + (proceeds - cost_basis)
            snapshot["quantity"] = qty - quantity
            snapshot["remaining_cost"] = remaining_cost - cost_basis

    for row in dividends:
        symbol = row["symbol"]
        snapshot = state.setdefault(
            symbol,
            {
                "symbol": symbol,
                "asset_class": "unknown",
                "market": "",
                "quantity": Decimal("0"),
                "remaining_cost": Decimal("0"),
                "capital_in": Decimal("0"),
                "realized_pnl": Decimal("0"),
                "dividends": Decimal("0"),
            },
        )
        if row["dividend_type"] == "cash":
            snapshot["dividends"] = to_decimal(snapshot["dividends"]) + to_decimal(row["amount"])
        else:
            snapshot["quantity"] = to_decimal(snapshot["quantity"]) + to_decimal(row["quantity"])

    price_map = {
        row["symbol"]: {
            "price": to_decimal(row["price"]),
            "currency": row["currency"],
            "as_of": row["as_of"],
        }
        for row in prices
    }

    positions: list[PositionSnapshot] = []
    for symbol, raw in state.items():
        price_info = price_map.get(symbol)
        positions.append(
            PositionSnapshot(
                symbol=symbol,
                asset_class=str(raw["asset_class"]),
                market=str(raw["market"]),
                quantity=to_decimal(raw["quantity"]),
                remaining_cost=to_decimal(raw["remaining_cost"]),
                capital_in=to_decimal(raw["capital_in"]),
                realized_pnl=to_decimal(raw["realized_pnl"]),
                dividends=to_decimal(raw["dividends"]),
                price=price_info["price"] if price_info else None,
                currency=price_info["currency"] if price_info else "",
                price_date=price_info["as_of"] if price_info else None,
            )
        )

    positions.sort(key=lambda item: item.symbol)
    return positions


def cmd_invest_portfolio(_: argparse.Namespace) -> int:
    positions = compute_positions()
    if not positions:
        print("No investment records found.")
        return 0

    rows: list[list[str]] = []
    total_capital_in = Decimal("0")
    total_remaining_cost = Decimal("0")
    total_realized = Decimal("0")
    total_dividends = Decimal("0")
    total_market_value = Decimal("0")
    missing_prices: list[str] = []

    for item in positions:
        total_capital_in += item.capital_in
        total_remaining_cost += item.remaining_cost
        total_realized += item.realized_pnl
        total_dividends += item.dividends

        market_value = item.market_value
        if market_value is None and item.quantity > 0:
            missing_prices.append(item.symbol)
        if market_value is not None:
            total_market_value += market_value

        rows.append(
            [
                item.symbol,
                item.asset_class,
                item.market,
                quantity_text(item.quantity),
                money(item.remaining_cost),
                money(item.price) if item.price is not None else "N/A",
                money(market_value) if market_value is not None else "N/A",
                signed_money(item.realized_pnl),
                signed_money(item.dividends),
                signed_money(item.unrealized_pnl or Decimal("0")) if market_value is not None else "N/A",
                percent_text(item.roi_pct),
                item.price_date or "",
            ]
        )

    print(render_table(
        ["Symbol", "Asset", "Market", "Qty", "Cost", "Price", "Value", "Realized", "Dividend", "Unrealized", "ROI", "PriceDate"],
        rows,
    ))

    print("")
    print(f"Capital In : {money(total_capital_in)}")
    print(f"Open Cost  : {money(total_remaining_cost)}")
    print(f"Realized   : {signed_money(total_realized)}")
    print(f"Dividends  : {signed_money(total_dividends)}")

    if missing_prices:
        print(f"Market Value: partial only, missing prices for {', '.join(sorted(missing_prices))}")
    else:
        unrealized = total_market_value - total_remaining_cost
        total_return = unrealized + total_realized + total_dividends
        roi = (total_return / total_capital_in * Decimal("100")) if total_capital_in > 0 else None
        print(f"Market Value: {money(total_market_value)}")
        print(f"Unrealized : {signed_money(unrealized)}")
        print(f"Total Return: {signed_money(total_return)}")
        print(f"Portfolio ROI: {percent_text(roi)}")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wealth",
        description="CLI bookkeeping and investment tracking with SQLite storage.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create the database file and tables.")
    init_parser.set_defaults(func=cmd_init)

    ledger_parser = subparsers.add_parser("ledger", help="Manage income and expense records.")
    ledger_subparsers = ledger_parser.add_subparsers(dest="ledger_command", required=True)

    ledger_add = ledger_subparsers.add_parser("add", help="Add an income or expense entry.")
    ledger_add.add_argument("entry_type", choices=["expense", "income", "exp", "inc"])
    ledger_add.add_argument("amount")
    ledger_add.add_argument("description")
    ledger_add.add_argument("--category", default="uncategorized")
    ledger_add.add_argument("--tags")
    ledger_add.add_argument("--date")
    ledger_add.set_defaults(func=cmd_ledger_add)

    ledger_list = ledger_subparsers.add_parser("list", help="List ledger entries.")
    ledger_list.add_argument("--period", choices=["day", "week", "month", "year", "all"], default="all")
    ledger_list.add_argument("--start")
    ledger_list.add_argument("--end")
    ledger_list.add_argument("--type", dest="entry_type", choices=["expense", "income", "exp", "inc"])
    ledger_list.add_argument("--category")
    ledger_list.set_defaults(func=cmd_ledger_list)

    ledger_delete = ledger_subparsers.add_parser("delete", help="Delete a ledger entry by ID.")
    ledger_delete.add_argument("entry_id", type=int)
    ledger_delete.set_defaults(func=cmd_ledger_delete)

    ledger_report = ledger_subparsers.add_parser("report", help="Show summary report with category breakdown.")
    ledger_report.add_argument("--period", choices=["day", "week", "month", "year", "all"], default="month")
    ledger_report.add_argument("--start")
    ledger_report.add_argument("--end")
    ledger_report.set_defaults(func=cmd_ledger_report)

    invest_parser = subparsers.add_parser("invest", help="Manage trades, dividends, and portfolio summary.")
    invest_subparsers = invest_parser.add_subparsers(dest="invest_command", required=True)

    for trade_type in ("buy", "sell"):
        trade_parser = invest_subparsers.add_parser(trade_type, help=f"Record a {trade_type} trade.")
        trade_parser.add_argument("symbol")
        trade_parser.add_argument("quantity")
        trade_parser.add_argument("unit_price")
        trade_parser.add_argument("--fee", default="0")
        trade_parser.add_argument("--date")
        trade_parser.add_argument("--asset-class", default="stock")
        trade_parser.add_argument("--market", default="")
        trade_parser.add_argument("--note", default="")
        trade_parser.set_defaults(func=cmd_invest_trade, trade_type=trade_type)

    dividend_parser = invest_subparsers.add_parser("dividend", help="Record a cash or stock dividend.")
    dividend_parser.add_argument("symbol")
    dividend_parser.add_argument("amount")
    dividend_parser.add_argument("--type", dest="dividend_type", choices=["cash", "stock"], default="cash")
    dividend_parser.add_argument("--quantity", default="0")
    dividend_parser.add_argument("--date")
    dividend_parser.add_argument("--note", default="")
    dividend_parser.set_defaults(func=cmd_invest_dividend)

    price_parser = invest_subparsers.add_parser("price", help="Manage latest market prices.")
    price_subparsers = price_parser.add_subparsers(dest="price_command", required=True)
    price_set = price_subparsers.add_parser("set", help="Store the latest price for a symbol.")
    price_set.add_argument("symbol")
    price_set.add_argument("price")
    price_set.add_argument("--currency", default="")
    price_set.add_argument("--source", default="manual")
    price_set.add_argument("--date")
    price_set.set_defaults(func=cmd_invest_price_set)

    trades_parser = invest_subparsers.add_parser("trades", help="List recorded trades and dividends.")
    trades_parser.set_defaults(func=cmd_invest_trades)

    portfolio_parser = invest_subparsers.add_parser("portfolio", help="Show current positions and ROI.")
    portfolio_parser.set_defaults(func=cmd_invest_portfolio)

    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        return args.func(args)
    except BrokenPipeError:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
