#!/usr/bin/env node

import { parseArgs } from "node:util";
import process from "node:process";

import {
  addLedgerEntry,
  deleteLedgerEntry,
  getBtcRealtimeInfo,
  getDatabasePath,
  getLedgerReport,
  getPortfolioSummary,
  initDatabase,
  listInvestmentActivity,
  listLedgerEntries,
  listRecurringInvestmentPlans,
  recordInvestmentTrade,
  refreshMarketPrices,
  runDueRecurringInvestmentPlans,
  setMarketPrice,
  upsertRecurringInvestmentPlan,
} from "./wealth-service.js";

function extractGlobalFlags(argv) {
  const args = [];
  let json = false;

  for (const token of argv) {
    if (token === "--json") {
      json = true;
      continue;
    }
    args.push(token);
  }

  return { args, json };
}

function parseCommandArgs(args, options = {}) {
  return parseArgs({
    args,
    allowPositionals: true,
    strict: true,
    ...options,
  });
}

function toNumber(value, label) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    throw new Error(`${label} must be numeric.`);
  }
  return amount;
}

function toPositiveNumber(value, label) {
  const amount = toNumber(value, label);
  if (amount <= 0) {
    throw new Error(`${label} must be greater than 0.`);
  }
  return amount;
}

function toInteger(value, label) {
  const amount = Number(value);
  if (!Number.isInteger(amount)) {
    throw new Error(`${label} must be an integer.`);
  }
  return amount;
}

function parseBoolean(value, label = "value") {
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

  throw new Error(`${label} must be true or false.`);
}

function requirePositionals(positionals, count, usage) {
  if (positionals.length < count) {
    throw new Error(`Usage: ${usage}`);
  }
}

function looksNumeric(text) {
  if (text === null || text === undefined) {
    return false;
  }
  const value = String(text);
  if (value.includes("-") && value.includes(":") === false && value.split("-").length >= 3) {
    return false;
  }
  const cleaned = value.replaceAll(",", "").replaceAll("%", "").replaceAll("+", "").replaceAll("-", "");
  return cleaned.replace(".", "").match(/^\d+$/) !== null || value === "N/A";
}

function renderTable(headers, rows) {
  const widths = headers.map((header) => String(header).length);

  for (const row of rows) {
    row.forEach((cell, index) => {
      widths[index] = Math.max(widths[index], String(cell).length);
    });
  }

  const formatRow = (values) =>
    values
      .map((value, index) => {
        const text = String(value);
        return looksNumeric(text) ? text.padStart(widths[index]) : text.padEnd(widths[index]);
      })
      .join(" | ");

  const separator = widths.map((width) => "-".repeat(width)).join("-+-");
  return [formatRow(headers), separator, ...rows.map(formatRow)].join("\n");
}

function numberText(value, digits = 2) {
  if (value === null || value === undefined) {
    return "N/A";
  }
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return "N/A";
  }
  return amount.toLocaleString("zh-TW", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function quantityText(value, digits = 6) {
  if (value === null || value === undefined) {
    return "N/A";
  }
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return "N/A";
  }
  return amount.toLocaleString("zh-TW", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

function signedNumberText(value, digits = 2) {
  if (value === null || value === undefined) {
    return "N/A";
  }
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return "N/A";
  }
  return `${amount >= 0 ? "+" : ""}${amount.toLocaleString("zh-TW", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })}`;
}

function percentText(value, digits = 2) {
  const text = signedNumberText(value, digits);
  return text === "N/A" ? text : `${text}%`;
}

function output(result, json, printer) {
  if (json) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  printer(result);
}

function printHelp() {
  console.log(`Wealth CLI (Node.js)

Commands:
  wealth init
  wealth ledger add <expense|income> <amount> <description> [--category 類別] [--tags a,b] [--date YYYY-MM-DD]
  wealth ledger list [--period day|week|month|year|all] [--entry-type expense|income] [--category 類別] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--limit 20]
  wealth ledger report [--period day|week|month|year|all] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
  wealth ledger delete <id>
  wealth invest buy <symbol> <quantity> <unitPrice> [--fee 0] [--asset-class stock|crypto] [--market BINANCE] [--note 備註] [--date YYYY-MM-DD]
  wealth invest sell <symbol> <quantity> <unitPrice> [--fee 0] [--asset-class stock|crypto] [--market BINANCE] [--note 備註] [--date YYYY-MM-DD]
  wealth invest price set <symbol> <price> [--currency USDT] [--source manual] [--date YYYY-MM-DD]
  wealth invest price refresh [--symbol BTC]
  wealth invest trades [--symbol BTC] [--limit 20]
  wealth invest portfolio
  wealth market btc [--save]
  wealth recurring set <symbol> <budgetAmount> <runTime> [--quote-currency USDT] [--timezone Asia/Taipei] [--asset-class crypto] [--market BINANCE] [--price-source binance] [--note 備註] [--enabled true|false]
  wealth recurring list [--enabled true|false]
  wealth recurring run

Global flags:
  --json   以 JSON 輸出，方便後續串接金融分析或記帳系統
`);
}

function printLedgerList(result) {
  if (result.entries.length === 0) {
    console.log("查無收支資料。");
    return;
  }

  console.log(renderTable(
    ["ID", "日期", "類型", "分類", "金額", "說明", "標籤"],
    result.entries.map((entry) => [
      entry.id,
      entry.date,
      entry.entryType,
      entry.category,
      numberText(entry.amount),
      entry.description,
      entry.tags.join(","),
    ]),
  ));
  console.log("");
  console.log(`收入 ${numberText(result.totalIncome)} | 支出 ${numberText(result.totalExpense)} | 淨額 ${signedNumberText(result.net)}`);
}

function printLedgerReport(result) {
  console.log(`收入 ${numberText(result.totalIncome)} | 支出 ${numberText(result.totalExpense)} | 淨額 ${signedNumberText(result.net)}`);
  if (result.range.startDate || result.range.endDate) {
    console.log(`區間 ${result.range.startDate ?? "起始不限"} ~ ${result.range.endDate ?? "結束不限"}`);
  }

  if (result.categories.length === 0) {
    return;
  }

  console.log("");
  console.log(renderTable(
    ["分類", "收入", "支出", "淨額"],
    result.categories.map((category) => [
      category.category,
      numberText(category.income),
      numberText(category.expense),
      signedNumberText(category.net),
    ]),
  ));
}

function printInvestmentActivity(result) {
  if (result.trades.length > 0) {
    console.log("交易紀錄");
    console.log(renderTable(
      ["ID", "日期", "類型", "標的", "數量", "單價", "費用", "市場", "備註"],
      result.trades.map((trade) => [
        trade.id,
        trade.tradeDate,
        trade.tradeType,
        trade.symbol,
        quantityText(trade.quantity),
        numberText(trade.unitPrice),
        numberText(trade.fee),
        trade.market,
        trade.note,
      ]),
    ));
  }

  if (result.trades.length === 0) {
    console.log("查無投資紀錄。");
  }
}

function printPortfolio(result) {
  if (result.positions.length === 0) {
    console.log("目前沒有投資部位。");
    return;
  }

  console.log(renderTable(
    ["標的", "數量", "成本", "現價", "市值", "未實現", "已實現", "ROI%"],
    result.positions.map((position) => [
      position.symbol,
      quantityText(position.quantity),
      numberText(position.remainingCost),
      position.price === null ? "N/A" : numberText(position.price, 6),
      position.marketValue === null ? "N/A" : numberText(position.marketValue),
      position.unrealizedPnl === null ? "N/A" : signedNumberText(position.unrealizedPnl),
      signedNumberText(position.realizedPnl),
      percentText(position.roiPct),
    ]),
  ));
  console.log("");
  console.log(
    `投入 ${numberText(result.totals.capitalIn)} | 持倉成本 ${numberText(result.totals.openCost)} | 市值 ${result.totals.marketValue === null ? "N/A" : numberText(result.totals.marketValue)} | 總報酬 ${result.totals.totalReturn === null ? "N/A" : signedNumberText(result.totals.totalReturn)}`,
  );

  if (result.totals.missingPrices.length > 0) {
    console.log(`缺少價格：${result.totals.missingPrices.join(", ")}`);
  }
}

function printRecurringPlans(result) {
  if (result.plans.length === 0) {
    console.log("查無定投計畫。");
    return;
  }

  console.log(renderTable(
    ["ID", "標的", "預算", "時間", "時區", "啟用", "上次排程", "上次交易"],
    result.plans.map((plan) => [
      plan.id,
      plan.symbol,
      `${numberText(plan.budgetAmount)} ${plan.quoteCurrency}`,
      plan.runTime,
      plan.timeZone,
      plan.enabled ? "yes" : "no",
      plan.lastScheduledFor ?? "-",
      plan.lastTradeId ?? "-",
    ]),
  ));
}

async function handleLedger(command, args, json) {
  if (command === "add") {
    const parsed = parseCommandArgs(args, {
      options: {
        category: { type: "string" },
        tags: { type: "string" },
        date: { type: "string" },
      },
    });

    requirePositionals(parsed.positionals, 3, "wealth ledger add <expense|income> <amount> <description>");
    const [entryType, amount, ...descriptionParts] = parsed.positionals;
    if (!["expense", "income"].includes(entryType)) {
      throw new Error("entryType 必須為 expense 或 income。");
    }

    const result = addLedgerEntry({
      entryType,
      amount: toPositiveNumber(amount, "amount"),
      description: descriptionParts.join(" ").trim(),
      category: parsed.values.category,
      tags: parsed.values.tags,
      entryDate: parsed.values.date,
    });

    output(result, json, ({ entry, dbPath }) => {
      console.log(`已新增 ${entry.entryType} #${entry.id}：${numberText(entry.amount)} ${entry.description}`);
      console.log(`資料庫：${dbPath}`);
    });
    return;
  }

  if (command === "list") {
    const parsed = parseCommandArgs(args, {
      options: {
        period: { type: "string" },
        "start-date": { type: "string" },
        "end-date": { type: "string" },
        "entry-type": { type: "string" },
        category: { type: "string" },
        limit: { type: "string" },
      },
    });

    const result = listLedgerEntries({
      period: parsed.values.period,
      startDate: parsed.values["start-date"],
      endDate: parsed.values["end-date"],
      entryType: parsed.values["entry-type"],
      category: parsed.values.category,
      limit: parsed.values.limit ? toInteger(parsed.values.limit, "limit") : undefined,
    });

    output(result, json, printLedgerList);
    return;
  }

  if (command === "report") {
    const parsed = parseCommandArgs(args, {
      options: {
        period: { type: "string" },
        "start-date": { type: "string" },
        "end-date": { type: "string" },
      },
    });

    const result = getLedgerReport({
      period: parsed.values.period,
      startDate: parsed.values["start-date"],
      endDate: parsed.values["end-date"],
    });

    output(result, json, printLedgerReport);
    return;
  }

  if (command === "delete") {
    requirePositionals(args, 1, "wealth ledger delete <id>");
    const result = deleteLedgerEntry({ entryId: toInteger(args[0], "id") });
    output(result, json, ({ deleted }) => {
      console.log(`已刪除 #${deleted.id}：${numberText(deleted.amount)} ${deleted.description}`);
    });
    return;
  }

  throw new Error("Unsupported ledger command.");
}

async function handleInvest(command, args, json) {
  if (["buy", "sell"].includes(command)) {
    const parsed = parseCommandArgs(args, {
      options: {
        fee: { type: "string" },
        "asset-class": { type: "string" },
        market: { type: "string" },
        note: { type: "string" },
        date: { type: "string" },
      },
    });

    requirePositionals(parsed.positionals, 3, `wealth invest ${command} <symbol> <quantity> <unitPrice>`);
    const [symbol, quantity, unitPrice] = parsed.positionals;
    const result = recordInvestmentTrade({
      tradeType: command,
      symbol,
      quantity: toPositiveNumber(quantity, "quantity"),
      unitPrice: toNumber(unitPrice, "unitPrice"),
      fee: parsed.values.fee ? toNumber(parsed.values.fee, "fee") : 0,
      assetClass: parsed.values["asset-class"] ?? (symbol.toUpperCase() === "BTC" ? "crypto" : "stock"),
      market: parsed.values.market ?? "",
      note: parsed.values.note ?? "",
      tradeDate: parsed.values.date,
    });

    output(result, json, ({ trade }) => {
      console.log(`已記錄 ${trade.tradeType} #${trade.id}：${trade.symbol} ${quantityText(trade.quantity)} @ ${numberText(trade.unitPrice)}${trade.market ? ` (${trade.market})` : ""}`);
    });
    return;
  }

  if (command === "price") {
    const priceCommand = args[0];
    const rest = args.slice(1);

    if (priceCommand === "set") {
      const parsed = parseCommandArgs(rest, {
        options: {
          currency: { type: "string" },
          source: { type: "string" },
          date: { type: "string" },
        },
      });

      requirePositionals(parsed.positionals, 2, "wealth invest price set <symbol> <price>");
      const [symbol, price] = parsed.positionals;
      const result = setMarketPrice({
        symbol,
        price: toNumber(price, "price"),
        currency: parsed.values.currency ?? "",
        source: parsed.values.source ?? "manual",
        asOf: parsed.values.date,
      });

      output(result, json, ({ price: saved }) => {
        console.log(`已更新價格：${saved.symbol} = ${numberText(saved.price, 6)} ${saved.currency}`);
      });
      return;
    }

    if (priceCommand === "refresh") {
      const parsed = parseCommandArgs(rest, {
        options: {
          symbol: { type: "string" },
        },
      });

      const result = await refreshMarketPrices({ symbol: parsed.values.symbol });
      output(result, json, (payload) => {
        if (payload.updated.length === 0 && payload.failed.length === 0) {
          console.log("沒有可更新的 BTC 標的。");
          return;
        }

        if (payload.updated.length > 0) {
          console.log(renderTable(
            ["標的", "交易對", "價格", "幣別", "來源", "日期"],
            payload.updated.map((item) => [
              item.symbol,
              item.resolvedSymbol,
              numberText(item.price, 6),
              item.currency,
              item.source,
              item.asOf,
            ]),
          ));
        }

        if (payload.failed.length > 0) {
          console.log("");
          console.log(renderTable(
            ["標的", "錯誤"],
            payload.failed.map((item) => [item.symbol, item.error]),
          ));
        }
      });
      return;
    }

    throw new Error("Usage: wealth invest price <set|refresh> ...");
  }

  if (command === "trades") {
    const parsed = parseCommandArgs(args, {
      options: {
        symbol: { type: "string" },
        limit: { type: "string" },
      },
    });

    const result = listInvestmentActivity({
      symbol: parsed.values.symbol,
      limit: parsed.values.limit ? toInteger(parsed.values.limit, "limit") : undefined,
    });
    output(result, json, printInvestmentActivity);
    return;
  }

  if (command === "portfolio") {
    const result = getPortfolioSummary();
    output(result, json, printPortfolio);
    return;
  }

  throw new Error("Unsupported invest command.");
}

async function handleMarket(command, args, json) {
  if (command !== "btc") {
    throw new Error("Usage: wealth market btc [--save]");
  }

  const parsed = parseCommandArgs(args, {
    options: {
      save: { type: "boolean" },
    },
  });

  const result = await getBtcRealtimeInfo({ save: parsed.values.save ?? false });
  output(result, json, ({ snapshot, saved }) => {
    console.log(`BTC 即時資訊 (${snapshot.resolvedSymbol})`);
    console.log(`最新價 ${numberText(snapshot.price, 6)} ${snapshot.currency}`);
    console.log(`24h 漲跌 ${signedNumberText(snapshot.priceChange, 6)} (${percentText(snapshot.priceChangePercent)})`);
    console.log(`24h 高低 ${numberText(snapshot.highPrice, 6)} / ${numberText(snapshot.lowPrice, 6)}`);
    console.log(`24h 成交量 ${numberText(snapshot.volume, 6)} BTC | 成交額 ${numberText(snapshot.quoteVolume, 2)} ${snapshot.currency}`);
    console.log(`時間區間 ${snapshot.openTime} ~ ${snapshot.closeTime}`);
    if (saved) {
      console.log("已同步寫入 market_prices。");
    }
  });
}

async function handleRecurring(command, args, json) {
  if (command === "set") {
    const parsed = parseCommandArgs(args, {
      options: {
        "quote-currency": { type: "string" },
        timezone: { type: "string" },
        "asset-class": { type: "string" },
        market: { type: "string" },
        "price-source": { type: "string" },
        note: { type: "string" },
        enabled: { type: "string" },
      },
    });

    requirePositionals(parsed.positionals, 3, "wealth recurring set <symbol> <budgetAmount> <runTime>");
    const [symbol, budgetAmount, runTime] = parsed.positionals;
    const result = upsertRecurringInvestmentPlan({
      symbol,
      budgetAmount: toPositiveNumber(budgetAmount, "budgetAmount"),
      runTime,
      quoteCurrency: parsed.values["quote-currency"] ?? "USDT",
      timeZone: parsed.values.timezone ?? "Asia/Taipei",
      assetClass: parsed.values["asset-class"] ?? "crypto",
      market: parsed.values.market ?? "BINANCE",
      priceSource: parsed.values["price-source"] ?? "binance",
      note: parsed.values.note ?? "",
      enabled: parsed.values.enabled === undefined ? true : parseBoolean(parsed.values.enabled, "enabled"),
    });

    output(result, json, ({ plan }) => {
      console.log(`已儲存定投計畫 #${plan.id}：${plan.symbol} ${numberText(plan.budgetAmount)} ${plan.quoteCurrency} @ ${plan.runTime}`);
    });
    return;
  }

  if (command === "list") {
    const parsed = parseCommandArgs(args, {
      options: {
        enabled: { type: "string" },
      },
    });

    const plans = listRecurringInvestmentPlans({
      enabled: parsed.values.enabled === undefined ? undefined : parseBoolean(parsed.values.enabled, "enabled"),
    });
    output({ plans }, json, printRecurringPlans);
    return;
  }

  if (command === "run") {
    const result = await runDueRecurringInvestmentPlans();
    output(result, json, (payload) => {
      if (payload.executed.length === 0) {
        console.log("本次沒有執行任何定投。");
      } else {
        console.log(renderTable(
          ["計畫", "標的", "數量", "價格", "預算", "交易", "排程時間"],
          payload.executed.map((item) => [
            item.planId,
            item.symbol,
            quantityText(item.quantity, 8),
            numberText(item.price, 6),
            `${numberText(item.budgetAmount)} ${item.quoteCurrency}`,
            item.tradeId,
            item.scheduledFor,
          ]),
        ));
      }

      if (payload.failed.length > 0) {
        console.log("");
        console.log(renderTable(
          ["計畫", "標的", "排程時間", "原因"],
          payload.failed.map((item) => [item.planId, item.symbol, item.scheduledFor, item.reason]),
        ));
      }
    });
    return;
  }

  throw new Error("Unsupported recurring command.");
}

async function main() {
  const { args, json } = extractGlobalFlags(process.argv.slice(2));

  if (args.length === 0 || ["help", "--help", "-h"].includes(args[0])) {
    printHelp();
    return;
  }

  const [area, command, ...rest] = args;

  if (area === "init") {
    const result = initDatabase();
    output(result, json, ({ dbPath }) => {
      console.log(`資料庫已初始化：${dbPath}`);
    });
    return;
  }

  if (area === "db" && command === "path") {
    output({ dbPath: String(getDatabasePath()) }, json, ({ dbPath }) => {
      console.log(dbPath);
    });
    return;
  }

  if (area === "ledger") {
    await handleLedger(command, rest, json);
    return;
  }

  if (area === "invest") {
    await handleInvest(command, rest, json);
    return;
  }

  if (area === "market") {
    await handleMarket(command, rest, json);
    return;
  }

  if (area === "recurring") {
    await handleRecurring(command, rest, json);
    return;
  }

  throw new Error("Unknown command. Run 'wealth help' to see available commands.");
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`Error: ${message}`);
  process.exitCode = 1;
});
