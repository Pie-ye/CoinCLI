#!/usr/bin/env node

import process from "node:process";

import { runDueRecurringInvestmentPlans } from "./recurring-investments.js";

function printResult(result) {
  if (result.executed.length === 0) {
    console.log("No recurring investments were executed.");
  } else {
    for (const item of result.executed) {
      console.log(
        [
          `Executed plan #${item.planId}`,
          `${item.symbol} ${item.quantity}`,
          `@ ${item.price} ${item.quoteCurrency}`,
          `budget ${item.budgetAmount} ${item.quoteCurrency}`,
          `trade #${item.tradeId}`,
          `scheduled ${item.scheduledFor}`,
        ].join(" | "),
      );
    }
  }

  if (result.skipped.length > 0) {
    for (const item of result.skipped) {
      console.log(
        [
          `Skipped plan #${item.planId}`,
          item.symbol,
          item.scheduledFor,
          item.reason,
        ].join(" | "),
      );
    }
  }

  if (result.failed.length > 0) {
    for (const item of result.failed) {
      console.log(
        [
          `Failed plan #${item.planId}`,
          item.symbol,
          item.scheduledFor,
          item.reason,
        ].join(" | "),
      );
    }
  }
}

async function main() {
  try {
    const result = await runDueRecurringInvestmentPlans();
    printResult(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Recurring investment runner failed.");
    console.error(message);
    process.exitCode = 1;
  }
}

await main();
