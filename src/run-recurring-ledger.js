#!/usr/bin/env node

import process from "node:process";

import { runDueRecurringLedgerPlans } from "./wealth-service.js";

function printResult(result) {
  if (result.executed.length === 0) {
    console.log("No recurring ledger plans were executed.");
  } else {
    for (const item of result.executed) {
      console.log(
        [
          `Executed plan #${item.planId}`,
          `${item.entryType} ${item.amount}`,
          item.category,
          item.description,
          `entry #${item.ledgerEntryId}`,
          `scheduled ${item.scheduledFor}`,
        ].join(" | "),
      );
    }
  }

  for (const item of result.skipped) {
    console.log(
      [
        `Skipped plan #${item.planId}`,
        item.scheduledFor,
        item.reason,
      ].join(" | "),
    );
  }

  for (const item of result.failed) {
    console.log(
      [
        `Failed plan #${item.planId}`,
        item.scheduledFor,
        item.reason,
      ].join(" | "),
    );
  }
}

async function main() {
  try {
    const result = runDueRecurringLedgerPlans();
    printResult(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("Recurring ledger runner failed.");
    console.error(message);
    process.exitCode = 1;
  }
}

await main();
