import { runDueRecurringLedgerPlans } from "./wealth-service.js";

const DEFAULT_INTERVAL_MS = Math.max(10_000, Number(process.env.WEALTH_LEDGER_SCHEDULER_INTERVAL_MS || 60_000));

export function startRecurringLedgerScheduler({
  intervalMs = DEFAULT_INTERVAL_MS,
  enabled = String(process.env.WEALTH_LEDGER_SCHEDULER_ENABLED ?? "true").trim().toLowerCase() !== "false",
  runOnStart = false,
} = {}) {
  let timer = null;
  let running = false;

  const tick = () => {
    if (running) {
      return;
    }

    running = true;
    Promise.resolve()
      .then(() => runDueRecurringLedgerPlans())
      .catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[recurring-ledger-scheduler] ${message}`);
      })
      .finally(() => {
        running = false;
      });
  };

  if (enabled) {
    if (runOnStart) {
      tick();
    }
    timer = setInterval(tick, intervalMs);
    timer.unref?.();
  }

  return {
    enabled,
    intervalMs,
    stop() {
      if (timer) {
        clearInterval(timer);
        timer = null;
      }
    },
    tick,
  };
}
