const state = {
  ledgerEntries: [],
  ledgerReport: null,
  recurringLedgerPlans: [],
  ledgerFilters: {
    category: "",
    startDate: "",
    endDate: "",
  },
  portfolio: null,
  investmentActivity: null,
  btc: null,
  btcError: null,
};

const elements = {
  feedbackBanner: document.querySelector("#feedback-banner"),
  summaryIncome: document.querySelector("#summary-income"),
  summaryExpense: document.querySelector("#summary-expense"),
  summaryNet: document.querySelector("#summary-net"),
  summaryBtcPrice: document.querySelector("#summary-btc-price"),
  summaryBtcChange: document.querySelector("#summary-btc-change"),
  investmentBtcPrice: document.querySelector("#investment-btc-price"),
  ledgerReportBody: document.querySelector("#ledger-report-body"),
  ledgerTableBody: document.querySelector("#ledger-table-body"),
  recurringLedgerTableBody: document.querySelector("#recurring-ledger-table-body"),
  ledgerFilterForm: document.querySelector("#ledger-filter-form"),
  clearLedgerFiltersButton: document.querySelector("#clear-ledger-filters"),
  portfolioCapitalIn: document.querySelector("#portfolio-capital-in"),
  portfolioOpenCost: document.querySelector("#portfolio-open-cost"),
  portfolioMarketValue: document.querySelector("#portfolio-market-value"),
  portfolioTotalReturn: document.querySelector("#portfolio-total-return"),
  portfolioTableBody: document.querySelector("#portfolio-table-body"),
  tradesTableBody: document.querySelector("#trades-table-body"),
  ledgerForm: document.querySelector("#ledger-form"),
  recurringLedgerForm: document.querySelector("#recurring-ledger-form"),
  tradeForm: document.querySelector("#trade-form"),
  initDbButton: document.querySelector("#init-db-button"),
  refreshAllButton: document.querySelector("#refresh-all-button"),
  runRecurringLedgerButton: document.querySelector("#run-recurring-ledger-button"),
  refreshBtcButton: document.querySelector("#refresh-btc-button"),
  refreshLedgerButton: document.querySelector("#refresh-ledger-button"),
  refreshPortfolioButton: document.querySelector("#refresh-portfolio-button"),
  refreshActivityButton: document.querySelector("#refresh-activity-button"),
  recurringScheduleType: document.querySelector("#recurring-schedule-type"),
  recurringDayOfMonthField: document.querySelector("#recurring-day-of-month-field"),
  tabs: [...document.querySelectorAll(".tab")],
  panels: [...document.querySelectorAll(".tab-panel")],
};

function todayInputValue() {
  return new Date().toISOString().slice(0, 10);
}

function buildQueryString(params) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  }
  const text = query.toString();
  return text ? `?${text}` : "";
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  return Number(value).toLocaleString("zh-TW", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatSignedNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  const amount = Number(value);
  return `${amount >= 0 ? "+" : ""}${formatNumber(amount, digits)}`;
}

function formatPercent(value, digits = 2) {
  const formatted = formatSignedNumber(value, digits);
  return formatted === "—" ? formatted : `${formatted}%`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function showFeedback(message, type = "success") {
  elements.feedbackBanner.textContent = message;
  elements.feedbackBanner.className = `feedback-banner ${type}`;
  window.clearTimeout(showFeedback.timeoutId);
  showFeedback.timeoutId = window.setTimeout(() => {
    elements.feedbackBanner.className = "feedback-banner hidden";
    elements.feedbackBanner.textContent = "";
  }, 3200);
}

async function apiFetch(path, options = {}) {
  let response;
  try {
    response = await fetch(path, {
      headers: {
        "Content-Type": "application/json",
        ...(options.headers ?? {}),
      },
      ...options,
    });
  } catch {
    throw new Error("fetch failed：請確認 API 服務已在專案目錄啟動。");
  }

  const hasJson = response.headers.get("content-type")?.includes("application/json");
  const payload = hasJson ? await response.json() : null;
  if (!response.ok) {
    throw new Error(payload?.error?.message ?? `Request failed: ${response.status}`);
  }
  return payload;
}

function renderRecurringLedgerPlans() {
  if (state.recurringLedgerPlans.length === 0) {
    elements.recurringLedgerTableBody.innerHTML = '<tr><td colspan="8" class="empty">尚無定期收支計畫</td></tr>';
    return;
  }

  elements.recurringLedgerTableBody.innerHTML = state.recurringLedgerPlans
    .map(
      (plan) => `
        <tr>
          <td>${plan.id}</td>
          <td>${escapeHtml(plan.entryType)}</td>
          <td>${escapeHtml(plan.description)}</td>
          <td>${escapeHtml(plan.category)}</td>
          <td>${formatNumber(plan.amount)}</td>
          <td>${escapeHtml(plan.scheduleType === "monthly" ? `每月 ${plan.dayOfMonth} 日 ${plan.runTime}` : `每日 ${plan.runTime}`)}</td>
          <td>${escapeHtml(plan.lastStatus === "success" ? `${plan.nextOccurrence}（上次成功）` : plan.nextOccurrence)}</td>
          <td><button class="delete-button" data-recurring-plan-id="${plan.id}">刪除</button></td>
        </tr>
      `,
    )
    .join("");
}

function renderLedgerSummary() {
  const report = state.ledgerReport;
  elements.summaryIncome.textContent = report ? formatNumber(report.totalIncome) : "—";
  elements.summaryExpense.textContent = report ? formatNumber(report.totalExpense) : "—";
  elements.summaryNet.textContent = report ? formatSignedNumber(report.net) : "—";

  if (!report || report.categories.length === 0) {
    elements.ledgerReportBody.innerHTML = '<tr><td colspan="4" class="empty">尚無資料</td></tr>';
    return;
  }

  elements.ledgerReportBody.innerHTML = report.categories
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row.category)}</td>
          <td>${formatNumber(row.income)}</td>
          <td>${formatNumber(row.expense)}</td>
          <td>${formatSignedNumber(row.net)}</td>
        </tr>
      `,
    )
    .join("");
}

function renderLedgerEntries() {
  if (state.ledgerEntries.length === 0) {
    elements.ledgerTableBody.innerHTML = '<tr><td colspan="8" class="empty">尚無資料</td></tr>';
    return;
  }

  elements.ledgerTableBody.innerHTML = state.ledgerEntries
    .map(
      (entry) => `
        <tr>
          <td>${entry.id}</td>
          <td>${escapeHtml(entry.date)}</td>
          <td>${escapeHtml(entry.entryType)}</td>
          <td>${escapeHtml(entry.category)}</td>
          <td>${formatNumber(entry.amount)}</td>
          <td>${escapeHtml(entry.description)}</td>
          <td>${escapeHtml(entry.tags.join(", "))}</td>
          <td><button class="delete-button" data-entry-id="${entry.id}">刪除</button></td>
        </tr>
      `,
    )
    .join("");
}

function renderBtcSummary() {
  const snapshot = state.btc?.snapshot;
  if (!snapshot) {
    elements.summaryBtcPrice.textContent = "—";
    elements.summaryBtcChange.textContent = state.btcError ?? "等待資料";
    elements.investmentBtcPrice.textContent = state.btcError ? "暫時無法取得" : "—";
    return;
  }

  elements.summaryBtcPrice.textContent = `${formatNumber(snapshot.price, 2)} ${snapshot.currency}`;
  if (snapshot.priceChange === null && snapshot.priceChangePercent === null) {
    elements.summaryBtcChange.textContent = `來源：${snapshot.source}`;
  } else if (snapshot.priceChange === null) {
    elements.summaryBtcChange.textContent = `${formatPercent(snapshot.priceChangePercent, 2)} | ${snapshot.source}`;
  } else {
    elements.summaryBtcChange.textContent = `${formatSignedNumber(snapshot.priceChange, 2)} (${formatPercent(snapshot.priceChangePercent, 2)})`;
  }
  elements.investmentBtcPrice.textContent = `${formatNumber(snapshot.price, 2)} ${snapshot.currency}`;
}

function renderPortfolio() {
  const portfolio = state.portfolio;
  if (!portfolio) {
    elements.portfolioCapitalIn.textContent = "—";
    elements.portfolioOpenCost.textContent = "—";
    elements.portfolioMarketValue.textContent = "—";
    elements.portfolioTotalReturn.textContent = "—";
    elements.portfolioTableBody.innerHTML = '<tr><td colspan="8" class="empty">尚無投資資料</td></tr>';
    return;
  }

  elements.portfolioCapitalIn.textContent = formatNumber(portfolio.totals.capitalIn);
  elements.portfolioOpenCost.textContent = formatNumber(portfolio.totals.openCost);
  elements.portfolioMarketValue.textContent = portfolio.totals.marketValue === null ? "缺價格" : formatNumber(portfolio.totals.marketValue);
  elements.portfolioTotalReturn.textContent = portfolio.totals.totalReturn === null ? "缺價格" : formatSignedNumber(portfolio.totals.totalReturn);

  if (portfolio.positions.length === 0) {
    elements.portfolioTableBody.innerHTML = '<tr><td colspan="8" class="empty">尚無投資資料</td></tr>';
    return;
  }

  elements.portfolioTableBody.innerHTML = portfolio.positions
    .map(
      (position) => `
        <tr>
          <td>${escapeHtml(position.symbol)}</td>
          <td>${formatNumber(position.quantity, 6)}</td>
          <td>${formatNumber(position.remainingCost)}</td>
          <td>${position.price === null ? "—" : formatNumber(position.price, 6)}</td>
          <td>${position.marketValue === null ? "—" : formatNumber(position.marketValue)}</td>
          <td>${position.unrealizedPnl === null ? "—" : formatSignedNumber(position.unrealizedPnl)}</td>
          <td>${formatSignedNumber(position.realizedPnl)}</td>
          <td>${formatPercent(position.roiPct)}</td>
        </tr>
      `,
    )
    .join("");
}

function renderInvestmentActivity() {
  const activity = state.investmentActivity;
  if (!activity || activity.trades.length === 0) {
    elements.tradesTableBody.innerHTML = '<tr><td colspan="6" class="empty">尚無交易資料</td></tr>';
  } else {
    elements.tradesTableBody.innerHTML = activity.trades
      .map(
        (trade) => `
          <tr>
            <td>${trade.id}</td>
            <td>${escapeHtml(trade.tradeDate)}</td>
            <td>${escapeHtml(trade.tradeType)}</td>
            <td>${escapeHtml(trade.symbol)}</td>
            <td>${formatNumber(trade.quantity, 6)}</td>
            <td>${formatNumber(trade.unitPrice, 2)}</td>
          </tr>
        `,
      )
      .join("");
  }
}

async function loadLedgerData() {
  const entryQuery = buildQueryString({
    limit: 50,
    category: state.ledgerFilters.category,
    startDate: state.ledgerFilters.startDate,
    endDate: state.ledgerFilters.endDate,
  });
  const reportQuery = buildQueryString({
    period: state.ledgerFilters.startDate || state.ledgerFilters.endDate || state.ledgerFilters.category ? undefined : "month",
    category: state.ledgerFilters.category,
    startDate: state.ledgerFilters.startDate,
    endDate: state.ledgerFilters.endDate,
  });

  const [entries, report, recurringPlans] = await Promise.all([
    apiFetch(`/api/ledger/entries${entryQuery}`),
    apiFetch(`/api/ledger/report${reportQuery}`),
    apiFetch("/api/ledger/recurring-plans"),
  ]);
  state.ledgerEntries = entries.entries;
  state.ledgerReport = report;
  state.recurringLedgerPlans = recurringPlans.plans;
  renderLedgerEntries();
  renderLedgerSummary();
  renderRecurringLedgerPlans();
}

function syncLedgerFiltersFromForm() {
  const payload = normalizeBlankFields(formToObject(elements.ledgerFilterForm));
  state.ledgerFilters = {
    category: payload.category ?? "",
    startDate: payload.startDate ?? "",
    endDate: payload.endDate ?? "",
  };
}

async function loadInvestmentData() {
  const [portfolio, activity] = await Promise.all([
    apiFetch("/api/investments/portfolio"),
    apiFetch("/api/investments/activity?limit=20"),
  ]);
  state.portfolio = portfolio;
  state.investmentActivity = activity;
  renderPortfolio();
  renderInvestmentActivity();
}

async function loadBtcData() {
  try {
    state.btc = await apiFetch("/api/market/btc/realtime");
    state.btcError = null;
  } catch (error) {
    state.btc = null;
    state.btcError = error.message;
  }
  renderBtcSummary();
}

async function refreshAllData() {
  const results = await Promise.allSettled([loadLedgerData(), loadInvestmentData(), loadBtcData()]);
  const failed = results.find((result) => result.status === "rejected");
  if (failed) {
    showFeedback(failed.reason.message, "error");
  }
}

function syncRecurringScheduleFields() {
  const isMonthly = elements.recurringScheduleType.value === "monthly";
  elements.recurringDayOfMonthField.classList.toggle("hidden", !isMonthly);
}

function setDefaultDates() {
  const today = todayInputValue();
  elements.ledgerForm.elements.entryDate.value = today;
  elements.tradeForm.elements.tradeDate.value = today;
  elements.recurringLedgerForm.elements.startDate.value = today;
  elements.recurringLedgerForm.elements.runTime.value = elements.recurringLedgerForm.elements.runTime.value || "09:00";
}

function switchTab(tabName) {
  elements.tabs.forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.tab === tabName);
  });
  elements.panels.forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.panel === tabName);
  });
}

function formToObject(formElement) {
  const formData = new FormData(formElement);
  return Object.fromEntries(formData.entries());
}

function normalizeBlankFields(payload) {
  return Object.fromEntries(
    Object.entries(payload).filter(([, value]) => value !== ""),
  );
}

async function handleLedgerSubmit(event) {
  event.preventDefault();
  const payload = normalizeBlankFields(formToObject(elements.ledgerForm));
  payload.amount = Number(payload.amount);

  try {
    await apiFetch("/api/ledger/entries", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    elements.ledgerForm.reset();
    setDefaultDates();
    await loadLedgerData();
    showFeedback("收支資料已新增。", "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

async function handleTradeSubmit(event) {
  event.preventDefault();
  const payload = normalizeBlankFields(formToObject(elements.tradeForm));
  payload.quantity = Number(payload.quantity);
  payload.unitPrice = Number(payload.unitPrice);
  payload.fee = Number(payload.fee ?? 0);

  try {
    await apiFetch("/api/investments/trades", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    elements.tradeForm.reset();
    setDefaultDates();
    await Promise.all([loadInvestmentData(), loadBtcData()]);
    showFeedback("投資交易已新增。", "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

async function handleRecurringLedgerSubmit(event) {
  event.preventDefault();
  const payload = normalizeBlankFields(formToObject(elements.recurringLedgerForm));
  payload.amount = Number(payload.amount);
  if (payload.dayOfMonth !== undefined) {
    payload.dayOfMonth = Number(payload.dayOfMonth);
  }

  try {
    await apiFetch("/api/ledger/recurring-plans", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    elements.recurringLedgerForm.reset();
    setDefaultDates();
    syncRecurringScheduleFields();
    await loadLedgerData();
    showFeedback("定期收支計畫已新增。", "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

async function handleRecurringLedgerRun() {
  try {
    const result = await apiFetch("/api/ledger/recurring-plans/run", { method: "POST" });
    await loadLedgerData();
    showFeedback(`定期收支已執行 ${result.executed.length} 筆。`, "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

async function handleLedgerFilterSubmit(event) {
  event.preventDefault();
  syncLedgerFiltersFromForm();
  try {
    await loadLedgerData();
    showFeedback("查詢條件已套用。", "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

async function handleLedgerFilterClear() {
  elements.ledgerFilterForm.reset();
  syncLedgerFiltersFromForm();
  try {
    await loadLedgerData();
    showFeedback("查詢條件已清除。", "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

async function handleLedgerDeleteClick(event) {
  const button = event.target.closest("[data-entry-id]");
  if (button) {
    const entryId = button.dataset.entryId;
    const confirmed = window.confirm(`確定刪除收支紀錄 #${entryId}？`);
    if (!confirmed) {
      return;
    }

    try {
      await apiFetch(`/api/ledger/entries/${entryId}`, { method: "DELETE" });
      await loadLedgerData();
      showFeedback(`收支紀錄 #${entryId} 已刪除。`, "success");
    } catch (error) {
      showFeedback(error.message, "error");
    }
    return;
  }

  const planButton = event.target.closest("[data-recurring-plan-id]");
  if (!planButton) {
    return;
  }

  const planId = planButton.dataset.recurringPlanId;
  const confirmed = window.confirm(`確定刪除定期收支計畫 #${planId}？`);
  if (!confirmed) {
    return;
  }

  try {
    await apiFetch(`/api/ledger/recurring-plans/${planId}`, { method: "DELETE" });
    await loadLedgerData();
    showFeedback(`定期收支計畫 #${planId} 已刪除。`, "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

async function handleInitDatabase() {
  try {
    await apiFetch("/api/init", { method: "POST" });
    await refreshAllData();
    showFeedback("資料庫初始化完成。", "success");
  } catch (error) {
    showFeedback(error.message, "error");
  }
}

function bindEvents() {
  elements.tabs.forEach((tab) => {
    tab.addEventListener("click", () => switchTab(tab.dataset.tab));
  });

  elements.ledgerForm.addEventListener("submit", handleLedgerSubmit);
  elements.recurringLedgerForm.addEventListener("submit", handleRecurringLedgerSubmit);
  elements.tradeForm.addEventListener("submit", handleTradeSubmit);
  elements.ledgerFilterForm.addEventListener("submit", handleLedgerFilterSubmit);
  elements.clearLedgerFiltersButton.addEventListener("click", handleLedgerFilterClear);
  elements.ledgerTableBody.addEventListener("click", handleLedgerDeleteClick);
  elements.recurringLedgerTableBody.addEventListener("click", handleLedgerDeleteClick);
  elements.recurringScheduleType.addEventListener("change", syncRecurringScheduleFields);

  elements.initDbButton.addEventListener("click", handleInitDatabase);
  elements.refreshAllButton.addEventListener("click", refreshAllData);
  elements.runRecurringLedgerButton.addEventListener("click", handleRecurringLedgerRun);
  elements.refreshBtcButton.addEventListener("click", async () => {
    try {
      await loadBtcData();
      showFeedback("BTC 即時價格已更新。", "success");
    } catch (error) {
      showFeedback(error.message, "error");
    }
  });
  elements.refreshLedgerButton.addEventListener("click", async () => {
    try {
      await loadLedgerData();
      showFeedback("收支列表已更新。", "success");
    } catch (error) {
      showFeedback(error.message, "error");
    }
  });
  elements.refreshPortfolioButton.addEventListener("click", async () => {
    try {
      await loadInvestmentData();
      showFeedback("投組摘要已更新。", "success");
    } catch (error) {
      showFeedback(error.message, "error");
    }
  });
  elements.refreshActivityButton.addEventListener("click", async () => {
    try {
      await loadInvestmentData();
      showFeedback("投資活動已更新。", "success");
    } catch (error) {
      showFeedback(error.message, "error");
    }
  });
}

async function main() {
  setDefaultDates();
  syncRecurringScheduleFields();
  bindEvents();
  await refreshAllData();
}

main();
