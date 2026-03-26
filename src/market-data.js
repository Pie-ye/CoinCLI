const HTTP_HEADERS = {
  Accept: "application/json",
  "User-Agent": "CLI-Wealth/0.1",
};

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function toNumber(value) {
  const amount = Number(value);
  return Number.isFinite(amount) ? amount : null;
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: HTTP_HEADERS });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`.trim());
  }
  return response.json();
}

function normalizeSupportedSymbol(symbol) {
  const normalized = String(symbol).trim().toUpperCase();
  if (normalized === "BTC" || normalized === "BTCUSDT") {
    return normalized;
  }
  throw new Error("Automatic market price refresh currently supports BTC only.");
}

export async function fetchLatestMarketPrice(target) {
  const normalized = normalizeSupportedSymbol(target.symbol);
  const payload = await fetchJson("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT");
  const price = toNumber(payload?.price);
  if (price === null) {
    throw new Error("Binance response did not include a numeric BTCUSDT price.");
  }

  return {
    symbol: normalized,
    resolvedSymbol: "BTCUSDT",
    price,
    currency: "USDT",
    source: "binance",
    asOf: todayIso(),
  };
}
