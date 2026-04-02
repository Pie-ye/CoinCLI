const HTTP_HEADERS = {
  Accept: "application/json",
  "User-Agent": "CLI-Wealth/0.1",
};

const COINBASE_HEADERS = {
  Accept: "application/json",
  "User-Agent": "CLI-Wealth/0.1",
};

const BINANCE_SYMBOL = "BTCUSDT";
const DAY_INTERVAL = "1d";
const MAX_KLINE_LIMIT = 1000;

function todayIso() {
  return new Date().toISOString().slice(0, 10);
}

function toNumber(value) {
  const amount = Number(value);
  return Number.isFinite(amount) ? amount : null;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetriableStatus(status) {
  return status === 429 || status >= 500;
}

async function fetchJson(url, options = {}) {
  const {
    fetchImpl = globalThis.fetch,
    retries = 3,
    baseDelayMs = 250,
    maxDelayMs = 3000,
    headers = HTTP_HEADERS,
  } = options;

  if (typeof fetchImpl !== "function") {
    throw new Error("Fetch API is unavailable in the current runtime.");
  }

  let attempt = 0;

  while (true) {
    const response = await fetchImpl(url, { headers });
    if (response.ok) {
      return response.json();
    }

    if (attempt >= retries || !isRetriableStatus(response.status)) {
      throw new Error(`${response.status} ${response.statusText}`.trim());
    }

    const backoff = Math.min(maxDelayMs, baseDelayMs * (2 ** attempt));
    const jitter = Math.floor(Math.random() * Math.max(50, Math.floor(backoff * 0.25)));
    await delay(backoff + jitter);
    attempt += 1;
  }
}

function normalizeSupportedSymbol(symbol) {
  const normalized = String(symbol).trim().toUpperCase();
  if (normalized === "BTC" || normalized === "BTCUSDT") {
    return normalized;
  }
  throw new Error("Automatic market price refresh currently supports BTC only.");
}

function buildSnapshotFromQuote({
  symbol,
  resolvedSymbol,
  price,
  currency,
  source,
  asOf = todayIso(),
  priceChange = null,
  priceChangePercent = null,
  weightedAvgPrice = null,
  openPrice = null,
  highPrice = null,
  lowPrice = null,
  volume = null,
  quoteVolume = null,
  openTime = null,
  closeTime = null,
  count = null,
}) {
  return {
    symbol,
    resolvedSymbol,
    price,
    currency,
    source,
    asOf,
    priceChange,
    priceChangePercent,
    weightedAvgPrice,
    openPrice,
    highPrice,
    lowPrice,
    volume,
    quoteVolume,
    openTime,
    closeTime,
    count,
  };
}

async function fetchBinanceLatestPrice({ normalized, fetchImpl }) {
  const payload = await fetchJson(`https://api.binance.com/api/v3/ticker/price?symbol=${BINANCE_SYMBOL}`, { fetchImpl });
  const price = toNumber(payload?.price);
  if (price === null) {
    throw new Error(`Binance response did not include a numeric ${BINANCE_SYMBOL} price.`);
  }

  return {
    symbol: normalized,
    resolvedSymbol: BINANCE_SYMBOL,
    price,
    currency: "USDT",
    source: "binance",
    asOf: todayIso(),
  };
}

async function fetchCoinbaseLatestPrice({ normalized, fetchImpl }) {
  const payload = await fetchJson("https://api.coinbase.com/v2/prices/BTC-USD/spot", {
    fetchImpl,
    retries: 1,
    headers: COINBASE_HEADERS,
  });
  const price = toNumber(payload?.data?.amount);
  if (price === null) {
    throw new Error("Coinbase response did not include a numeric BTC-USD price.");
  }

  return {
    symbol: normalized,
    resolvedSymbol: "BTC-USD",
    price,
    currency: "USD",
    source: "coinbase",
    asOf: todayIso(),
  };
}

async function fetchCoinGeckoLatestPrice({ normalized, fetchImpl }) {
  const payload = await fetchJson(
    "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_last_updated_at=true",
    {
      fetchImpl,
      retries: 1,
    },
  );

  const price = toNumber(payload?.bitcoin?.usd);
  if (price === null) {
    throw new Error("CoinGecko response did not include a numeric BTC USD price.");
  }

  return {
    symbol: normalized,
    resolvedSymbol: "bitcoin/usd",
    price,
    currency: "USD",
    source: "coingecko",
    asOf: payload?.bitcoin?.last_updated_at
      ? new Date(Number(payload.bitcoin.last_updated_at) * 1000).toISOString().slice(0, 10)
      : todayIso(),
  };
}

async function fetchBinanceRealtime({ normalized, fetchImpl }) {
  const payload = await fetchJson(`https://api.binance.com/api/v3/ticker/24hr?symbol=${BINANCE_SYMBOL}`, { fetchImpl });

  const price = toNumber(payload?.lastPrice);
  if (price === null) {
    throw new Error(`Binance response did not include a numeric ${BINANCE_SYMBOL} lastPrice.`);
  }

  return buildSnapshotFromQuote({
    symbol: normalized === BINANCE_SYMBOL ? "BTC" : normalized,
    resolvedSymbol: BINANCE_SYMBOL,
    price,
    currency: "USDT",
    source: "binance",
    asOf: todayIso(),
    priceChange: toNumber(payload?.priceChange),
    priceChangePercent: toNumber(payload?.priceChangePercent),
    weightedAvgPrice: toNumber(payload?.weightedAvgPrice),
    openPrice: toNumber(payload?.openPrice),
    highPrice: toNumber(payload?.highPrice),
    lowPrice: toNumber(payload?.lowPrice),
    volume: toNumber(payload?.volume),
    quoteVolume: toNumber(payload?.quoteVolume),
    openTime: payload?.openTime ? new Date(payload.openTime).toISOString() : null,
    closeTime: payload?.closeTime ? new Date(payload.closeTime).toISOString() : null,
    count: toNumber(payload?.count),
  });
}

async function fetchCoinbaseRealtime({ normalized, fetchImpl }) {
  const latest = await fetchCoinbaseLatestPrice({ normalized, fetchImpl });
  return buildSnapshotFromQuote({
    ...latest,
    symbol: normalized === BINANCE_SYMBOL ? "BTC" : normalized,
  });
}

async function fetchCoinGeckoRealtime({ normalized, fetchImpl }) {
  const payload = await fetchJson(
    "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true&include_last_updated_at=true",
    {
      fetchImpl,
      retries: 1,
    },
  );

  const price = toNumber(payload?.bitcoin?.usd);
  if (price === null) {
    throw new Error("CoinGecko response did not include a numeric BTC USD price.");
  }

  return buildSnapshotFromQuote({
    symbol: normalized === BINANCE_SYMBOL ? "BTC" : normalized,
    resolvedSymbol: "bitcoin/usd",
    price,
    currency: "USD",
    source: "coingecko",
    asOf: payload?.bitcoin?.last_updated_at
      ? new Date(Number(payload.bitcoin.last_updated_at) * 1000).toISOString().slice(0, 10)
      : todayIso(),
    priceChangePercent: toNumber(payload?.bitcoin?.usd_24h_change),
  });
}

async function tryFetchProviders(providers) {
  const errors = [];
  for (const provider of providers) {
    try {
      return await provider.run();
    } catch (error) {
      errors.push(`${provider.name}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  throw new Error(`Unable to fetch BTC market data. ${errors.join(" | ")}`);
}

export async function fetchLatestMarketPrice(target) {
  const normalized = normalizeSupportedSymbol(target.symbol);
  return tryFetchProviders([
    { name: "binance", run: () => fetchBinanceLatestPrice({ normalized }) },
    { name: "coinbase", run: () => fetchCoinbaseLatestPrice({ normalized }) },
    { name: "coingecko", run: () => fetchCoinGeckoLatestPrice({ normalized }) },
  ]);
}

export async function fetchBtcRealtimeSnapshot({ symbol = "BTC", fetchImpl } = {}) {
  const normalized = normalizeSupportedSymbol(symbol);
  return tryFetchProviders([
    { name: "binance", run: () => fetchBinanceRealtime({ normalized, fetchImpl }) },
    { name: "coinbase", run: () => fetchCoinbaseRealtime({ normalized, fetchImpl }) },
    { name: "coingecko", run: () => fetchCoinGeckoRealtime({ normalized, fetchImpl }) },
  ]);
}

function normalizeInterval(interval) {
  const normalized = String(interval ?? DAY_INTERVAL).trim();
  const supported = new Set(["1d", "1w", "1M"]);
  if (!supported.has(normalized)) {
    throw new Error(`Unsupported Binance interval: ${normalized}`);
  }
  return normalized;
}

function normalizeKlineRow(symbol, interval, row) {
  return {
    symbol,
    interval,
    openTime: new Date(row[0]).toISOString(),
    openPrice: Number(row[1]),
    highPrice: Number(row[2]),
    lowPrice: Number(row[3]),
    closePrice: Number(row[4]),
    volume: Number(row[5]),
    closeTime: new Date(row[6]).toISOString(),
    quoteVolume: Number(row[7]),
    tradeCount: Number(row[8]),
    takerBuyBaseVolume: Number(row[9]),
    takerBuyQuoteVolume: Number(row[10]),
  };
}

export async function fetchBinanceKlines({
  symbol = BINANCE_SYMBOL,
  interval = DAY_INTERVAL,
  limit = MAX_KLINE_LIMIT,
  startTime,
  endTime,
  fetchImpl,
} = {}) {
  const normalizedInterval = normalizeInterval(interval);
  const query = new URLSearchParams({
    symbol,
    interval: normalizedInterval,
    limit: String(Math.max(1, Math.min(Number(limit) || MAX_KLINE_LIMIT, MAX_KLINE_LIMIT))),
  });

  if (startTime) {
    query.set("startTime", String(startTime));
  }
  if (endTime) {
    query.set("endTime", String(endTime));
  }

  const payload = await fetchJson(`https://api.binance.com/api/v3/klines?${query.toString()}`, { fetchImpl });
  if (!Array.isArray(payload)) {
    throw new Error("Binance kline response was not an array.");
  }

  return payload.map((row) => normalizeKlineRow(symbol, normalizedInterval, row));
}

export async function fetchHistoricalKlines({
  symbol = BINANCE_SYMBOL,
  interval = DAY_INTERVAL,
  years = 5,
  fetchImpl,
  pageLimit = MAX_KLINE_LIMIT,
  delayMs = 150,
} = {}) {
  const normalizedInterval = normalizeInterval(interval);
  const now = Date.now();
  const rangeEnd = now;
  const rangeStart = now - Math.max(1, Number(years)) * 365 * 24 * 60 * 60 * 1000;
  const collected = [];
  let endTime = undefined;

  while (true) {
    const batch = await fetchBinanceKlines({
      symbol,
      interval: normalizedInterval,
      limit: pageLimit,
      endTime,
      fetchImpl,
    });

    if (batch.length === 0) {
      break;
    }

    collected.push(...batch);

    const oldest = batch[0];
    if (Date.parse(oldest.openTime) <= rangeStart || batch.length < pageLimit) {
      break;
    }

    endTime = Date.parse(oldest.openTime) - 1;
    if (delayMs > 0) {
      await delay(delayMs + Math.floor(Math.random() * 100));
    }
  }

  const deduped = new Map();
  for (const item of collected) {
    if (Date.parse(item.openTime) < rangeStart || Date.parse(item.openTime) > rangeEnd) {
      continue;
    }
    deduped.set(item.openTime, item);
  }

  return [...deduped.values()].sort((left, right) => Date.parse(left.openTime) - Date.parse(right.openTime));
}
