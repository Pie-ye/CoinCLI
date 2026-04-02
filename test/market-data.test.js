import test from "node:test";
import assert from "node:assert/strict";

import { fetchBtcRealtimeSnapshot, fetchLatestMarketPrice } from "../src/market-data.js";

test("fetchBtcRealtimeSnapshot 會回傳可用於分析的 BTC 即時資料", async () => {
  const snapshot = await fetchBtcRealtimeSnapshot({
    fetchImpl: async () => ({
      ok: true,
      async json() {
        return {
          symbol: "BTCUSDT",
          priceChange: "123.45",
          priceChangePercent: "1.23",
          weightedAvgPrice: "69000.12",
          lastPrice: "70000.50",
          openPrice: "69800.00",
          highPrice: "71000.00",
          lowPrice: "68000.00",
          volume: "1234.56789",
          quoteVolume: "85000000.12",
          openTime: 1719990000000,
          closeTime: 1719993600000,
          count: 987654,
        };
      },
    }),
  });

  assert.equal(snapshot.symbol, "BTC");
  assert.equal(snapshot.resolvedSymbol, "BTCUSDT");
  assert.equal(snapshot.currency, "USDT");
  assert.equal(snapshot.price, 70000.5);
  assert.equal(snapshot.highPrice, 71000);
  assert.equal(snapshot.lowPrice, 68000);
  assert.equal(snapshot.priceChangePercent, 1.23);
  assert.equal(snapshot.count, 987654);
  assert.match(snapshot.openTime, /^\d{4}-\d{2}-\d{2}T/);
  assert.match(snapshot.closeTime, /^\d{4}-\d{2}-\d{2}T/);
});

test("fetchBtcRealtimeSnapshot 在 Binance 失敗時會 fallback 到 Coinbase", async () => {
  const calls = [];
  const snapshot = await fetchBtcRealtimeSnapshot({
    fetchImpl: async (url) => {
      const target = String(url);
      calls.push(target);

      if (target.includes("api.binance.com")) {
        return {
          ok: false,
          status: 400,
          statusText: "Bad Request",
        };
      }

      assert.match(target, /api\.coinbase\.com\/v2\/prices\/BTC-USD\/spot/);
      return {
        ok: true,
        async json() {
          return {
            data: {
              amount: "69321.88",
            },
          };
        },
      };
    },
  });

  assert.equal(snapshot.source, "coinbase");
  assert.equal(snapshot.currency, "USD");
  assert.equal(snapshot.price, 69321.88);
  assert.equal(calls.length >= 2, true);
});

test("fetchLatestMarketPrice 在 Binance 失敗時會 fallback 到 CoinGecko", async () => {
  const originalFetch = globalThis.fetch;
  let binanceCalled = false;

  globalThis.fetch = async (url) => {
    const target = String(url);
    if (target.includes("api.binance.com")) {
      binanceCalled = true;
      return {
        ok: false,
        status: 400,
        statusText: "Bad Request",
      };
    }

    if (target.includes("api.coinbase.com")) {
      return {
        ok: false,
        status: 503,
        statusText: "Service Unavailable",
      };
    }

    assert.match(target, /api\.coingecko\.com/);
    return {
      ok: true,
      async json() {
        return {
          bitcoin: {
            usd: 70111.33,
            last_updated_at: 1775097600,
          },
        };
      },
    };
  };

  try {
    const quote = await fetchLatestMarketPrice({ symbol: "BTC" });
    assert.equal(binanceCalled, true);
    assert.equal(quote.source, "coingecko");
    assert.equal(quote.currency, "USD");
    assert.equal(quote.price, 70111.33);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
