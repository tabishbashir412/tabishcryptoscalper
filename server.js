import 'dotenv/config';
import express from 'express';
import WebSocket from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = Number(process.env.PORT || 3000);
const RESTS = [process.env.BINANCE_REST_URL || 'https://api.binance.com', 'https://data-api.binance.vision'];
const WS = process.env.BINANCE_WS_URL || 'wss://stream.binance.com:9443/stream?streams=!miniTicker@arr';
const cache = new Map();
const tickers = new Map();

const TF = {
  '30m': ['30m', 1], '1h': ['1h', 1], '4h': ['4h', 1],
  '1d': ['1d', 1], '2d': ['1d', 2], '3d': ['1d', 3], '1w': ['1w', 1]
};

function connectBinance() {
  const ws = new WebSocket(WS);
  ws.on('open', () => console.log('Binance live ticker connected'));
  ws.on('message', raw => {
    try {
      const payload = JSON.parse(raw);
      const arr = Array.isArray(payload.data) ? payload.data : [];
      for (const x of arr) {
        if (x.s?.endsWith('USDT')) tickers.set(x.s, {
          symbol: x.s, price: Number(x.c), change: Number(x.P),
          vol: Number(x.q), high: Number(x.h), low: Number(x.l)
        });
      }
    } catch {}
  });
  ws.on('close', () => setTimeout(connectBinance, 3000));
  ws.on('error', () => { try { ws.close(); } catch {} });
}
connectBinance();

async function binance(pathname) {
  let lastErr;
  for (const base of RESTS) {
    try {
      const r = await fetch(base + pathname, { headers: { 'User-Agent': 'Tabish Crypto Scalper/1.0' }, signal: AbortSignal.timeout(12000) });
      if (!r.ok) { lastErr = new Error(`Binance API ${r.status}`); continue; }
      return await r.json();
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error('Binance API unavailable');
}

let tickerRefreshAt = 0;
async function refreshTickersFromRest() {
  if (Date.now() - tickerRefreshAt < 10000 && tickers.size) return;
  const arr = await binance('/api/v3/ticker/24hr');
  if (!Array.isArray(arr)) throw new Error('Invalid Binance ticker response');
  for (const x of arr) {
    if (x.symbol?.endsWith('USDT') && Number(x.lastPrice) > 0) {
      tickers.set(x.symbol, {
        symbol: x.symbol, price: Number(x.lastPrice), change: Number(x.priceChangePercent),
        vol: Number(x.quoteVolume), high: Number(x.highPrice), low: Number(x.lowPrice)
      });
    }
  }
  tickerRefreshAt = Date.now();
}


async function klines(symbol, tf) {
  const [interval, aggregate] = TF[tf];
  const raw = await binance(`/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=180`);
  if (aggregate === 1) return raw;
  const out = [];
  for (let i = 0; i + aggregate <= raw.length; i += aggregate) {
    const c = raw.slice(i, i + aggregate);
    out.push([
      c[0][0], c[0][1], Math.max(...c.map(x => Number(x[2]))),
      Math.min(...c.map(x => Number(x[3]))), c.at(-1)[4],
      c.reduce((sum, x) => sum + Number(x[5]), 0), c.at(-1)[6]
    ]);
  }
  return out;
}

function rsiSeries(closes, period = 14) {
  const out = Array(closes.length).fill(null);
  if (closes.length <= period) return out;
  let gain = 0, loss = 0;
  for (let i = 1; i <= period; i++) {
    const d = closes[i] - closes[i - 1];
    gain += Math.max(d, 0); loss += Math.max(-d, 0);
  }
  let avgGain = gain / period, avgLoss = loss / period;
  out[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  for (let i = period + 1; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + Math.max(d, 0)) / period;
    avgLoss = (avgLoss * (period - 1) + Math.max(-d, 0)) / period;
    out[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }
  return out;
}

function pivots(values, high) {
  const out = [];
  for (let i = 3; i < values.length - 3; i++) {
    let ok = true;
    for (let j = i - 3; j <= i + 3; j++) {
      if (j !== i && (high ? values[j] >= values[i] : values[j] <= values[i])) { ok = false; break; }
    }
    if (ok) out.push(i);
  }
  return out;
}

function average(a) { return a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0; }

function analyse(candles) {
  const closes = candles.map(x => Number(x[4]));
  const highs = candles.map(x => Number(x[2]));
  const lows = candles.map(x => Number(x[3]));
  const rsis = rsiSeries(closes);
  const lowsIdx = pivots(lows, false);
  const highsIdx = pivots(highs, true);
  let regularBull = false, hiddenBull = false, regularBear = false, hiddenBear = false;

  if (lowsIdx.length >= 2) {
    const x = lowsIdx.at(-2), y = lowsIdx.at(-1);
    if (rsis[x] != null && rsis[y] != null) {
      regularBull = closes[y] < closes[x] && rsis[y] > rsis[x];
      hiddenBull = closes[y] > closes[x] && rsis[y] < rsis[x];
    }
  }
  if (highsIdx.length >= 2) {
    const x = highsIdx.at(-2), y = highsIdx.at(-1);
    if (rsis[x] != null && rsis[y] != null) {
      regularBear = closes[y] > closes[x] && rsis[y] < rsis[x];
      hiddenBear = closes[y] < closes[x] && rsis[y] > rsis[x];
    }
  }

  const fast = average(closes.slice(-20));
  const slow = average(closes.slice(-50));
  const trend = fast > slow * 1.01 ? 'Uptrend' : fast < slow * 0.99 ? 'Downtrend' : 'Sideways';
  const rsi = rsis.at(-1) ?? 50;
  let score = 50;
  score += trend === 'Uptrend' ? 15 : trend === 'Downtrend' ? -15 : 0;
  score += rsi < 30 ? 12 : rsi < 40 ? 7 : rsi > 70 ? -12 : rsi > 60 ? -5 : 0;
  score += regularBull ? 15 : regularBear ? -15 : 0;
  score += hiddenBull ? 10 : hiddenBear ? -10 : 0;
  score = Math.max(0, Math.min(100, Math.round(score)));
  const signal = score >= 82 ? 'STRONG BUY' : score >= 68 ? 'BUY' : score <= 25 ? 'STRONG SELL' : score <= 40 ? 'SELL' : 'WATCH';

  return {
    rsi, trend, regularBull, regularBear, hiddenBull, hiddenBear,
    support: Math.min(...lows.slice(-50)), resistance: Math.max(...highs.slice(-50)),
    score, signal
  };
}

async function analysis(symbol, tf) {
  const key = symbol + ':' + tf;
  const cached = cache.get(key);
  if (cached && Date.now() - cached.time < 45000) return cached.data;
  const data = analyse(await klines(symbol, tf));
  cache.set(key, { time: Date.now(), data });
  return data;
}

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/status', (req, res) => res.json({
  ok: true, liveBinance: tickers.size > 0, symbols: tickers.size,
  updatedAt: Date.now(), message: 'Public Tabish Crypto Scalper scanner — no login or payment required.'
}));

app.get('/api/scanner', async (req, res) => {
  const tf = TF[req.query.tf] ? req.query.tf : '1h';
  const q = String(req.query.q || '').trim().toUpperCase();
  const limit = Math.min(100, Math.max(10, Number(req.query.limit) || 50));
  try { await refreshTickersFromRest(); } catch (e) { if (!tickers.size) return res.status(502).json({ error: 'Unable to reach Binance: ' + e.message }); }
  const source = [...tickers.values()]
    .filter(x => x.symbol.endsWith('USDT') && (!q || x.symbol.includes(q)))
    .sort((a, b) => b.vol - a.vol)
    .slice(0, limit);
  const out = [];
  const concurrency = 8;
  for (let i = 0; i < source.length; i += concurrency) {
    const batch = source.slice(i, i + concurrency);
    const results = await Promise.allSettled(batch.map(async ticker => ({ ...ticker, ...(await analysis(ticker.symbol, tf)) })));
    for (const r of results) if (r.status === 'fulfilled') out.push(r.value);
  }
  res.json({ tf, updatedAt: Date.now(), data: out });
});

app.get('/api/scanner/detail/:symbol', async (req, res) => {
  const symbol = String(req.params.symbol).toUpperCase();
  if (!/^[A-Z0-9]+USDT$/.test(symbol)) return res.status(400).json({ error: 'Invalid symbol' });
  const out = { symbol, ticker: tickers.get(symbol) || null };
  for (const tf of Object.keys(TF)) {
    try { out[tf] = await analysis(symbol, tf); } catch {}
  }
  res.json(out);
});

app.get('/api/health', (req, res) => res.json({ status: 'ok', uptime: process.uptime() }));
app.get(/.*/, (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

app.listen(PORT, () => console.log(`Tabish Crypto Scalper live scanner running on http://localhost:${PORT}`));
