# Tabish Crypto Scalper — Real Binance Public Scanner

This version intentionally has **NO username, password, login, subscription, Stripe, Visa, Mastercard or manual-payment system**.

It is a public live scanner that reads real public Binance market data and calculates:
- 30M, 1H, 4H, D1, D2, D3 and W1
- Live USDT ticker prices and 24H change/volume
- RSI (14)
- Regular bullish/bearish divergence
- Hidden bullish/bearish divergence
- Trend
- Support/resistance
- Technical score and signal

## Run locally
1. Install Node.js 20+.
2. Open this folder in Terminal/PowerShell.
3. Run `npm install`.
4. Run `npm start`.
5. Open `http://localhost:3000`.

No Binance API key is required because the scanner uses Binance's public market-data endpoints and public WebSocket streams.

## Important
The scanner is real-data/technical-analysis software, but its signals are algorithmic and are not guaranteed to predict price movements.

For production hosting, use HTTPS, a process manager, monitoring and a reliable server. If Binance is unavailable from your host, configure `BINANCE_REST_URL`/`BINANCE_WS_URL` to an appropriate market-data proxy.
