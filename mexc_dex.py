import asyncio
import aiohttp
import json
from telegram import Bot
import time
import logging

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)

# --- CONFIG ---
SPREAD_THRESHOLD = 3.0  # %
DEX_PAIRS_CACHE = {}  # кеш DexScreener відповідей

bot = Bot(token=TELEGRAM_TOKEN)

async def get_futures_symbols():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://contract.mexc.com/api/v1/contract/detail") as resp:
            data = await resp.json()
            usdt_pairs = [x for x in data["data"] if x["quoteCoin"] == "USDT"]
            return usdt_pairs

async def get_token_address(symbol: str):
    """Отримуємо адресу контракту токена з MEXC за його символом."""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.mexc.com/api/v2/contract/coin/{symbol}") as resp:
            data = await resp.json()
            if data.get("code") == 200:
                return data["data"]["coin"]["address"]
            else:
                logging.warning(f"[MEXC] Адреса контракту для {symbol} не знайдена.")
                return None

async def get_dex_price(contract_address: str):
    """Отримуємо ціну токена за адресою контракту на DexScreener."""
    url = f"https://api.dexscreener.com/latest/dex/pairs/{contract_address}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            if not data.get("pairs"):
                return None
            best = max(data["pairs"], key=lambda x: float(x.get("liquidity", {}).get("usd", 0)))
            price = float(best["priceUsd"])
            logging.info(f"[DEX] {contract_address}: ${price:.6f}")
            return price

async def monitor_prices():
    symbols = await get_futures_symbols()
    logging.info(f"[MEXC] Отримано {len(symbols)} ф'ючерсних пар.")

    url = "wss://contract.mexc.com/ws"
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url) as ws:
            for symbol in symbols:
                await ws.send_json({
                    "method": "sub.deal",
                    "params": [symbol["symbol"]],
                    "id": symbol["id"]
                })
                await asyncio.sleep(0.02)

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data.get("channel") and data.get("data"):
                        symbol = data["channel"].split("_")[1]
                        price_cex = float(data["data"][0]["p"])
                        base_coin = symbol.split("_")[0]

                        # Отримуємо адресу контракту для пошуку на Dex
                        contract_address = await get_token_address(base_coin)
                        if contract_address:
                            price_dex = await get_dex_price(contract_address)
                            if price_dex:
                                spread = ((price_cex - price_dex) / price_dex) * 100
                                logging.info(f"[SPREAD] {symbol}: MEXC={price_cex:.6f} | DEX={price_dex:.6f} | Δ={spread:.2f}%")

                                if spread > SPREAD_THRESHOLD:
                                    text = (
                                        f"📊 Спред знайдено!\n"
                                        f"Токен: {base_coin}\n"
                                        f"DEX: {price_dex:.4f} $\n"
                                        f"MEXC Futures: {price_cex:.4f} $\n"
                                        f"📈 Спред: +{spread:.2f}%"
                                    )
                                    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logging.error("WebSocket error:", msg)
                    break

if __name__ == "__main__":
    asyncio.run(monitor_prices())