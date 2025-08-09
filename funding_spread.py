import asyncio
import aiohttp
import json
import time
import os
from dotenv import load_dotenv

# ==== Налаштування ====
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FUNDING_THRESHOLD = 0.1  # %
CHECK_INTERVAL = 600      # сек для HTTP опитування
SYMBOLS_DIR = "symbols_cache"

funding_data = {}
symbols_per_exchange = {}

def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ==== Відправка в Telegram ====
async def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    async with aiohttp.ClientSession() as session:
        resp = await session.post(url, json=payload)
        if resp.status == 200:
            log("Повідомлення відправлено успішно.")
        else:
            text_resp = await resp.text()
            log(f"Помилка відправки повідомлення: HTTP {resp.status} — {text_resp}")
            

# ==== Перевірка фандингу та арбітражу ====
alert_lock = asyncio.Lock()
async def process_alerts():
    async with alert_lock:
        log("process_alerts викликана")
        log(f"funding_data keys: {list(funding_data.keys())}")
        for ex, rates in funding_data.items():
            log(f"Перевіряємо біржу: {ex} з {len(rates)} символами")
            for symbol, rate in rates.items():
                log(f"Перевіряємо: {ex} {symbol} funding = {rate:.6f}% (поріг {FUNDING_THRESHOLD}%)")
                if abs(rate) >= FUNDING_THRESHOLD:
                    log(f"Умова пройдена: відправляємо повідомлення для {ex} {symbol} funding = {rate:.6f}%")
                    await send_telegram_message(f"⚡ *{ex}* {symbol} funding: `{rate:.3f}%`")
                else:
                    log(f"Умова не пройдена для {ex} {symbol}")


        # Арбітраж
        all_symbols = set()
        for rates in funding_data.values():
            all_symbols.update(rates.keys())

        for symbol in all_symbols:
            rates_for_symbol = {ex: funding_data[ex][symbol]
                                for ex in funding_data if symbol in funding_data[ex]}
            if len(rates_for_symbol) > 1:
                max_ex = max(rates_for_symbol, key=rates_for_symbol.get)
                min_ex = min(rates_for_symbol, key=rates_for_symbol.get)
                diff = rates_for_symbol[max_ex] - rates_for_symbol[min_ex]
                if abs(diff) >= FUNDING_THRESHOLD:
                    await send_telegram_message(
                        f"💰 *Arbitrage opportunity* {symbol}:\n"
                        f"{max_ex}: `{rates_for_symbol[max_ex]:.3f}%`\n"
                        f"{min_ex}: `{rates_for_symbol[min_ex]:.3f}%`\n"
                        f"Diff: `{diff:.3f}%`"
                    )
                    
# ==== Завантаження кешованих символів ====
def load_symbols(exchange_name):
    path = os.path.join(SYMBOLS_DIR, f"{exchange_name.lower()}.json")
    if not os.path.exists(path):
        log(f"⚠️ Файл кешу не знайдено для {exchange_name}")
        return []
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        log(f"❌ Помилка зчитування {exchange_name}: {e}")
        return []

# ==== Обгортка для надійного запуску корутин з логуванням помилок ====
async def safe_runner(name, coro):
    try:
        log(f"Запуск {name}")
        await coro
    except Exception as e:
        log(f"Помилка у {name}: {e}")

# ==== Binance WebSocket ====
async def binance_funding():
    symbols = load_symbols("binance")
    if not symbols:
        log("Немає торговий пар Binance")
        return

    async with aiohttp.ClientSession() as session:
        try:
            for sym in symbols:
                async with session.get("https://fapi.binance.com/fapi/v1/fundingRate", params={"symbol": sym, "limit": 1}) as resp:
                    data = await resp.json()
                    log(data)
                    if isinstance(data, list) and len(data) > 0:
                        rate = float(data[0]["fundingRate"]) * 100
                        log(f"Отримано funding {rate:.3f}% від BINANCE для {sym}")
                        funding_data.setdefault("BINANCE", {})[sym] = rate
                await process_alerts()
        except Exception as e:
            log(f"BINANCE funding error: {e}")
        await asyncio.sleep(CHECK_INTERVAL)


# ==== OKX HTTP ====
async def okx_http():
    symbols = load_symbols("okx")
    if not symbols:
        log("Немає торговий пар OKX")
        return
    
    async with aiohttp.ClientSession() as session:
        try:
            for sym in symbols:
                async with session.get("https://www.okx.com/api/v5/public/funding-rate", params={"instId": sym}) as resp:
                    data = await resp.json()
                    rate = float(data["data"][0]["fundingRate"]) * 100
                    log(f"Отримано funding {rate:.3f}% від OKX для {sym}")
                    funding_data.setdefault("OKX", {})[sym] = rate
                await process_alerts()
        except Exception as e:
            log(f"OKX funding error: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

# ==== Bitget HTTP ====
async def bitget_http():
    symbols = load_symbols("bitget")
    if not symbols:
        log("Немає торговий пар Bitget")
        return
    
    async with aiohttp.ClientSession() as session:
        try:
            for sym in symbols:
                async with session.get("https://api.bitget.com/api/mix/v1/market/current-fundRate", params={"symbol": sym}) as resp:
                    data = await resp.json()
                    rate = float(data["data"]["fundingRate"]) * 100
                    log(f"Отримано funding {rate:.3f}% від Bitget для {sym}")
                    funding_data.setdefault("Bitget", {})[sym] = rate
                await process_alerts()
        except Exception as e:
            log(f"Bitget funding error: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

# ==== MEXC HTTP ====
async def mexc_http():
    symbols = load_symbols("mexc")
    if not symbols:
        log("Немає торговий пар MEXC")
        return
    
    async with aiohttp.ClientSession() as session:
        try:
            for sym in symbols:
                async with session.get(f"https://contract.mexc.com/api/v1/contract/funding_rate/{sym}") as resp:
                    data = await resp.json()
                    if "data" in data:
                        rate = float(data["data"]["fundingRate"]) * 100
                        log(f"Отримано funding {rate:.3f}% від MEXC для {sym}")
                        funding_data.setdefault("MEXC", {})[sym] = rate
                await process_alerts()
        except Exception as e:
            log(f"MEXC funding error: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

# ==== Запуск ====
async def main():
    await send_telegram_message("🤖 *Funding bot запущено!*")
    while True:
        await asyncio.gather(
            # safe_runner("binance_funding", binance_funding()),
            # safe_runner("okx_http", okx_http()),
            # safe_runner("bitget_http", bitget_http()),
            safe_runner("mexc_http", mexc_http()),
            asyncio.sleep(CHECK_INTERVAL),
            return_exceptions=True
        )

if __name__ == "__main__":
    asyncio.run(main())
