import asyncio
import aiohttp
import json
import os
import time

# ==== Налаштування ====
TELEGRAM_TOKEN = "8177239083:AAHgb9Ehhrm1dFU1ynCkfQs82pQgDLedjOU"  # встав свій токен
TELEGRAM_CHAT_ID = "-1002699499900" 
FUNDING_THRESHOLD = 0.3  # %
CHECK_INTERVAL = 120  # кожні 2 хвилини
SYMBOLS_DIR = "symbols_cache"

funding_data = {}
alert_buffer = []

def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ==== Telegram ====
async def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    async with aiohttp.ClientSession() as session:
        resp = await session.post(url, json=payload)
        if resp.status == 200:
            log("✅ Telegram повідомлення надіслано.")
        else:
            log(f"❌ Telegram помилка: HTTP {resp.status}")

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

# ==== Функції перевірки для кожної біржі ====
async def check_binance():
    symbols = load_symbols("binance")
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    await check_exchange("Binance", url, symbols, params_key="symbol")

async def check_bybit():
    symbols = load_symbols("bybit")
    url = "https://api.bybit.com/v5/market/funding/history"
    await check_exchange("Bybit", url, symbols, params_key="symbol", extra_params={"category": "linear"})

async def check_okx():
    symbols = load_symbols("okx")
    url = "https://www.okx.com/api/v5/public/funding-rate"
    await check_exchange("OKX", url, symbols, params_key="instId")

async def check_bitget():
    symbols = load_symbols("bitget")
    url = "https://api.bitget.com/api/mix/v1/market/funding-rate"
    await check_exchange("Bitget", url, symbols, params_key="symbol")

# ==== Загальна функція перевірки фандінгу ====
async def check_exchange(name, url, symbols, params_key="symbol", extra_params=None):
    if not symbols:
        log(f"⛔ Немає символів для {name}")
        return

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            try:
                params = {params_key: symbol, "limit": 1}
                if extra_params:
                    params.update(extra_params)

                async with session.get(url, params=params) as resp:
                    data = await resp.json()
                    rate = extract_rate(name, data)
                    if rate is not None:
                        funding_data.setdefault(name, {})[symbol] = rate
                        if abs(rate) >= FUNDING_THRESHOLD:
                            line = f"{symbol} ({name}): `{rate:.4f}%`"
                            alert_buffer.append(line)
                            log(f"➡️ {line}")
            except Exception as e:
                log(f"❌ {name} funding error for {symbol}: {e}")
            await asyncio.sleep(0.05)

def extract_rate(exchange, data):
    try:
        if exchange == "Binance":
            return float(data[0]["fundingRate"]) * 100 if isinstance(data, list) and data else None
        elif exchange == "Bybit":
            return float(data["result"]["list"][0]["fundingRate"]) * 100
        elif exchange == "OKX":
            return float(data["data"][0]["fundingRate"]) * 100
        elif exchange == "Bitget":
            return float(data["data"]["fundingRate"]) * 100
    except Exception as e:
        log(f"❌ Помилка обробки відповіді {exchange}: {e}")
    return None

# ==== Основний цикл ====
async def periodic_checker():
    await send_telegram_message("🚀 Бот запущено (групові повідомлення).")
    while True:
        global alert_buffer
        alert_buffer.clear()

        await asyncio.gather(
            check_binance(),
            check_bybit(),
            check_okx(),
            #check_bitget()
        )

        if alert_buffer:
            message = "⚠️ *Funding Alerts*: +".join(alert_buffer)
            await send_telegram_message(message)
        else:
            log("✅ Немає фандінгів, які перевищують поріг.")

        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(periodic_checker())
