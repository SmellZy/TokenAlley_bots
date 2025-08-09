import asyncio
import aiohttp
import json
import os
import time

# ==== Налаштування ====
CHECK_INTERVAL = 60 * 60  # перевіряти фандінги щогодини
SYMBOLS_DIR = "symbols_cache"
os.makedirs(SYMBOLS_DIR, exist_ok=True)

symbols_per_exchange = {}
funding_data = {}
last_updated = {}

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ==== Кешування символів ====
def load_symbols_from_file(exchange_name):
    file_path = os.path.join(SYMBOLS_DIR, f"{exchange_name.lower()}.json")
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception as e:
        log(f"❌ Помилка зчитування кешу для {exchange_name}: {e}")
        return []

def save_symbols_to_file(exchange_name, symbols):
    file_path = os.path.join(SYMBOLS_DIR, f"{exchange_name.lower()}.json")
    try:
        with open(file_path, "w") as f:
            json.dump(symbols, f, indent=2)
        log(f"💾 Символи {exchange_name} збережено: {len(symbols)}")
    except Exception as e:
        log(f"❌ Помилка запису кешу для {exchange_name}: {e}")

# ==== Binance ====
async def binance_get_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    cached = load_symbols_from_file("binance")
    new_symbols = []

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"Binance get symbols: помилка HTTP {resp.status}")
                symbols_per_exchange["BINANCE"] = cached
                return cached

            data = await resp.json()
            all_symbols = [item["symbol"] for item in data.get("symbols", []) if item["contractType"] == "PERPETUAL"]
            new_symbols = [s for s in all_symbols if s not in cached]
            if new_symbols:
                log(f"🆕 Знайдено {len(new_symbols)} нових символів на Binance")
                save_symbols_to_file("binance", all_symbols)
            symbols_per_exchange["BINANCE"] = all_symbols
            log(f"Binance symbols отримано: {len(all_symbols)}")
            return all_symbols

# ==== OKX ====
async def okx_get_symbols():
    url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
    cached = load_symbols_from_file("okx")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"OKX get symbols: помилка HTTP {resp.status}")
                symbols_per_exchange["OKX"] = cached
                return cached

            data = await resp.json()
            all_symbols = [item["instId"] for item in data.get("data", [])]
            new_symbols = [s for s in all_symbols if s not in cached]
            if new_symbols:
                log(f"🆕 Знайдено {len(new_symbols)} нових символів на OKX")
                save_symbols_to_file("okx", all_symbols)
            symbols_per_exchange["OKX"] = all_symbols
            log(f"OKX symbols отримано: {len(all_symbols)}")
            return all_symbols

# ==== Bybit ====
async def bybit_get_symbols():
    url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
    cached = load_symbols_from_file("bybit")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"Bybit get symbols: помилка HTTP {resp.status}")
                symbols_per_exchange["BYBIT"] = cached
                return cached

            data = await resp.json()
            all_symbols = [item["symbol"] for item in data.get("result", {}).get("list", [])]
            new_symbols = [s for s in all_symbols if s not in cached]
            if new_symbols:
                log(f"🆕 Знайдено {len(new_symbols)} нових символів на Bybit")
                save_symbols_to_file("bybit", all_symbols)
            symbols_per_exchange["BYBIT"] = all_symbols
            log(f"Bybit symbols отримано: {len(all_symbols)}")
            return all_symbols

# ==== Bitget ====
async def bitget_get_symbols():
    url = "https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl"
    cached = load_symbols_from_file("bitget")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"Bitget get symbols: помилка HTTP {resp.status}")
                symbols_per_exchange["BITGET"] = cached
                return cached

            data = await resp.json()
            all_symbols = [item["symbol"] for item in data.get("data", [])]
            new_symbols = [s for s in all_symbols if s not in cached]
            if new_symbols:
                log(f"🆕 Знайдено {len(new_symbols)} нових символів на Bitget")
                save_symbols_to_file("bitget", all_symbols)
            symbols_per_exchange["BITGET"] = all_symbols
            log(f"Bitget symbols отримано: {len(all_symbols)}")
            return all_symbols
        
# ==== MEXC ====
async def mexc_get_symbols():
    url = "https://contract.mexc.com/api/v1/contract/detail"
    cached = load_symbols_from_file("mexc")

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"MEXC get symbols: помилка HTTP {resp.status}")
                symbols_per_exchange["MEXC"] = cached
                return cached

            data = await resp.json()
            all_symbols = [item["symbol"] for item in data.get("data", [])]
            new_symbols = [s for s in all_symbols if s not in cached]
            if new_symbols:
                log(f"🆕 Знайдено {len(new_symbols)} нових символів на MEXC")
                save_symbols_to_file("mexc", all_symbols)
            symbols_per_exchange["MEXC"] = all_symbols
            log(f"MEXC symbols отримано: {len(all_symbols)}")
            return all_symbols

# ==== Головна функція ====
async def main():
    await asyncio.gather(
        binance_get_symbols(),
        okx_get_symbols(),
        bybit_get_symbols(),
        bitget_get_symbols(),
        mexc_get_symbols()
    )

if __name__ == "__main__":
    asyncio.run(main())
