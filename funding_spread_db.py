#!/usr/bin/env python3

import asyncio
import aiohttp
import json
import os
import time
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from funding_database import FundingDatabase
from message_templates import (
    format_ticker_message, format_level_header, format_startup_message,
    format_stats_message, get_no_data_message, template_manager
)

# ===================== Налаштування =====================
load_dotenv()

# Telegram Bot 1 - для фандингів >= 1%
TELEGRAM_TOKEN_1 = os.getenv("TELEGRAM_TOKEN_1", "").strip()
TELEGRAM_CHAT_ID_1 = os.getenv("TELEGRAM_CHAT_ID_1", "").strip()

# Telegram Bot 2 - для фандингів >= 2%
TELEGRAM_TOKEN_2 = os.getenv("TELEGRAM_TOKEN_2", "").strip()
TELEGRAM_CHAT_ID_2 = os.getenv("TELEGRAM_CHAT_ID_2", "").strip()

# Пороги для різних рівнів алертів
FUNDING_THRESHOLD_LEVEL_1 = float(os.getenv("FUNDING_THRESHOLD_LEVEL_1", "1.0"))  # >= 1%
FUNDING_THRESHOLD_LEVEL_2 = float(os.getenv("FUNDING_THRESHOLD_LEVEL_2", "2.0"))  # >= 2%

DATA_COLLECTION_INTERVAL = int(os.getenv("DATA_COLLECTION_INTERVAL", "300"))  # 5 хвилин
SYMBOLS_DIR = os.getenv("SYMBOLS_DIR", "symbols_cache")

# Конкурентність та "мʼякий" rate-limit по біржах (одночасні запити)
EXCHANGE_CONCURRENCY = {
    "Binance": int(os.getenv("BINANCE_CONCURRENCY", "8")),
    "Bybit": int(os.getenv("BYBIT_CONCURRENCY", "6")),
    "OKX": int(os.getenv("OKX_CONCURRENCY", "6")),
    "Bitget": int(os.getenv("BITGET_CONCURRENCY", "6")),
    "MEXC": int(os.getenv("MEXC_CONCURRENCY", "6")),
}

# Дрібний "джиттер" між запитами, щоб не бити API рівно одночасно
PER_REQUEST_DELAY = float(os.getenv("PER_REQUEST_DELAY", "0.05"))  # сек

# Обмеження на розмір одного Telegram-повідомлення
TELEGRAM_MAX_CHARS = 3500
TELEGRAM_CHUNK_DELAY = float(os.getenv("TELEGRAM_CHUNK_DELAY", "1.0"))  # пауза між чанками, сек
TELEGRAM_MAX_RETRIES = 3

# Time-based alert scheduling
ALERT_TIMES = [1, 8, 15, 18, 20, 25, 30, 35, 40, 45, 50, 55, 56, 57, 58, 59]  # minutes after hour: 1min, 30min, 50min, 55min

# ========================================================

# Initialize database
db = FundingDatabase()

def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def should_send_alert() -> bool:
    """Check if it's time to send alerts based on schedule"""
    now = datetime.datetime.now()
    current_minute = now.minute
    return current_minute in ALERT_TIMES


def should_collect_data() -> bool:
    """Check if it's time to collect data (only at minute 30 of each hour)"""
    now = datetime.datetime.now()
    current_minute = now.minute
    return current_minute == 30


def format_time_until_payout(next_settle_time: int) -> str:
    """Format time until next funding payout"""
    try:
        # Convert timestamp to datetime
        settle_time = datetime.datetime.fromtimestamp(next_settle_time / 1000)
        now = datetime.datetime.now()
        
        # Calculate time difference
        time_diff = settle_time - now
        total_minutes = int(time_diff.total_seconds() / 60)
        
        if total_minutes < 0:
            return "Payout overdue"
        elif total_minutes < 60:
            return f"Payout in {total_minutes}min"
        else:
            hours = total_minutes // 60
            minutes = total_minutes % 60
            if minutes == 0:
                return f"Payout in {hours}h"
            else:
                return f"Payout in {hours}h {minutes}min"
    except Exception as e:
        log(f"Error formatting payout time: {e}")
        return "Payout time unknown"


def get_funding_cycle_hours(collect_cycle: int) -> str:
    """Convert collect cycle to human readable format"""
    if collect_cycle == 1:
        return "1h"
    elif collect_cycle == 4:
        return "4h"
    elif collect_cycle == 8:
        return "8h"
    else:
        return f"{collect_cycle}h"


# --------------------- Завантаження символів ---------------------
def _candidate_paths(exchange: str) -> List[Path]:
    here = Path(__file__).resolve().parent
    candidates = [
        Path(SYMBOLS_DIR) / f"{exchange.lower()}.json",
        Path(f"{exchange.lower()}.json"),
        here / f"{exchange.lower()}.json",
        Path("/mnt/data") / f"{exchange.lower()}.json"
    ]
    return candidates


def load_symbols(exchange: str) -> List[str]:
    for p in _candidate_paths(exchange):
        if p.exists():
            try:
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return [str(x) for x in data]
                log(f"⚠️ {exchange}: файл {p} має неочікуваний формат.")
            except Exception as e:
                log(f"❌ {exchange}: помилка читання {p}: {e}")
    log(f"⛔ Не знайдено жодного файлу символів для {exchange}.")
    return []


# --------------------- Telegram ---------------------
async def send_telegram(text: str, bot_token: str, chat_id: str, bot_name: str = "Telegram") -> None:
    """Send message to Telegram using specified bot with topic support"""
    if not bot_token or not chat_id:
        log(f"⚠️ {bot_name}: TOKEN або CHAT_ID не задано.")
        return

    # Parse chat_id and topic_id if format is "chat_id_topic_id"
    base_chat_id = chat_id
    topic_id = None
    
    if "_" in chat_id:
        try:
            parts = chat_id.split("_")
            if len(parts) == 2:
                base_chat_id = parts[0]
                topic_id = int(parts[1])
                log(f"📝 {bot_name}: Використовую тему {topic_id} для чату {base_chat_id}")
        except ValueError:
            log(f"⚠️ {bot_name}: Невірний формат CHAT_ID: {chat_id}")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": base_chat_id, "text": text}
    
    # Add topic_id if specified
    if topic_id is not None:
        payload["message_thread_id"] = topic_id

    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        topic_info = f" (тема {topic_id})" if topic_id else ""
                        log(f"📨 {bot_name}: повідомлення надіслано{topic_info}.")
                        return
                    elif resp.status in (429, 400):
                        retry_after = resp.headers.get("Retry-After")
                        wait_s = float(retry_after) if retry_after else min(3.0 * attempt, 10.0)
                        text_resp = await resp.text()
                        log(f"⚠️ {bot_name} {resp.status}: {text_resp}. Очікування {wait_s:.1f}s...")
                        await asyncio.sleep(wait_s)
                    else:
                        text_resp = await resp.text()
                        log(f"❌ {bot_name} HTTP {resp.status}: {text_resp}")
                        return
        except Exception as e:
            log(f"❌ {bot_name} помилка {attempt}: {e}")
            await asyncio.sleep(1.0 * attempt)
    log(f"⛔ {bot_name}: вичерпані спроби відправки.")


async def send_telegram_level_1(text: str) -> None:
    """Send message to Level 1 bot (>= 1%)"""
    await send_telegram(text, TELEGRAM_TOKEN_1, TELEGRAM_CHAT_ID_1, "Telegram Level 1")


async def send_telegram_level_2(text: str) -> None:
    """Send message to Level 2 bot (>= 2%)"""
    await send_telegram(text, TELEGRAM_TOKEN_2, TELEGRAM_CHAT_ID_2, "Telegram Level 2")


def create_alerts_by_level() -> Tuple[List[str], List[str]]:
    """Create alerts from database data, separated by levels"""
    # Get all funding rates above level 1 threshold
    funding_rates = db.get_latest_funding_rates(FUNDING_THRESHOLD_LEVEL_1)
    
    log(f"🔍 Знайдено {len(funding_rates)} тікерів вище порогу {FUNDING_THRESHOLD_LEVEL_1}%")
    
    if not funding_rates:
        return [], []
    
    level_1_messages = []  # >= 1%
    level_2_messages = []  # >= 2%
    
    for item in funding_rates:
        ticker = item['ticker']
        rates = item['rates']
        
        # Calculate max rate for this ticker
        rate_values = [rates.get('binance'), rates.get('bybit'), rates.get('okx'), rates.get('mexc'), rates.get('bitget')]
        max_rate = max([abs(r) for r in rate_values if r is not None] or [0])
        log(f"📊 {ticker}: максимальний фандинг = {max_rate:.4f}%")
        
        # Create message using template
        message = format_ticker_message(ticker, rates, max_rate)
        
        # Determine which level this ticker belongs to
        if max_rate >= FUNDING_THRESHOLD_LEVEL_2:
            level_2_messages.append(message)
            level_1_messages.append(message)  # Level 2 tickers also go to level 1
        elif max_rate >= FUNDING_THRESHOLD_LEVEL_1:
            level_1_messages.append(message)
    
    log(f"📊 Рівень 1 (>= {FUNDING_THRESHOLD_LEVEL_1}%): {len(level_1_messages)} тікерів")
    log(f"📊 Рівень 2 (>= {FUNDING_THRESHOLD_LEVEL_2}%): {len(level_2_messages)} тікерів")
    
    return level_1_messages, level_2_messages


async def flush_alerts_from_database() -> None:
    """Send alerts from database data to different bots based on levels"""
    level_1_messages, level_2_messages = create_alerts_by_level()
    
    if not level_1_messages and not level_2_messages:
        log(get_no_data_message())
        return
    
    # Send Level 1 alerts (>= 1%)
    if level_1_messages:
        log(f"📤 Відправляю {len(level_1_messages)} повідомлень рівня 1 (>= {FUNDING_THRESHOLD_LEVEL_1}%)")
        
        # Add level header
        level_1_header = format_level_header(1, FUNDING_THRESHOLD_LEVEL_1)
        await send_telegram_level_1(level_1_header)
        await asyncio.sleep(TELEGRAM_CHUNK_DELAY)
        
        for message in level_1_messages:
            await send_telegram_level_1(message)
            await asyncio.sleep(TELEGRAM_CHUNK_DELAY)
    
    # Send Level 2 alerts (>= 2%)
    if level_2_messages:
        log(f"📤 Відправляю {len(level_2_messages)} повідомлень рівня 2 (>= {FUNDING_THRESHOLD_LEVEL_2}%)")
        
        # Add level header
        level_2_header = format_level_header(2, FUNDING_THRESHOLD_LEVEL_2)
        await send_telegram_level_2(level_2_header)
        await asyncio.sleep(TELEGRAM_CHUNK_DELAY)
        
        for message in level_2_messages:
            await send_telegram_level_2(message)
            await asyncio.sleep(TELEGRAM_CHUNK_DELAY)


# --------------------- API ---------------------
async def _fetch_json(session: aiohttp.ClientSession, url: str, params: Dict[str, Any]) -> Optional[Any]:
    try:
        async with session.get(url, params=params) as resp:
            return await resp.json(content_type=None)
    except Exception as e:
        log(f"❌ HTTP помилка {url} {params}: {e}")
        return None


def _extract_funding_info(exchange: str, data: Any) -> Optional[Dict[str, Any]]:
    """Extract funding rate and payout information from API response"""
    try:
        if exchange == "Binance":
            if isinstance(data, list) and data:
                item = data[0]
                # Calculate next funding time (8 hours from current funding time)
                current_funding_time = int(item["fundingTime"])
                next_funding_time = current_funding_time + (8 * 60 * 60 * 1000)  # 8 hours in milliseconds
                return {
                    "rate": float(item["fundingRate"]) * 100.0,
                    "nextSettleTime": next_funding_time,
                    "collectCycle": 8  # Binance is 8h
                }
        elif exchange == "Bybit":
            lst = data.get("result", {}).get("list", []) if isinstance(data, dict) else []
            if lst:
                item = lst[0]
                # Calculate next funding time (8 hours from current funding time)
                current_funding_time = int(item["fundingRateTimestamp"])
                next_funding_time = current_funding_time + (8 * 60 * 60 * 1000)  # 8 hours in milliseconds
                return {
                    "rate": float(item["fundingRate"]) * 100.0,
                    "nextSettleTime": next_funding_time,
                    "collectCycle": 8  # Bybit is 8h
                }
        elif exchange == "OKX":
            if isinstance(data, dict) and data.get("data"):
                item = data["data"][0]
                return {
                    "rate": float(item["fundingRate"]) * 100.0,
                    "nextSettleTime": int(item["nextFundingTime"]),
                    "collectCycle": 8  # OKX is 8h
                }
        elif exchange == "Bitget":
            if isinstance(data, dict) and "data" in data:
                d = data["data"]
                if isinstance(d, dict) and "fundingRate" in d:
                    # Bitget doesn't provide timing info, use current time + 8h as estimate
                    current_time = int(time.time() * 1000)
                    next_funding_time = current_time + (8 * 60 * 60 * 1000)  # 8 hours in milliseconds
                    return {
                        "rate": float(d["fundingRate"]) * 100.0,
                        "nextSettleTime": next_funding_time,
                        "collectCycle": 8  # Bitget is typically 8h
                    }
                if isinstance(d, list) and d and "fundingRate" in d[0]:
                    item = d[0]
                    # Bitget doesn't provide timing info, use current time + 8h as estimate
                    current_time = int(time.time() * 1000)
                    next_funding_time = current_time + (8 * 60 * 60 * 1000)  # 8 hours in milliseconds
                    return {
                        "rate": float(item["fundingRate"]) * 100.0,
                        "nextSettleTime": next_funding_time,
                        "collectCycle": 8  # Bitget is typically 8h
                    }
        elif exchange == "MEXC":
            if isinstance(data, dict) and data.get("success") and "data" in data:
                d = data["data"]
                if isinstance(d, dict) and "fundingRate" in d:
                    return {
                        "rate": float(d["fundingRate"]) * 100.0,
                        "nextSettleTime": int(d["nextSettleTime"]),
                        "collectCycle": int(d["collectCycle"])
                    }
    except Exception as e:
        log(f"❌ {exchange}: помилка парсингу: {e}")
    return None


def _params_builder(exchange: str, symbol: str) -> Dict[str, Any]:
    if exchange == "Binance":
        return {"symbol": symbol, "limit": 1}
    elif exchange == "Bybit":
        return {"category": "linear", "symbol": symbol, "limit": 1}
    elif exchange == "OKX":
        return {"instId": symbol}
    elif exchange == "Bitget":
        return {"symbol": symbol}
    elif exchange == "MEXC":
        return {}  # MEXC uses path parameters, not query parameters
    return {}


def _url_for(exchange: str) -> str:
    if exchange == "Binance":
        return "https://fapi.binance.com/fapi/v1/fundingRate"
    elif exchange == "Bybit":
        return "https://api.bybit.com/v5/market/funding/history"
    elif exchange == "OKX":
        return "https://www.okx.com/api/v5/public/funding-rate"
    elif exchange == "Bitget":
        return "https://api.bitget.com/api/mix/v1/market/funding-rate"
    elif exchange == "MEXC":
        return "https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}"
    return ""


async def _check_exchange(exchange: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Collect funding data for an exchange"""
    funding_data = {}
    
    if not symbols:
        log(f"⛔ Немає символів для {exchange}")
        return funding_data

    concurrency = EXCHANGE_CONCURRENCY.get(exchange, 5)
    sem = asyncio.Semaphore(concurrency)
    url = _url_for(exchange)
    if not url:
        return funding_data

    async with aiohttp.ClientSession() as session:
        async def worker(sym: str):
            async with sem:
                params = _params_builder(exchange, sym)
                # Handle MEXC URL format with path parameters
                if exchange == "MEXC":
                    sym_url = url.format(symbol=sym)
                else:
                    sym_url = url
                data = await _fetch_json(session, sym_url, params)
                funding_info = _extract_funding_info(exchange, data) if data is not None else None
                if funding_info is not None:
                    funding_data[sym] = funding_info
                    log(f"{exchange} {sym}: {funding_info['rate']:.4f}%")
                await asyncio.sleep(PER_REQUEST_DELAY)

        await asyncio.gather(*[worker(s) for s in symbols], return_exceptions=True)
    
    return funding_data


# --------------------- Головний цикл ---------------------
async def collect_funding_data() -> None:
    """Collect funding data from all exchanges and save to database"""
    log("🔄 Збір даних фандингу...")
    
    ex_symbols = {
        "Binance": load_symbols("binance"),
        "Bybit": load_symbols("bybit"),
        "OKX": load_symbols("okx"),
        "Bitget": load_symbols("bitget"),
        "MEXC": load_symbols("mexc"),
    }
    
    # Save symbols to database
    db.save_symbols(ex_symbols)
    
    # Collect funding data from all exchanges
    all_funding_data = {}
    for exchange, symbols in ex_symbols.items():
        log(f"📊 Збір даних з {exchange} ({len(symbols)} символів)...")
        funding_data = await _check_exchange(exchange, symbols)
        all_funding_data[exchange] = funding_data
    
    # Save to database
    db.save_funding_rates(all_funding_data)
    
    # Show database stats
    stats = db.get_database_stats()
    log(f"💾 База даних: {stats['total_records']} записів, {stats['unique_tickers']} тікерів, {stats['database_size_mb']:.2f}MB")
    
    # Count how many tickers were updated
    total_tickers = sum(len(symbols) for symbols in ex_symbols.values())
    log(f"🔄 Оновлено {stats['unique_tickers']} унікальних тікерів з {total_tickers} символів")


async def main():
    # Send startup messages to both bots
    startup_msg = format_startup_message(FUNDING_THRESHOLD_LEVEL_1, FUNDING_THRESHOLD_LEVEL_2, "30 minutes of each hour")
    
    await send_telegram_level_1(startup_msg)
    await send_telegram_level_2(startup_msg)
    
    last_alert_minute = -1  # Track last alert minute to avoid duplicates
    last_collection_minute = -1  # Track last collection minute to avoid duplicates
    
    while True:
        now = datetime.datetime.now()
        current_minute = now.minute
        
        # Collect data only at minute 30 of each hour
        if should_collect_data() and current_minute != last_collection_minute:
            log(f"📊 Час для збору даних: {current_minute}хв після години")
            last_collection_minute = current_minute
            
            # Collect funding data from all exchanges
            await collect_funding_data()
        
        # Check if it's time to send alerts
        if current_minute in ALERT_TIMES and current_minute != last_alert_minute:
            log(f"⏰ Час для алертів: {current_minute}хв після години")
            last_alert_minute = current_minute
            
            # Send alerts from database
            await flush_alerts_from_database()
        
        # Wait until next check (check every minute)
        await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Зупинено користувачем")
