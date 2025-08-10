import asyncio
import aiohttp
import json
import os
import time
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv

# ===================== Налаштування =====================
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

FUNDING_THRESHOLD = float(os.getenv("FUNDING_THRESHOLD", "1.0"))  # у %
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120"))  # сек, раз на 2 хв за замовчуванням
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
ALERT_TIMES = [1, 30, 39, 50, 55]  # minutes after hour: 1min, 30min, 50min, 55min

# ========================================================

funding_data: Dict[str, Dict[str, Dict[str, Any]]] = {}


def log(msg: str):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def should_send_alert() -> bool:
    """Check if it's time to send alerts based on schedule"""
    now = datetime.datetime.now()
    current_minute = now.minute
    return current_minute in ALERT_TIMES


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
async def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log("⚠️ TELEGRAM_TOKEN або TELEGRAM_CHAT_ID не задано.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        log("📨 Telegram: повідомлення надіслано.")
                        return
                    elif resp.status in (429, 400):
                        retry_after = resp.headers.get("Retry-After")
                        wait_s = float(retry_after) if retry_after else min(3.0 * attempt, 10.0)
                        text_resp = await resp.text()
                        log(f"⚠️ Telegram {resp.status}: {text_resp}. Очікування {wait_s:.1f}s...")
                        await asyncio.sleep(wait_s)
                    else:
                        text_resp = await resp.text()
                        log(f"❌ Telegram HTTP {resp.status}: {text_resp}")
                        return
        except Exception as e:
            log(f"❌ Telegram помилка {attempt}: {e}")
            await asyncio.sleep(1.0 * attempt)
    log("⛔ Telegram: вичерпані спроби відправки.")


def chunk_text(lines: List[str], max_chars: int) -> List[str]:
    chunks = []
    buf = []
    cur_len = 0
    for line in lines:
        add_len = len(line) + 1
        if cur_len + add_len > max_chars and buf:
            chunks.append("\n".join(buf))
            buf = [line]
            cur_len = add_len
        else:
            buf.append(line)
            cur_len += add_len
    if buf:
        chunks.append("\n".join(buf))
    return chunks


def normalize_symbol(symbol: str, exchange: str) -> str:
    """Normalize symbol names across exchanges for comparison"""
    # Remove exchange-specific suffixes
    symbol = symbol.replace("_UMCBL", "")  # Bitget
    symbol = symbol.replace("-SWAP", "")   # OKX
    symbol = symbol.replace("_", "")       # MEXC uses underscores
    
    # Handle OKX format (e.g., GMX-USDT -> GMX/USDT)
    if "-" in symbol and ("USDT" in symbol or "USD" in symbol):
        parts = symbol.split("-")
        if len(parts) >= 2:
            base = parts[0]
            quote = parts[1]
            return f"{base}/{quote}"
    
    # Convert to common format (e.g., BTCUSDT -> BTC/USDT)
    if "USDT" in symbol:
        base = symbol.replace("USDT", "")
        return f"{base}/USDT"
    elif "USD" in symbol:
        base = symbol.replace("USD", "")
        return f"{base}/USD"
    else:
        return symbol


def create_symbol_grouped_alerts() -> List[str]:
    """Create alerts grouped by symbol across all exchanges"""
    # Group all funding data by normalized symbol
    symbol_data: Dict[str, List[Dict[str, Any]]] = {}
    
    for exchange, symbols_data in funding_data.items():
        for symbol, funding_info in symbols_data.items():
            if abs(funding_info["rate"]) >= FUNDING_THRESHOLD:
                normalized_symbol = normalize_symbol(symbol, exchange)
                symbol_data.setdefault(normalized_symbol, []).append({
                    "exchange": exchange,
                    "original_symbol": symbol,
                    "rate": funding_info["rate"],
                    "nextSettleTime": funding_info["nextSettleTime"],
                    "collectCycle": funding_info["collectCycle"]
                })
    
    if not symbol_data:
        return []
    
    # Create messages for each symbol
    messages = []
    for symbol, exchanges_data in symbol_data.items():
        # Sort exchanges by absolute rate (highest first)
        exchanges_data.sort(key=lambda x: abs(x["rate"]), reverse=True)
        
        # Create the message in the requested format
        lines = []
        lines.append(f"__________________________")
        lines.append(f"|{symbol:<42}|")
        
        for data in exchanges_data:
            exchange_name = data["exchange"]
            rate = data["rate"]
            sign = "+" if rate >= 0 else ""
            lines.append(f"|-{exchange_name} {symbol} = {sign}{rate:.4f}%|")
        
        lines.append(f"|__________________________|")
        
        message = "\n".join(lines)
        messages.append(message)
    
    return messages


async def flush_alerts_grouped() -> None:
    """Send alerts in the new symbol-grouped format"""
    messages = create_symbol_grouped_alerts()
    
    if not messages:
        log("✅ Немає фандінгів для відправки.")
        return
    
    log(f"📤 Відправляю {len(messages)} повідомлень з алертами")
    
    for message in messages:
        await send_telegram(message)
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


async def _check_exchange(exchange: str, symbols: List[str]) -> None:
    if not symbols:
        log(f"⛔ Немає символів для {exchange}")
        return

    concurrency = EXCHANGE_CONCURRENCY.get(exchange, 5)
    sem = asyncio.Semaphore(concurrency)
    url = _url_for(exchange)
    if not url:
        return

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
                    funding_data.setdefault(exchange, {})[sym] = funding_info
                    # Always log for debugging
                    if abs(funding_info["rate"]) >= FUNDING_THRESHOLD:
                        log(f"{exchange} {sym}: {funding_info['rate']:.4f}% (above threshold)")
                    else:
                        log(f"{exchange} {sym}: {funding_info['rate']:.4f}%")
                await asyncio.sleep(PER_REQUEST_DELAY)

        await asyncio.gather(*[worker(s) for s in symbols], return_exceptions=True)


# --------------------- Головний цикл ---------------------
async def run_once() -> None:
    ex_symbols = {
        "Binance": load_symbols("binance"),
        "Bybit": load_symbols("bybit"),
        "OKX": load_symbols("okx"),
        "Bitget": load_symbols("bitget"),
        "MEXC": load_symbols("mexc"),
    }
    await asyncio.gather(*[_check_exchange(ex, syms) for ex, syms in ex_symbols.items()])


async def main():
    await send_telegram(f"🚀 Старт бота (поріг {FUNDING_THRESHOLD:.2f}%, алерти: {ALERT_TIMES}хв після години)")
    
    last_alert_minute = -1  # Track last alert minute to avoid duplicates
    
    while True:
        now = datetime.datetime.now()
        current_minute = now.minute
        
        # Check if it's time to send alerts
        if current_minute in ALERT_TIMES and current_minute != last_alert_minute:
            log(f"⏰ Час для алертів: {current_minute}хв після години")
            last_alert_minute = current_minute
            
            # Collect new funding data
            t0 = time.time()
            await run_once()
            elapsed = time.time() - t0
            
            # Send alerts based on collected data
            await flush_alerts_grouped()
        else:
            # Just collect data without sending alerts
            await run_once()
        
        # Wait until next check (check every minute)
        await asyncio.sleep(60)  # Check every minute


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("Зупинено користувачем")
