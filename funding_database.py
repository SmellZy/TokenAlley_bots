#!/usr/bin/env python3

import sqlite3
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

class FundingDatabase:
    def __init__(self, db_path: str = "funding_rates.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create symbols table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbols (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    original_symbol TEXT NOT NULL,
                    normalized_symbol TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, exchange)
                )
            """)
            
            # Create funding_rates table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS funding_rates (
                    ticker TEXT PRIMARY KEY,
                    binance_rate REAL,
                    bybit_rate REAL,
                    okx_rate REAL,
                    mexc_rate REAL,
                    bitget_rate REAL,
                    binance_next_settle INTEGER,
                    bybit_next_settle INTEGER,
                    okx_next_settle INTEGER,
                    mexc_next_settle INTEGER,
                    bitget_next_settle INTEGER,
                    binance_cycle INTEGER,
                    bybit_cycle INTEGER,
                    okx_cycle INTEGER,
                    mexc_cycle INTEGER,
                    bitget_cycle INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON funding_rates(ticker)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON funding_rates(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbols_ticker ON symbols(ticker)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbols_exchange ON symbols(exchange)")
            
            conn.commit()
    
    def normalize_symbol(self, symbol: str, exchange: str) -> str:
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
    
    def extract_ticker(self, normalized_symbol: str) -> str:
        """Extract ticker from normalized symbol (e.g., GMX/USDT -> GMX)"""
        if "/" in normalized_symbol:
            return normalized_symbol.split("/")[0]
        return normalized_symbol
    
    def save_symbols(self, symbols_data: Dict[str, List[str]]):
        """Save symbols from all exchanges to database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for exchange, symbols in symbols_data.items():
                for symbol in symbols:
                    normalized_symbol = self.normalize_symbol(symbol, exchange)
                    ticker = self.extract_ticker(normalized_symbol)
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO symbols (ticker, exchange, original_symbol, normalized_symbol)
                        VALUES (?, ?, ?, ?)
                    """, (ticker, exchange, symbol, normalized_symbol))
            
            conn.commit()
    
    def get_all_tickers(self) -> List[str]:
        """Get all unique tickers from database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT ticker FROM symbols ORDER BY ticker")
            return [row[0] for row in cursor.fetchall()]
    
    def save_funding_rates(self, funding_data: Dict[str, Dict[str, Dict[str, Any]]]):
        """Save funding rates for all tickers across all exchanges"""
        # Get all unique tickers
        all_tickers = set()
        for exchange, symbols_data in funding_data.items():
            for symbol, funding_info in symbols_data.items():
                normalized_symbol = self.normalize_symbol(symbol, exchange)
                ticker = self.extract_ticker(normalized_symbol)
                all_tickers.add(ticker)
        
        # Prepare data for each ticker
        current_time = int(time.time())
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for ticker in all_tickers:
                # Initialize with NULL values
                rates = {
                    'binance_rate': None,
                    'bybit_rate': None,
                    'okx_rate': None,
                    'mexc_rate': None,
                    'bitget_rate': None,
                    'binance_next_settle': None,
                    'bybit_next_settle': None,
                    'okx_next_settle': None,
                    'mexc_next_settle': None,
                    'bitget_next_settle': None,
                    'binance_cycle': None,
                    'bybit_cycle': None,
                    'okx_cycle': None,
                    'mexc_cycle': None,
                    'bitget_cycle': None
                }
                
                # Fill in available data
                for exchange, symbols_data in funding_data.items():
                    for symbol, funding_info in symbols_data.items():
                        normalized_symbol = self.normalize_symbol(symbol, exchange)
                        symbol_ticker = self.extract_ticker(normalized_symbol)
                        
                        if symbol_ticker == ticker:
                            exchange_lower = exchange.lower()
                            rates[f'{exchange_lower}_rate'] = funding_info['rate']
                            rates[f'{exchange_lower}_next_settle'] = funding_info['nextSettleTime']
                            rates[f'{exchange_lower}_cycle'] = funding_info['collectCycle']
                
                # Insert or update in database (UPSERT)
                cursor.execute("""
                    INSERT OR REPLACE INTO funding_rates (
                        ticker, timestamp,
                        binance_rate, bybit_rate, okx_rate, mexc_rate, bitget_rate,
                        binance_next_settle, bybit_next_settle, okx_next_settle, mexc_next_settle, bitget_next_settle,
                        binance_cycle, bybit_cycle, okx_cycle, mexc_cycle, bitget_cycle
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ticker, current_time,
                    rates['binance_rate'], rates['bybit_rate'], rates['okx_rate'], rates['mexc_rate'], rates['bitget_rate'],
                    rates['binance_next_settle'], rates['bybit_next_settle'], rates['okx_next_settle'], rates['mexc_next_settle'], rates['bitget_next_settle'],
                    rates['binance_cycle'], rates['bybit_cycle'], rates['okx_cycle'], rates['mexc_cycle'], rates['bitget_cycle']
                ))
            
            conn.commit()
    
    def get_latest_funding_rates(self, threshold: float = 0.0) -> List[Dict[str, Any]]:
        """Get latest funding rates for all tickers, filtered by threshold"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get all rates (each ticker has only one record now)
            cursor.execute("""
                SELECT 
                    ticker,
                    binance_rate, bybit_rate, okx_rate, mexc_rate, bitget_rate,
                    binance_next_settle, bybit_next_settle, okx_next_settle, mexc_next_settle, bitget_next_settle,
                    binance_cycle, bybit_cycle, okx_cycle, mexc_cycle, bitget_cycle
                FROM funding_rates 
                ORDER BY ticker
            """)
            
            results = []
            for row in cursor.fetchall():
                ticker = row[0]
                rates = {
                    'binance': row[1], 'bybit': row[2], 'okx': row[3], 'mexc': row[4], 'bitget': row[5],
                    'binance_next_settle': row[6], 'bybit_next_settle': row[7], 'okx_next_settle': row[8], 
                    'mexc_next_settle': row[9], 'bitget_next_settle': row[10],
                    'binance_cycle': row[11], 'bybit_cycle': row[12], 'okx_cycle': row[13], 
                    'mexc_cycle': row[14], 'bitget_cycle': row[15]
                }
                
                # Check if any rate meets threshold
                rate_values = [rates.get('binance'), rates.get('bybit'), rates.get('okx'), rates.get('mexc'), rates.get('bitget')]
                max_rate = max([abs(r) for r in rate_values if r is not None] or [0])
                if max_rate >= threshold:
                    results.append({
                        'ticker': ticker,
                        'rates': rates
                    })
            
            return results
    
    def cleanup_old_data(self, days_to_keep: int = 7):
        """Remove old funding rate data"""
        cutoff_time = int(time.time()) - (days_to_keep * 24 * 60 * 60)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM funding_rates WHERE timestamp < ?", (cutoff_time,))
            deleted_count = cursor.rowcount
            conn.commit()
            
            return deleted_count
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Count records (now each ticker has only one record)
            cursor.execute("SELECT COUNT(*) FROM funding_rates")
            total_records = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM funding_rates")
            unique_tickers = cursor.fetchone()[0]
            
            # Get latest timestamp
            cursor.execute("SELECT MAX(timestamp) FROM funding_rates")
            latest_timestamp = cursor.fetchone()[0]
            
            return {
                'total_records': total_records,
                'unique_tickers': unique_tickers,
                'latest_timestamp': latest_timestamp,
                'database_size_mb': Path(self.db_path).stat().st_size / (1024 * 1024) if Path(self.db_path).exists() else 0
            }
