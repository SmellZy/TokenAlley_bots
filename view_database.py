#!/usr/bin/env python3

import sqlite3
import json
from datetime import datetime
from funding_database import FundingDatabase

def view_database():
    """View and analyze database contents"""
    db = FundingDatabase()
    
    print("📊 Funding Database Analysis")
    print("=" * 60)
    
    # Get database stats
    stats = db.get_database_stats()
    print(f"📈 Database Statistics:")
    print(f"  Total records: {stats['total_records']}")
    print(f"  Unique tickers: {stats['unique_tickers']}")
    print(f"  Unique timestamps: {stats['unique_timestamps']}")
    print(f"  Database size: {stats['database_size_mb']:.2f} MB")
    print(f"  Latest timestamp: {datetime.fromtimestamp(stats['latest_timestamp'])}")
    
    print("\n" + "=" * 60)
    
    # Get latest funding rates
    print("💰 Latest Funding Rates (above 0.1% threshold):")
    latest_rates = db.get_latest_funding_rates(threshold=0.1)
    
    if not latest_rates:
        print("  No funding rates above threshold found.")
    else:
        for item in latest_rates:
            ticker = item['ticker']
            rates = item['rates']
            
            print(f"\n  {ticker}/USDT:")
            exchanges = ['binance', 'bybit', 'okx', 'mexc', 'bitget']
            for exchange in exchanges:
                rate = rates.get(exchange)
                if rate is not None:
                    sign = "+" if rate >= 0 else ""
                    print(f"    {exchange.capitalize()}: {sign}{rate:.4f}%")
                else:
                    print(f"    {exchange.capitalize()}: NULL")
    
    print("\n" + "=" * 60)
    
    # Show sample data from database
    print("🔍 Sample Database Records:")
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.cursor()
        
        # Get latest 5 records
        cursor.execute("""
            SELECT ticker, binance_rate, bybit_rate, okx_rate, mexc_rate, bitget_rate, timestamp
            FROM funding_rates 
            ORDER BY timestamp DESC 
            LIMIT 5
        """)
        
        records = cursor.fetchall()
        if records:
            print("  Latest 5 records:")
            for record in records:
                ticker, binance, bybit, okx, mexc, bitget, timestamp = record
                dt = datetime.fromtimestamp(timestamp)
                print(f"    {dt}: {ticker} - B:{binance}, By:{bybit}, O:{okx}, M:{mexc}, Bi:{bitget}")
        else:
            print("  No records found in database.")
    
    print("\n" + "=" * 60)
    
    # Show unique tickers
    print("📋 All Tickers in Database:")
    tickers = db.get_all_tickers()
    if tickers:
        print(f"  Found {len(tickers)} tickers:")
        for i, ticker in enumerate(tickers[:20], 1):  # Show first 20
            print(f"    {i:2d}. {ticker}")
        if len(tickers) > 20:
            print(f"    ... and {len(tickers) - 20} more")
    else:
        print("  No tickers found in database.")

if __name__ == "__main__":
    view_database()
