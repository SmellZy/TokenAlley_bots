#!/usr/bin/env python3

import os
from typing import Dict, Any, List
from pathlib import Path

# Шаблони повідомлень
DEFAULT_TEMPLATES = {
    "level_1_header": "🔥 ФАНДИНГИ >= {threshold}% 🔥",
    "level_2_header": "🚨 ВИСОКІ ФАНДИНГИ >= {threshold}% 🚨",
    "ticker_box": """╔══════════════════════════════════╗
 🪙 {ticker}/USDT""",
    "ticker_footer": "╚═════════════════════════════════╝",
    "exchange_line": "📈 {exchange} {ticker}/USDT = {sign}{rate:.4f}% ({cycle_hours}, ⏰ {payout_time})",
    "no_data_message": "✅ Немає фандінгів для відправки.",
    "startup_message": """🚀 Старт бота з базою даних
📊 Рівень 1: >= {level1}%
📊 Рівень 2: >= {level2}%
⏰ Збір даних: {interval}""",
    "stats_message": """📊 Статистика збору:
🔍 Знайдено {total} тікерів вище порогу {threshold}%
📊 Рівень 1 (>= {level1}%): {count1} тікерів
📊 Рівень 2 (>= {level2}%): {count2} тікерів"""
}

class MessageTemplateManager:
    def __init__(self, template_file: str = "message_templates.json"):
        self.template_file = template_file
        self.templates = self.load_templates()
    
    def load_templates(self) -> Dict[str, str]:
        """Load templates from file or use defaults"""
        if os.path.exists(self.template_file):
            try:
                import json
                with open(self.template_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ Помилка завантаження шаблонів: {e}")
                return DEFAULT_TEMPLATES.copy()
        else:
            # Create default template file
            self.save_templates(DEFAULT_TEMPLATES)
            return DEFAULT_TEMPLATES.copy()
    
    def save_templates(self, templates: Dict[str, str]) -> None:
        """Save templates to file"""
        try:
            import json
            with open(self.template_file, 'w', encoding='utf-8') as f:
                json.dump(templates, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"❌ Помилка збереження шаблонів: {e}")
    
    def get_template(self, template_name: str) -> str:
        """Get template by name"""
        return self.templates.get(template_name, "")
    
    def format_template(self, template_name: str, **kwargs) -> str:
        """Format template with given parameters"""
        template = self.get_template(template_name)
        try:
            return template.format(**kwargs)
        except KeyError as e:
            print(f"⚠️ Помилка форматування шаблону {template_name}: {e}")
            return template
    
    def update_template(self, template_name: str, new_content: str) -> bool:
        """Update a specific template"""
        if template_name in self.templates:
            self.templates[template_name] = new_content
            self.save_templates(self.templates)
            return True
        return False
    
    def list_templates(self) -> List[str]:
        """List all available template names"""
        return list(self.templates.keys())
    
    def reset_to_defaults(self) -> None:
        """Reset all templates to default values"""
        self.templates = DEFAULT_TEMPLATES.copy()
        self.save_templates(self.templates)
    
    def create_custom_template(self, template_name: str, content: str) -> bool:
        """Create a new custom template"""
        if template_name not in self.templates:
            self.templates[template_name] = content
            self.save_templates(self.templates)
            return True
        return False

# Глобальний екземпляр менеджера шаблонів
template_manager = MessageTemplateManager()

def format_ticker_message(ticker: str, rates: Dict[str, Any], threshold: float) -> str:
    """Format a ticker message using templates"""
    lines = []
    
    # Header - format ticker box
    ticker_box = template_manager.format_template("ticker_box", ticker=ticker)
    lines.append(ticker_box)
    
    # Exchange rates
    exchanges = ['binance', 'bybit', 'okx', 'mexc', 'bitget']
    for exchange in exchanges:
        rate = rates.get(exchange)
        if rate is not None:
            sign = "+" if rate >= 0 else ""
            next_settle = rates.get(f'{exchange}_next_settle')
            cycle = rates.get(f'{exchange}_cycle', 8)
            
            # Format cycle hours
            if cycle == 1:
                cycle_hours = "1h"
            elif cycle == 4:
                cycle_hours = "4h"
            elif cycle == 8:
                cycle_hours = "8h"
            else:
                cycle_hours = f"{cycle}h"
            
            # Format payout time
            try:
                import datetime
                settle_time = datetime.datetime.fromtimestamp(next_settle / 1000)
                now = datetime.datetime.now()
                time_diff = settle_time - now
                total_minutes = int(time_diff.total_seconds() / 60)
                
                if total_minutes < 0:
                    payout_time = "Payout overdue"
                elif total_minutes < 60:
                    payout_time = f"Payout in {total_minutes}min"
                else:
                    hours = total_minutes // 60
                    minutes = total_minutes % 60
                    if minutes == 0:
                        payout_time = f"Payout in {hours}h"
                    else:
                        payout_time = f"Payout in {hours}h {minutes}min"
            except:
                payout_time = "Unknown"
            
            # Format exchange line using template formatting
            exchange_line = template_manager.format_template("exchange_line", 
                exchange=exchange.capitalize(),
                ticker=ticker,
                sign=sign,
                rate=rate,
                cycle_hours=cycle_hours,
                payout_time=payout_time
            )
            lines.append(exchange_line)
    
    # Footer
    lines.append(template_manager.get_template("ticker_footer"))
    
    return "\n".join(lines)

def format_level_header(level: int, threshold: float) -> str:
    """Format level header"""
    if level == 1:
        return template_manager.format_template("level_1_header", threshold=threshold)
    else:
        return template_manager.format_template("level_2_header", threshold=threshold)

def format_startup_message(level1: float, level2: float, interval: str) -> str:
    """Format startup message"""
    return template_manager.format_template("startup_message", level1=level1, level2=level2, interval=interval)

def format_stats_message(total: int, threshold: float, level1: float, level2: float, count1: int, count2: int) -> str:
    """Format statistics message"""
    return template_manager.format_template("stats_message", 
                                          total=total, threshold=threshold, 
                                          level1=level1, level2=level2, 
                                          count1=count1, count2=count2)

def get_no_data_message() -> str:
    """Get no data message"""
    return template_manager.get_template("no_data_message")
