#!/usr/bin/env python3

import json
import os
from message_templates import template_manager

def show_templates():
    """Show all available templates"""
    print("📝 Доступні шаблони:")
    print("=" * 50)
    
    templates = template_manager.templates
    for i, (name, content) in enumerate(templates.items(), 1):
        print(f"{i}. {name}")
        print(f"   {content[:100]}{'...' if len(content) > 100 else ''}")
        print()

def edit_template():
    """Edit a specific template"""
    templates = template_manager.list_templates()
    
    print("📝 Доступні шаблони для редагування:")
    for i, name in enumerate(templates, 1):
        print(f"{i}. {name}")
    
    try:
        choice = int(input("\nВиберіть номер шаблону для редагування: ")) - 1
        if 0 <= choice < len(templates):
            template_name = templates[choice]
            current_content = template_manager.get_template(template_name)
            
            print(f"\n📝 Редагування шаблону: {template_name}")
            print("=" * 50)
            print("Поточний вміст:")
            print(current_content)
            print("\n" + "=" * 50)
            
            print("\nВведіть новий вміст (натисніть Enter двічі для завершення):")
            lines = []
            while True:
                line = input()
                if line == "" and lines and lines[-1] == "":
                    lines.pop()  # Remove the last empty line
                    break
                lines.append(line)
            
            new_content = "\n".join(lines)
            
            if template_manager.update_template(template_name, new_content):
                print(f"✅ Шаблон '{template_name}' оновлено!")
            else:
                print(f"❌ Помилка оновлення шаблону '{template_name}'")
        else:
            print("❌ Невірний номер")
    except ValueError:
        print("❌ Введіть число")
    except KeyboardInterrupt:
        print("\n❌ Скасовано")

def show_template_info():
    """Show information about template variables"""
    print("📋 Інформація про змінні в шаблонах:")
    print("=" * 50)
    
    template_vars = {
        "level_1_header": {
            "description": "Заголовок для рівня 1",
            "variables": {"threshold": "Поріг фандингу (наприклад, 1.0)"}
        },
        "level_2_header": {
            "description": "Заголовок для рівня 2",
            "variables": {"threshold": "Поріг фандингу (наприклад, 2.0)"}
        },
        "ticker_box": {
            "description": "Верхня частина блоку тікера",
            "variables": {"ticker": "Назва тікера (наприклад, BTC)"}
        },
        "ticker_footer": {
            "description": "Нижня частина блоку тікера",
            "variables": {}
        },
        "exchange_line": {
            "description": "Рядок з інформацією про біржу",
            "variables": {
                "exchange": "Назва біржі (Binance, Bybit, тощо)",
                "ticker": "Назва тікера",
                "sign": "Знак (+ або -)",
                "rate": "Фандинг у відсотках",
                "cycle_hours": "Цикл фандингу (1h, 4h, 8h)",
                "payout_time": "Час до виплати"
            }
        },
        "startup_message": {
            "description": "Повідомлення при запуску",
            "variables": {
                "level1": "Поріг рівня 1",
                "level2": "Поріг рівня 2",
                "interval": "Інтервал збору даних"
            }
        },
        "stats_message": {
            "description": "Статистика збору",
            "variables": {
                "total": "Загальна кількість тікерів",
                "threshold": "Поріг фільтрації",
                "level1": "Поріг рівня 1",
                "level2": "Поріг рівня 2",
                "count1": "Кількість тікерів рівня 1",
                "count2": "Кількість тікерів рівня 2"
            }
        }
    }
    
    for template_name, info in template_vars.items():
        print(f"📝 {template_name}:")
        print(f"   Опис: {info['description']}")
        if info['variables']:
            print("   Змінні:")
            for var, desc in info['variables'].items():
                print(f"     {var}: {desc}")
        print()

def reset_templates():
    """Reset all templates to default values"""
    print("⚠️ Ви впевнені, що хочете скинути всі шаблони до значень за замовчуванням?")
    confirm = input("Введіть 'yes' для підтвердження: ")
    
    if confirm.lower() == 'yes':
        template_manager.reset_to_defaults()
        print("✅ Шаблони скинуто до значень за замовчуванням!")
    else:
        print("❌ Скасовано")

def preview_template():
    """Preview a template with sample data"""
    templates = template_manager.list_templates()
    
    print("📝 Доступні шаблони для перегляду:")
    for i, name in enumerate(templates, 1):
        print(f"{i}. {name}")
    
    try:
        choice = int(input("\nВиберіть номер шаблону для перегляду: ")) - 1
        if 0 <= choice < len(templates):
            template_name = templates[choice]
            template = template_manager.get_template(template_name)
            
            print(f"\n📝 Перегляд шаблону: {template_name}")
            print("=" * 50)
            print("Шаблон:")
            print(template)
            print("\n" + "=" * 50)
            
            # Try to format with sample data
            try:
                if template_name == "level_1_header":
                    formatted = template_manager.format_template(template_name, threshold=1.0)
                elif template_name == "level_2_header":
                    formatted = template_manager.format_template(template_name, threshold=2.0)
                elif template_name == "ticker_box":
                    formatted = template_manager.format_template(template_name, ticker="BTC")
                elif template_name == "exchange_line":
                    formatted = template_manager.format_template(
                        template_name, 
                        exchange="Binance", 
                        ticker="BTC", 
                        sign="+", 
                        rate=1.5, 
                        cycle_hours="8h", 
                        payout_time="Payout in 4h 30min"
                    )
                elif template_name == "startup_message":
                    formatted = template_manager.format_template(template_name, level1=1.0, level2=2.0, interval=300)
                elif template_name == "stats_message":
                    formatted = template_manager.format_template(
                        template_name, 
                        total=10, 
                        threshold=1.0, 
                        level1=1.0, 
                        level2=2.0, 
                        count1=5, 
                        count2=2
                    )
                else:
                    formatted = template
                
                print("Приклад форматування:")
                print(formatted)
            except Exception as e:
                print(f"⚠️ Помилка форматування: {e}")
        else:
            print("❌ Невірний номер")
    except ValueError:
        print("❌ Введіть число")
    except KeyboardInterrupt:
        print("\n❌ Скасовано")

def main():
    """Main menu"""
    while True:
        print("\n🎨 Редактор шаблонів повідомлень")
        print("=" * 40)
        print("1. Показати всі шаблони")
        print("2. Редагувати шаблон")
        print("3. Переглянути шаблон")
        print("4. Інформація про змінні")
        print("5. Скинути до значень за замовчуванням")
        print("6. Вихід")
        
        try:
            choice = input("\nВиберіть опцію: ")
            
            if choice == "1":
                show_templates()
            elif choice == "2":
                edit_template()
            elif choice == "3":
                preview_template()
            elif choice == "4":
                show_template_info()
            elif choice == "5":
                reset_templates()
            elif choice == "6":
                print("👋 До побачення!")
                break
            else:
                print("❌ Невірний вибір")
                
        except KeyboardInterrupt:
            print("\n👋 До побачення!")
            break

if __name__ == "__main__":
    main()
