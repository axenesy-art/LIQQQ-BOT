# fix_db.py dosyası oluştur
import asyncio
from datetime import datetime, timedelta
from data.data_storage import DataStorage

async def fix_database():
    print('🔧 DATABASE İ DÜZELTİYORUM...')
    
    storage = DataStorage()
    await storage.initialize()
    
    # 1. Tabloyu oluştur (eğer yoksa)
    create_table = '''
    CREATE TABLE IF NOT EXISTS liquidations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        size REAL NOT NULL,
        price REAL NOT NULL,
        exchange TEXT NOT NULL,
        timestamp DATETIME NOT NULL
    )
    '''
    await storage.execute_query(create_table)
    print('✅ liquidations tablosu oluşturuldu/var')
    
    # 2. Test data ekle
    test_data = [
        ('BTCUSDT', 'sell', 1.25, 86377.3, 'binance', (datetime.now() - timedelta(minutes=2)).isoformat()),
        ('BTCUSDT', 'sell', 0.85, 86350.0, 'bybit', (datetime.now() - timedelta(minutes=3)).isoformat()),
        ('BTCUSDT', 'buy', 0.45, 86200.0, 'okx', (datetime.now() - timedelta(minutes=4)).isoformat()),
        ('ETHUSDT', 'sell', 15.5, 3520.8, 'binance', (datetime.now() - timedelta(minutes=1)).isoformat()),
    ]
    
    insert_query = '''
    INSERT INTO liquidations (symbol, side, size, price, exchange, timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
    '''
    
    for data in test_data:
        await storage.execute_query(insert_query, data)
    
    print('✅ 4 test liquidation eklendi')
    
    # 3. Kontrol et
    count = await storage.fetch_one('SELECT COUNT(*) as c FROM liquidations')
    print(f'📊 Toplam liquidation: {count["c"]}')

if __name__ == "__main__":
    asyncio.run(fix_database())
    print('🎉 DATABASE DÜZELTİLDİ!')