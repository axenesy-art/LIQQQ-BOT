# check_db.py dosyası oluştur
import asyncio
from data.data_storage import DataStorage

async def check_database():
    storage = DataStorage()
    await storage.initialize()
    
    # Database'de liquidations tablosu var mı?
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name='liquidations'"
    result = await storage.fetch_one(query)
    
    if result:
        print('✅ liquidations tablosu VAR')
        
        # Kaç kayıt var?
        count_query = 'SELECT COUNT(*) as count FROM liquidations'
        count_result = await storage.fetch_one(count_query)
        print(f'📊 liquidations tablosunda {count_result["count"]} kayıt var')
        
        # Son 3 kaydı göster
        recent_query = 'SELECT * FROM liquidations ORDER BY id DESC LIMIT 3'
        recent = await storage.fetch_all(recent_query)
        
        if recent:
            print('📋 Son 3 liquidation:')
            for row in recent:
                print(f'   {row["symbol"]} {row["side"]} {row["size"]} @ {row["price"]}')
        else:
            print('⚠️ Tablo boş!')
    else:
        print('❌ liquidations tablosu YOK!')

if __name__ == "__main__":
    asyncio.run(check_database())