import aiosqlite
import redis.asyncio as redis
import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from contextlib import asynccontextmanager

# Logger doğrudan burada tanımla - utils'den import etme
logger = logging.getLogger(__name__)

class SQLiteDatabase:
    """SQLite veritabanı işlemleri"""
    
    def __init__(self, db_path: str = "liquidation_data.db"):  # ✅ BU DOĞRU ZATEN
        self.db_path = db_path  # ✅ BU DOĞRU
        
    @asynccontextmanager
    async def get_connection(self):
        """Async database connection context manager"""
        connection = await aiosqlite.connect(self.db_path)
        try:
            yield connection
        finally:
            await connection.close()
            
    async def initialize(self):
        """Veritabanı tablolarını oluştur"""
        try:
            async with self.get_connection() as db:
                # Trades tablosu
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp INTEGER NOT NULL,
                        exchange TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        price REAL NOT NULL,
                        quantity REAL NOT NULL,
                        side TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Liquidations tablosu
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS liquidations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp INTEGER NOT NULL,
                        exchange TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        price REAL NOT NULL,
                        order_type TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Orderbook tablosu
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS orderbook (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp INTEGER NOT NULL,
                        exchange TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        bids TEXT NOT NULL,
                        asks TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Whale orders tablosu
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS whale_orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp INTEGER NOT NULL,
                        exchange TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        quantity REAL NOT NULL,
                        price REAL NOT NULL,
                        detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Index'ler oluştur
                await db.execute('CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_liquidations_timestamp ON liquidations(timestamp)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_liquidations_symbol ON liquidations(symbol)')
                await db.execute('CREATE INDEX IF NOT EXISTS idx_whale_orders_timestamp ON whale_orders(timestamp)')
                
                await db.commit()
                logger.info("SQLite veritabanı tabloları oluşturuldu [OK]")
                
        except Exception as e:
            logger.error(f"Veritabanı başlatma hatası: {e}")
            raise
            
    async def insert_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Trade verisini veritabanına ekle"""
        try:
            async with self.get_connection() as db:
                await db.execute('''
                    INSERT INTO trades (timestamp, exchange, symbol, price, quantity, side)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data['timestamp'],
                    trade_data['exchange'],
                    trade_data['symbol'],
                    trade_data['price'],
                    trade_data['quantity'],
                    trade_data['side']
                ))
                await db.commit()
                return True
                
        except Exception as e:
            logger.error(f"Trade ekleme hatası: {e}")
            return False
            
    async def insert_liquidation(self, liquidation_data: Dict[str, Any]) -> bool:
        """Liquidation verisini veritabanına ekle"""
        try:
            async with self.get_connection() as db:
                await db.execute('''
                    INSERT INTO liquidations (timestamp, exchange, symbol, side, quantity, price, order_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    liquidation_data['timestamp'],
                    liquidation_data['exchange'],
                    liquidation_data['symbol'],
                    liquidation_data['side'],
                    liquidation_data['quantity'],
                    liquidation_data['price'],
                    liquidation_data.get('order_type', 'UNKNOWN')
                ))
                await db.commit()
                return True
                
        except Exception as e:
            logger.error(f"Liquidation ekleme hatası: {e}")
            return False
            
    async def insert_orderbook(self, orderbook_data: Dict[str, Any]) -> bool:
        """Orderbook verisini veritabanına ekle"""
        try:
            async with self.get_connection() as db:
                await db.execute('''
                    INSERT INTO orderbook (timestamp, exchange, symbol, bids, asks)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    orderbook_data['timestamp'],
                    orderbook_data['exchange'],
                    orderbook_data['symbol'],
                    json.dumps(orderbook_data['bids']),
                    json.dumps(orderbook_data['asks'])
                ))
                await db.commit()
                return True
                
        except Exception as e:
            logger.error(f"Orderbook ekleme hatası: {e}")
            return False
            
    async def insert_whale_order(self, whale_data: Dict[str, Any]) -> bool:
        """Whale order verisini veritabanına ekle"""
        try:
            async with self.get_connection() as db:
                await db.execute('''
                    INSERT INTO whale_orders (timestamp, exchange, symbol, side, quantity, price)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    whale_data['timestamp'],
                    whale_data['exchange'],
                    whale_data['symbol'],
                    whale_data['side'],
                    whale_data['quantity'],
                    whale_data['price']
                ))
                await db.commit()
                return True
                
        except Exception as e:
            logger.error(f"Whale order ekleme hatası: {e}")
            return False
            
    async def get_recent_liquidations(self, limit: int = 100, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Son liquidation'ları getir"""
        try:
            async with self.get_connection() as db:
                if symbol:
                    cursor = await db.execute('''
                        SELECT * FROM liquidations 
                        WHERE symbol = ? 
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    ''', (symbol, limit))
                else:
                    cursor = await db.execute('''
                        SELECT * FROM liquidations 
                        ORDER BY timestamp DESC 
                        LIMIT ?
                    ''', (limit,))
                    
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Liquidation getirme hatası: {e}")
            return []
            
    async def get_liquidation_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Liquidation istatistiklerini getir"""
        try:
            timestamp_cutoff = int(datetime.now().timestamp() * 1000) - (hours * 60 * 60 * 1000)
            
            async with self.get_connection() as db:
                # Toplam liquidation sayısı
                cursor = await db.execute(
                    'SELECT COUNT(*) FROM liquidations WHERE timestamp > ?',
                    (timestamp_cutoff,)
                )
                total_count = (await cursor.fetchone())[0]
                
                # Side bazlı istatistikler
                cursor = await db.execute('''
                    SELECT side, COUNT(*), SUM(quantity), AVG(quantity)
                    FROM liquidations 
                    WHERE timestamp > ?
                    GROUP BY side
                ''', (timestamp_cutoff,))
                
                side_stats = {}
                for row in await cursor.fetchall():
                    side_stats[row[0]] = {
                        'count': row[1],
                        'total_quantity': row[2],
                        'avg_quantity': row[3]
                    }
                    
                # Symbol bazlı sıralama
                cursor = await db.execute('''
                    SELECT symbol, COUNT(*) as count, SUM(quantity) as total_quantity
                    FROM liquidations 
                    WHERE timestamp > ?
                    GROUP BY symbol
                    ORDER BY total_quantity DESC
                    LIMIT 10
                ''', (timestamp_cutoff,))
                
                symbol_ranking = []
                for row in await cursor.fetchall():
                    symbol_ranking.append({
                        'symbol': row[0],
                        'count': row[1],
                        'total_quantity': row[2]
                    })
                    
                return {
                    'total_count': total_count,
                    'side_stats': side_stats,
                    'symbol_ranking': symbol_ranking,
                    'timeframe_hours': hours
                }
                
        except Exception as e:
            logger.error(f"İstatistik getirme hatası: {e}")
            return {}
        
        
    async def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """Execute query and return single result"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(query, params or ())
                result = await cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            self.logger.error(f"fetch_one error: {e}")
            return None

    async def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute query and return all results"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(query, params or ())
                results = await cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            self.logger.error(f"fetch_all error: {e}")
            return []

    async def execute_query(self, query: str, params: tuple = None) -> bool:
        """Execute INSERT/UPDATE/DELETE query"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(query, params or ())
                await db.commit()
                return True
        except Exception as e:
            self.logger.error(f"execute_query error: {e}")
            return False

    async def save_liquidation(self, liquidation_data: Dict) -> bool:
        """Save liquidation to database"""
        query = """
            INSERT INTO liquidations (symbol, side, quantity, price, exchange, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        params = (
            liquidation_data['symbol'],
            liquidation_data['side'],
            liquidation_data['quantity'],
            liquidation_data['price'],
            liquidation_data['exchange'],
            liquidation_data['timestamp'].isoformat()
        )
        return await self.execute_query(query, params)

    async def get_recent_liquidations(self, symbol: str = None, limit: int = 50) -> List[Dict]:
        """Get recent liquidations"""
        if symbol:
            query = "SELECT * FROM liquidations WHERE symbol = ? ORDER BY timestamp DESC LIMIT ?"
            return await self.fetch_all(query, (symbol, limit))
        else:
            query = "SELECT * FROM liquidations ORDER BY timestamp DESC LIMIT ?"
            return await self.fetch_all(query, (limit,))

    async def get_whale_positions(self, symbol: str = None) -> Dict:
        """Get whale positions (simulated for now)"""
        # This should analyze large positions from liquidation data
        query = """
            SELECT symbol, side, SUM(quantity) as total_quantity, AVG(price) as avg_price
            FROM liquidations 
            WHERE quantity > 10000  # Large positions considered whale
            GROUP BY symbol, side
        """
        results = await self.fetch_all(query)
        
        whale_positions = {}
        for row in results:
            symbol = row['symbol']
            if symbol not in whale_positions:
                whale_positions[symbol] = []
            whale_positions[symbol].append({
                'side': row['side'],
                'size': row['total_quantity'],
                'entry_price': row['avg_price']
            })
        
        return whale_positions
    
        

class RedisCache:
    """Redis cache işlemleri"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.client = None
        
    async def initialize(self):
        """Redis client'ı başlat"""
        try:
            self.client = redis.from_url(self.redis_url, decode_responses=True)
            # Bağlantıyı test et
            await self.client.ping()
            logger.info("Redis bağlantısı başarılı [OK]")
            
        except Exception as e:
            logger.error(f"Redis bağlantı hatası: {e}")
            # Redis yoksa warning ver ama devam et
            logger.warning("Redis bağlantısı kurulamadı, cache devre dışı")
            self.client = None
            
    async def cache_realtime_data(self, key: str, data: Any, expire: int = 300) -> bool:
        """Real-time veriyi cache'e ekle"""
        if not self.client:
            return False
            
        try:
            await self.client.setex(
                key, 
                expire, 
                json.dumps(data, default=str)
            )
            return True
            
        except Exception as e:
            logger.error(f"Cache ekleme hatası: {e}")
            return False
            
    async def get_cached_data(self, key: str) -> Optional[Any]:
        """Cache'den veri getir"""
        if not self.client:
            return None
            
        try:
            data = await self.client.get(key)
            return json.loads(data) if data else None
            
        except Exception as e:
            logger.error(f"Cache getirme hatası: {e}")
            return None
            
    async def get_connection_state(self, exchange: str) -> Optional[Dict[str, Any]]:
        """Exchange connection state'i getir"""
        return await self.get_cached_data(f"connection_state:{exchange}")
        
    async def set_connection_state(self, exchange: str, state: Dict[str, Any]) -> bool:
        """Exchange connection state'i kaydet"""
        return await self.cache_realtime_data(f"connection_state:{exchange}", state, 60)
        
    async def cache_realtime_trades(self, trades: List[Dict[str, Any]]) -> bool:
        """Real-time trades cache'le"""
        return await self.cache_realtime_data("realtime_trades", trades, 30)
        
    async def get_realtime_trades(self) -> Optional[List[Dict[str, Any]]]:
        """Real-time trades getir"""
        return await self.get_cached_data("realtime_trades")
        
    async def cache_realtime_liquidations(self, liquidations: List[Dict[str, Any]]) -> bool:
        """Real-time liquidations cache'le"""
        return await self.cache_realtime_data("realtime_liquidations", liquidations, 30)
        
    async def get_realtime_liquidations(self) -> Optional[List[Dict[str, Any]]]:
        """Real-time liquidations getir"""
        return await self.get_cached_data("realtime_liquidations")
        
    async def close(self):
        """Redis bağlantısını kapat"""
        if self.client:
            await self.client.close()
            logger.info("Redis bağlantısı kapatıldı")

class DataStorage:
    """Ana veri depolama sınıfı - SQLite ve Redis'i yönetir"""
    
    def __init__(self, db_path: str = "liquidation_data.db"):  # 🔥 db_path EKLE
        self.db_path = db_path  # 🔥 BU SATIRI EKLE
        self.sqlite = SQLiteDatabase(db_path)  # 🔥 db_path'i geç
        self.redis = RedisCache("redis://localhost:6379")
        self.is_initialized = False
        self.logger = logging.getLogger(__name__)  # 🔥 BU SATIRI EKLE
        print(f"✅ DataStorage created with db_path: {self.db_path}")  # Debug için
        
        
    async def initialize(self):
        """Veri depolama sistemini başlat"""
        try:
            await self.sqlite.initialize()
            await self.redis.initialize()
            self.is_initialized = True
            logger.info("Data Storage başlatıldı [OK]")
            
        except Exception as e:
            logger.error(f"Data Storage başlatma hatası: {e}")
            raise
            
    async def save_liquidation(self, liquidation_data: Dict[str, Any]) -> bool:
        """Liquidation verisini kaydet (hem SQLite hem Redis)"""
        try:
            # SQLite'a kaydet
            success_sqlite = await self.sqlite.insert_liquidation(liquidation_data)
            
            # Redis'e cache'le (eğer Redis varsa)
            success_redis = True
            if self.redis.client:
                current_liquidations = await self.redis.get_realtime_liquidations() or []
                current_liquidations.insert(0, liquidation_data)
                current_liquidations = current_liquidations[:50]
                success_redis = await self.redis.cache_realtime_liquidations(current_liquidations)
            
            return success_sqlite and success_redis
            
        except Exception as e:
            logger.error(f"Liquidation kaydetme hatası: {e}")
            return False
            
    async def save_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Trade verisini kaydet"""
        try:
            success_sqlite = await self.sqlite.insert_trade(trade_data)
            
            success_redis = True
            if self.redis.client:
                current_trades = await self.redis.get_realtime_trades() or []
                current_trades.insert(0, trade_data)
                current_trades = current_trades[:100]
                success_redis = await self.redis.cache_realtime_trades(current_trades)
            
            return success_sqlite and success_redis
            
        except Exception as e:
            logger.error(f"Trade kaydetme hatası: {e}")
            return False
            
    async def get_recent_liquidations(self, limit: int = 100, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Son liquidation'ları getir"""
        return await self.sqlite.get_recent_liquidations(limit, symbol)
        
    async def get_liquidation_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Liquidation istatistiklerini getir"""
        return await self.sqlite.get_liquidation_stats(hours)
        
    async def close(self):
        """Bağlantıları kapat"""
        await self.redis.close()
        logger.info("Data Storage kapatıldı [OK]")
        
    
    async def get_whale_positions(self, symbol: str = None) -> Dict:
        """Get whale positions"""
        try:
            # SQLiteDatabase'deki metod
            return await self.sqlite.get_whale_positions(symbol)
        except Exception as e:
            self.logger.error(f"DataStorage.get_whale_positions error: {e}")
            return {}    
        
    
    async def _get_connection(self):
        """Get database connection"""
        import aiosqlite
        conn = await aiosqlite.connect(self.db_path)
        conn.row_factory = aiosqlite.Row
        return conn
        
        
    async def fetch_one(self, query: str, params: tuple = None):
        """Fetch single row from database"""
        try:
            async with self._get_connection() as conn:
                if params:
                    cursor = await conn.execute(query, params)
                else:
                    cursor = await conn.execute(query)
                
                row = await cursor.fetchone()
                if row:
                    # Convert to dict with column names
                    column_names = [description[0] for description in cursor.description]
                    return dict(zip(column_names, row))
                return None
                
        except Exception as e:
            self.logger.error(f"Error in fetch_one: {str(e)}")
            return None

    async def fetch_all(self, query: str, params: tuple = None):
        """Fetch all rows from database"""
        try:
            async with self._get_connection() as conn:
                if params:
                    cursor = await conn.execute(query, params)
                else:
                    cursor = await conn.execute(query)
                
                rows = await cursor.fetchall()
                if rows:
                    # Convert to list of dicts
                    column_names = [description[0] for description in cursor.description]
                    return [dict(zip(column_names, row)) for row in rows]
                return []
                
        except Exception as e:
            self.logger.error(f"Error in fetch_all: {str(e)}")
            return []

    async def execute_query(self, query: str, params: tuple = None):
        """Execute query (INSERT, UPDATE, DELETE)"""
        try:
            async with self._get_connection() as conn:
                if params:
                    await conn.execute(query, params)
                else:
                    await conn.execute(query)
                await conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error in execute_query: {str(e)}")
            return False    
    

    async def save_liquidation(self, liquidation_data: dict):
        """Save liquidation to database"""
        try:
            query = """
                INSERT INTO liquidations 
                (symbol, side, size, price, exchange, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            
            params = (
                liquidation_data.get('symbol', ''),
                liquidation_data.get('side', ''),
                liquidation_data.get('size', 0),
                liquidation_data.get('price', 0),
                liquidation_data.get('exchange', ''),
                liquidation_data.get('timestamp', '')
            )
            
            success = await self.execute_query(query, params)
            
            if success:
                print(f"✅ Liquidation saved: {liquidation_data.get('symbol')} "
                    f"{liquidation_data.get('side')} {liquidation_data.get('size')}")
            
            return success
            
        except Exception as e:
            print(f"❌ Error saving liquidation: {e}")
            return False
       
       
        

# Global instance
data_storage = DataStorage()
