import asyncio
import aiohttp
import json
import logging
import sqlite3
import redis
from datetime import datetime
import time
import signal
import sys
from aiohttp import ClientSession, TCPConnector
from aiohttp.resolver import AsyncResolver

# Gelişmiş logging konfigürasyonu
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('UltraLiquidationBot')

class UltraStableDataCollector:
    def __init__(self, db_path='liquidation_data.db', redis_host='localhost', redis_port=6379):
        self.db_path = db_path
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        
        # Exchange WebSocket URL'leri
        self.exchanges = {
            'binance': {
                'url': 'wss://fstream.binance.com/ws/!forceOrder@arr',
                'subscription': None
            },
            'bybit': {
                'url': 'wss://stream.bybit.com/v5/public/linear',
                'subscription': {
                    "op": "subscribe",
                    "args": ["liquidation"]
                }
            },
            'okx': {
                'url': 'wss://ws.okx.com:8443/ws/v5/public',
                'subscription': {
                    "op": "subscribe",
                    "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]
                }
            }
        }
        
        self.websockets = {}
        self.is_running = False
        self.reconnect_attempts = {}
        self.max_reconnect_attempts = 10
        self.connection_timeout = 30
        self.heartbeat_interval = 25
        
        # DNS problemi için alternative resolver
        self.resolver = AsyncResolver()
        
        self.setup_database()

    def setup_database(self):
        """Veritabanını kur"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS liquidations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL NOT NULL,
                    timestamp DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Indexler
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exchange_timestamp ON liquidations(exchange, timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_side ON liquidations(symbol, side)')
            
            conn.commit()
            conn.close()
            logger.info("Database setup completed successfully")
        except Exception as e:
            logger.error(f"Database setup error: {e}")

    async def create_stable_session(self):
        """DNS problemleri için stabil HTTP session oluştur"""
        connector = TCPConnector(
            resolver=self.resolver,
            limit=100,
            limit_per_host=20,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        return ClientSession(connector=connector)

    async def connect_to_exchange(self, exchange_name):
        """Exchange WebSocket'ine bağlan (exponential backoff ile)"""
        if exchange_name not in self.reconnect_attempts:
            self.reconnect_attempts[exchange_name] = 0
            
        exchange_config = self.exchanges[exchange_name]
        
        try:
            # Exponential backoff delay
            attempt = self.reconnect_attempts[exchange_name]
            if attempt > 0:
                delay = min(2 ** attempt, 60)  # Max 60 saniye
                logger.info(f"Waiting {delay}s before reconnecting to {exchange_name}")
                await asyncio.sleep(delay)
            
            logger.info(f"Connecting to {exchange_name} (attempt {attempt + 1})")
            
            # DNS problemi için stabil session
            session = await self.create_stable_session()
            
            # Timeout ve heartbeat ile WebSocket bağlantısı
            websocket = await session.ws_connect(
                exchange_config['url'],
                timeout=self.connection_timeout,
                heartbeat=self.heartbeat_interval,
                receive_timeout=30
            )
            
            # Subscription mesajı gönder
            if exchange_config['subscription']:
                await websocket.send_json(exchange_config['subscription'])
                logger.info(f"Subscribed to {exchange_name} liquidation channel")
            
            self.websockets[exchange_name] = websocket
            self.reconnect_attempts[exchange_name] = 0  # Reset attempt counter
            
            logger.info(f"Successfully connected to {exchange_name}")
            return websocket
            
        except Exception as e:
            self.reconnect_attempts[exchange_name] += 1
            logger.error(f"Connection failed to {exchange_name}: {e}")
            
            if self.reconnect_attempts[exchange_name] >= self.max_reconnect_attempts:
                logger.error(f"Max reconnection attempts reached for {exchange_name}")
                return None
            
            return await self.connect_to_exchange(exchange_name)  # Retry

    async def handle_websocket_messages(self, exchange_name, websocket):
        """WebSocket mesajlarını işle"""
        try:
            async for message in websocket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await self.process_liquidation_data(exchange_name, message.data)
                elif message.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning(f"WebSocket connection closed for {exchange_name}")
                    break
                elif message.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error for {exchange_name}: {websocket.exception()}")
                    break
                    
        except asyncio.TimeoutError:
            logger.warning(f"WebSocket timeout for {exchange_name}")
        except Exception as e:
            logger.error(f"Error handling messages from {exchange_name}: {e}")
        finally:
            await self.reconnect_exchange(exchange_name)

    async def reconnect_exchange(self, exchange_name):
        """Exchange'e yeniden bağlan"""
        if exchange_name in self.websockets:
            try:
                await self.websockets[exchange_name].close()
            except:
                pass
            del self.websockets[exchange_name]
        
        if self.is_running:
            logger.info(f"Attempting to reconnect to {exchange_name}")
            await asyncio.sleep(2)
            asyncio.create_task(self.manage_exchange_connection(exchange_name))

    async def manage_exchange_connection(self, exchange_name):
        """Exchange bağlantısını yönet"""
        while self.is_running:
            websocket = await self.connect_to_exchange(exchange_name)
            if websocket:
                await self.handle_websocket_messages(exchange_name, websocket)
            else:
                logger.error(f"Failed to connect to {exchange_name}, retrying in 30s")
                await asyncio.sleep(30)

    async def process_liquidation_data(self, exchange_name, raw_data):
        """Liquidation verilerini işle ve kaydet"""
        try:
            data = json.loads(raw_data)
            liquidation_data = None
            
            # Exchange-specific data parsing
            if exchange_name == 'binance':
                if 'e' in data and data['e'] == 'forceOrder':
                    liquidation = data['o']
                    liquidation_data = {
                        'symbol': liquidation['s'],
                        'side': 'sell' if liquidation['S'] == 'SELL' else 'buy',
                        'quantity': float(liquidation['q']),
                        'price': float(liquidation['p']),
                        'timestamp': datetime.fromtimestamp(data['E'] / 1000)
                    }
                    
            elif exchange_name == 'bybit':
                if 'topic' in data and data['topic'] == 'liquidation':
                    for liquidation in data['data']:
                        liquidation_data = {
                            'symbol': liquidation['symbol'],
                            'side': liquidation['side'].lower(),
                            'quantity': float(liquidation['size']),
                            'price': float(liquidation['price']),
                            'timestamp': datetime.fromtimestamp(liquidation['timestamp'] / 1000)
                        }
                        await self.store_liquidation(exchange_name, liquidation_data)
                    return
                    
            elif exchange_name == 'okx':
                if 'arg' in data and data['arg']['channel'] == 'liquidation-orders':
                    for liquidation in data['data']:
                        liquidation_data = {
                            'symbol': liquidation['instId'],
                            'side': liquidation['side'].lower(),
                            'quantity': float(liquidation['sz']),
                            'price': float(liquidation['px']),
                            'timestamp': datetime.fromtimestamp(float(liquidation['ts']) / 1000)
                        }
                        await self.store_liquidation(exchange_name, liquidation_data)
                    return
            
            if liquidation_data:
                await self.store_liquidation(exchange_name, liquidation_data)
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error from {exchange_name}: {e}")
        except KeyError as e:
            logger.error(f"Key error processing {exchange_name} data: {e}")
        except Exception as e:
            logger.error(f"Error processing {exchange_name} data: {e}")

    async def store_liquidation(self, exchange, data):
        """Liquidation verisini SQLite ve Redis'e kaydet"""
        try:
            # SQLite'a kaydet
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO liquidations (exchange, symbol, side, quantity, price, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (exchange, data['symbol'], data['side'], data['quantity'], data['price'], data['timestamp']))
            conn.commit()
            conn.close()
            
            # Redis'e kaydet (real-time access için)
            timestamp_key = int(data['timestamp'].timestamp())
            redis_key = f"liquidations:{exchange}:{data['symbol']}:{timestamp_key}"
            
            self.redis_client.hset(redis_key, mapping={
                'exchange': exchange,
                'symbol': data['symbol'],
                'side': data['side'],
                'quantity': str(data['quantity']),
                'price': str(data['price']),
                'timestamp': data['timestamp'].isoformat()
            })
            self.redis_client.expire(redis_key, 3600)  # 1 saat TTL
            
            logger.info(f"Liquidation stored: {exchange} {data['symbol']} {data['side']} {data['quantity']} @ {data['price']}")
            
        except Exception as e:
            logger.error(f"Storage error: {e}")

    async def start(self):
        """Data Collector'ü başlat"""
        logger.info("🚀 Starting Ultra Liquidation Data Collector")
        self.is_running = True
        
        # Tüm exchange'lere bağlan
        tasks = []
        for exchange_name in self.exchanges.keys():
            task = asyncio.create_task(self.manage_exchange_connection(exchange_name))
            tasks.append(task)
        
        # Health check task
        health_task = asyncio.create_task(self.health_check())
        tasks.append(health_task)
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            await self.stop()

    async def health_check(self):
        """Sistem sağlık kontrolü"""
        while self.is_running:
            await asyncio.sleep(60)  # Her 60 saniyede bir
            
            active_connections = len([ws for ws in self.websockets.values() if not ws.closed])
            logger.info(f"Health check: {active_connections}/{len(self.exchanges)} connections active")
            
            # Bağlantı sayısı çok düşükse warning
            if active_connections < len(self.exchanges) // 2:
                logger.warning("Low connection count - attempting to restore connections")

    async def stop(self):
        """Data Collector'ü durdur"""
        logger.info("🛑 Stopping Ultra Liquidation Data Collector")
        self.is_running = False
        
        # Tüm WebSocket bağlantılarını kapat
        for exchange_name, websocket in self.websockets.items():
            try:
                await websocket.close()
                logger.info(f"Closed connection to {exchange_name}")
            except Exception as e:
                logger.error(f"Error closing connection to {exchange_name}: {e}")
        
        self.websockets.clear()

# Graceful shutdown handler
def signal_handler(signum, frame):
    logger.info("Received shutdown signal")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def main():
    collector = UltraStableDataCollector()
    try:
        await collector.start()
    except KeyboardInterrupt:
        pass
    finally:
        await collector.stop()

if __name__ == "__main__":
    asyncio.run(main())
